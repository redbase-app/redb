using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Models.Entities;
using redb.Core.Query;

namespace redb.Core.Query;

/// <summary>
/// Implementation of projections for tree LINQ queries in REDB
/// Specialized version for TreeRedbObject&lt;TProps&gt;
/// </summary>
public class TreeProjectedQueryable<TProps, TResult> : IRedbProjectedQueryable<TResult>
    where TProps : class, new()
{
    private readonly IRedbQueryable<TProps> _sourceQuery;
    private readonly Expression<Func<TreeRedbObject<TProps>, TResult>> _projection;

    // Chain of operations to execute after projection
    private readonly List<Expression<Func<TResult, bool>>> _wherePredicates = new();
    private readonly List<(Expression KeySelector, bool IsDescending)> _orderByExpressions = new();

    // TPQ-1 (review 2026-09-24), the tree twin of S-4 in RedbProjectedQueryable: Where/OrderBy run in memory after
    // the projection, so a Take/Skip called after them must too - sent to the source query it cut the page from
    // the unfiltered, unsorted rows. Distinct is over the projected rows, not over the source objects (every node
    // is distinct there). Pipeline: Where* -> OrderBy* -> Distinct -> Skip/Take in call order. A Take/Skip before
    // the first in-memory operation still goes to SQL.
    private readonly bool _isDistinct;
    private readonly List<(bool IsSkip, int Count)> _pagingOps = new();

    public TreeProjectedQueryable(
        IRedbQueryable<TProps> sourceQuery,
        Expression<Func<TreeRedbObject<TProps>, TResult>> projection)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
    }

    // Private constructor for creating copies with additional operations
    private TreeProjectedQueryable(
        IRedbQueryable<TProps> sourceQuery,
        Expression<Func<TreeRedbObject<TProps>, TResult>> projection,
        List<Expression<Func<TResult, bool>>> wherePredicates,
        List<(Expression KeySelector, bool IsDescending)> orderByExpressions,
        bool isDistinct,
        List<(bool IsSkip, int Count)> pagingOps)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
        _wherePredicates = new List<Expression<Func<TResult, bool>>>(wherePredicates);
        _orderByExpressions = new List<(Expression, bool)>(orderByExpressions);
        _isDistinct = isDistinct;
        _pagingOps = new List<(bool, int)>(pagingOps);
    }

    private TreeProjectedQueryable<TProps, TResult> With(
        IRedbQueryable<TProps>? sourceQuery = null,
        List<Expression<Func<TResult, bool>>>? wherePredicates = null,
        List<(Expression KeySelector, bool IsDescending)>? orderByExpressions = null,
        bool? isDistinct = null)
        => new(sourceQuery ?? _sourceQuery, _projection, wherePredicates ?? _wherePredicates,
            orderByExpressions ?? _orderByExpressions, isDistinct ?? _isDistinct, _pagingOps);

    private bool HasInMemoryOps =>
        _wherePredicates.Count > 0 || _orderByExpressions.Count > 0 || _isDistinct || _pagingOps.Count > 0;

    public IRedbProjectedQueryable<TResult> Where(Expression<Func<TResult, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        // Add filter to operations chain
        return With(wherePredicates: new List<Expression<Func<TResult, bool>>>(_wherePredicates) { predicate });
    }

    public IRedbProjectedQueryable<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        // Replace existing sorting
        return With(orderByExpressions: new List<(Expression, bool)> { (keySelector, false) });
    }

    public IRedbProjectedQueryable<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        // Replace existing sorting
        return With(orderByExpressions: new List<(Expression, bool)> { (keySelector, true) });
    }

    public IRedbProjectedQueryable<TResult> Take(int count)
    {
        if (HasInMemoryOps)
            return WithPagingOp(isSkip: false, count);
        return With(sourceQuery: _sourceQuery.Take(count));
    }

    public IRedbProjectedQueryable<TResult> Skip(int count)
    {
        if (HasInMemoryOps)
            return WithPagingOp(isSkip: true, count);
        return With(sourceQuery: _sourceQuery.Skip(count));
    }

    private TreeProjectedQueryable<TProps, TResult> WithPagingOp(bool isSkip, int count)
    {
        var copy = With();
        copy._pagingOps.Add((isSkip, count));
        return copy;
    }

    public IRedbProjectedQueryable<TResult> Distinct() => With(isDistinct: true);

    public async Task<List<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        // Everything after the projection runs in memory over the loaded nodes.
        var allObjects = await _sourceQuery.ToListAsync(cancellationToken: cancellationToken);
        var projection = _projection.Compile();

        IEnumerable<TResult> projectedResults = allObjects.Select(redbObj => projection((TreeRedbObject<TProps>)redbObj));

        foreach (var wherePredicate in _wherePredicates)
            projectedResults = projectedResults.Where(wherePredicate.Compile());

        IOrderedEnumerable<TResult>? orderedResults = null;
        foreach (var (keySelector, isDescending) in _orderByExpressions)
        {
            var compiledKeySelector = ((LambdaExpression)keySelector).Compile();
            if (orderedResults == null)
                orderedResults = isDescending
                    ? projectedResults.OrderByDescending(item => compiledKeySelector.DynamicInvoke(item))
                    : projectedResults.OrderBy(item => compiledKeySelector.DynamicInvoke(item));
            else
                orderedResults = isDescending
                    ? orderedResults.ThenByDescending(item => compiledKeySelector.DynamicInvoke(item))
                    : orderedResults.ThenBy(item => compiledKeySelector.DynamicInvoke(item));
        }
        if (orderedResults != null)
            projectedResults = orderedResults;

        if (_isDistinct)
            projectedResults = projectedResults.Distinct();

        foreach (var (isSkip, n) in _pagingOps)
            projectedResults = isSkip ? projectedResults.Skip(n) : projectedResults.Take(n);

        return projectedResults.ToList();
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        var results = await ToListAsync(cancellationToken: cancellationToken);
        return results.Count;
    }

    public async Task<TResult?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        var results = await ToListAsync(cancellationToken: cancellationToken);
        return results.FirstOrDefault();
    }

    /// <summary>
    /// Get projection info for tree queries (not optimized yet)
    /// </summary>
    public Task<string> GetProjectionInfoAsync(CancellationToken cancellationToken = default)
    {
        var info = @"=== TREE PROJECTION INFO ===
SQL Function: search_objects_with_facets (full load)
Note: Tree projections are not yet optimized for SQL projection
All data is loaded and projected in memory";
        return Task.FromResult(info);
    }
}
