using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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
        List<(Expression KeySelector, bool IsDescending)> orderByExpressions)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
        _wherePredicates = new List<Expression<Func<TResult, bool>>>(wherePredicates);
        _orderByExpressions = new List<(Expression, bool)>(orderByExpressions);
    }

    public IRedbProjectedQueryable<TResult> Where(Expression<Func<TResult, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));
        
        // Add filter to operations chain
        var newWherePredicates = new List<Expression<Func<TResult, bool>>>(_wherePredicates) { predicate };
        
        return new TreeProjectedQueryable<TProps, TResult>(
            _sourceQuery,
            _projection, 
            newWherePredicates, 
            _orderByExpressions);
    }

    public IRedbProjectedQueryable<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));
        
        // Replace existing sorting
        var newOrderByExpressions = new List<(Expression, bool)> { (keySelector, false) };
        
        return new TreeProjectedQueryable<TProps, TResult>(
            _sourceQuery,
            _projection,
            _wherePredicates,
            newOrderByExpressions);
    }

    public IRedbProjectedQueryable<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));
        
        // Replace existing sorting
        var newOrderByExpressions = new List<(Expression, bool)> { (keySelector, true) };
        
        return new TreeProjectedQueryable<TProps, TResult>(
            _sourceQuery,
            _projection,
            _wherePredicates,
            newOrderByExpressions);
    }

    public IRedbProjectedQueryable<TResult> Take(int count)
    {
        // Apply Take to source query
        var limitedSource = (IRedbQueryable<TProps>)_sourceQuery.Take(count);
        return new TreeProjectedQueryable<TProps, TResult>(limitedSource, _projection, _wherePredicates, _orderByExpressions);
    }

    public IRedbProjectedQueryable<TResult> Skip(int count)
    {
        // Apply Skip to source query
        var skippedSource = (IRedbQueryable<TProps>)_sourceQuery.Skip(count);
        return new TreeProjectedQueryable<TProps, TResult>(skippedSource, _projection, _wherePredicates, _orderByExpressions);
    }

    public IRedbProjectedQueryable<TResult> Distinct()
    {
        // Apply Distinct to source query
        var distinctSource = (IRedbQueryable<TProps>)_sourceQuery.Distinct();
        return new TreeProjectedQueryable<TProps, TResult>(distinctSource, _projection, _wherePredicates, _orderByExpressions);
    }

    public async Task<List<TResult>> ToListAsync()
    {
        // 🚨 CRITICAL PERFORMANCE ISSUE - EVERYTHING IN MEMORY! 
        // TODO: Rework to SQL-based projections for high performance
        
        // ⚡ OPTIMIZATION: Apply limits TO SOURCE QUERY before loading
        var optimizedSourceQuery = _sourceQuery;
        
        // If there's only projection without additional filters - can use limits
        if (!_wherePredicates.Any() && !_orderByExpressions.Any())
        {
            // Projection without additional logic - use original limits
            var fullObjects = await optimizedSourceQuery.ToListAsync();
            var simpleProjection = _projection.Compile();
            return fullObjects.Select(redbObj => simpleProjection((TreeRedbObject<TProps>)redbObj)).ToList();
        }
        
        // 🐌 FALLBACK: Old in-memory logic (for complex cases)
        // WARNING: Inefficient on large data!
        var allObjects = await optimizedSourceQuery.ToListAsync();
        var complexProjection = _projection.Compile();
        
        // Apply projection to each object
        var projectedResults = allObjects.Select(redbObj => complexProjection((TreeRedbObject<TProps>)redbObj));
        
        // Apply Where filters after projection
        foreach (var wherePredicate in _wherePredicates)
        {
            var compiledWhere = wherePredicate.Compile();
            projectedResults = projectedResults.Where(compiledWhere);
        }
        
        // Apply sorting after projection
        IOrderedEnumerable<TResult>? orderedResults = null;
        foreach (var (keySelector, isDescending) in _orderByExpressions)
        {
            // Compile expression to delegate
            var compiledKeySelector = ((LambdaExpression)keySelector).Compile();
            
            if (orderedResults == null)
            {
                // First sort
                orderedResults = isDescending 
                    ? projectedResults.OrderByDescending(item => compiledKeySelector.DynamicInvoke(item))
                    : projectedResults.OrderBy(item => compiledKeySelector.DynamicInvoke(item));
            }
            else
            {
                // Additional sort
                orderedResults = isDescending 
                    ? orderedResults.ThenByDescending(item => compiledKeySelector.DynamicInvoke(item))
                    : orderedResults.ThenBy(item => compiledKeySelector.DynamicInvoke(item));
            }
        }
        
        var finalResults = orderedResults?.AsEnumerable() ?? projectedResults;
        return finalResults.ToList();
    }

    public async Task<int> CountAsync()
    {
        var results = await ToListAsync();
        return results.Count;
    }

    public async Task<TResult?> FirstOrDefaultAsync()
    {
        var results = await ToListAsync();
        return results.FirstOrDefault();
    }
    
    /// <summary>
    /// Get projection info for tree queries (not optimized yet)
    /// </summary>
    public Task<string> GetProjectionInfoAsync()
    {
        var info = @"=== TREE PROJECTION INFO ===
SQL Function: search_objects_with_facets (full load)
Note: Tree projections are not yet optimized for SQL projection
All data is loaded and projected in memory";
        return Task.FromResult(info);
    }
}
