using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Queryable for applying window functions to grouped data.
/// Allows ranking, running totals, and other analytics on aggregated results.
/// </summary>
/// <typeparam name="TKey">Group key type</typeparam>
/// <typeparam name="TProps">Object properties type</typeparam>
public interface IGroupedWindowedQueryable<TKey, TProps>
    where TProps : class, new()
{
    /// <summary>
    /// Execute query and materialize results with window functions applied to groups.
    /// </summary>
    /// <typeparam name="TResult">Result type</typeparam>
    /// <param name="selector">Projection expression with group aggregations and window functions</param>
    /// <returns>List of results with window calculations</returns>
    /// <param name="cancellationToken">Cancels the operation (OCE only; a rollback in flight always completes).</param>
    Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector,
        CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Get SQL preview for debugging.
    /// </summary>
    Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Window specification for grouped queries.
/// </summary>
/// <typeparam name="TKey">Group key type</typeparam>
/// <typeparam name="TProps">Object properties type</typeparam>
public interface IGroupedWindowSpec<TKey, TProps>
    where TProps : class, new()
{
    /// <summary>
    /// Partition window by group key field (for ranking within partitions).
    /// </summary>
    IGroupedWindowSpec<TKey, TProps> PartitionBy<TField>(
        Expression<Func<TKey, TField>> keySelector);
    
    /// <summary>
    /// Order rows within window ascending.
    /// </summary>
    IGroupedWindowSpec<TKey, TProps> OrderBy<TField>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TField>> orderSelector);
    
    /// <summary>
    /// Order rows within window descending.
    /// </summary>
    IGroupedWindowSpec<TKey, TProps> OrderByDesc<TField>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TField>> orderSelector);
}
