using System.Linq.Expressions;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Interface for grouped REDB queries.
/// Allows executing aggregations per group via SelectAsync.
/// </summary>
public interface IRedbGroupedQueryable<TKey, TProps> where TProps : class, new()
{
    /// <summary>
    /// Executes projection with aggregations for each group.
    /// </summary>
    Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector);
    
    /// <summary>
    /// Returns group count.
    /// </summary>
    Task<int> CountAsync();
    
    /// <summary>
    /// Returns SQL string that will be executed for this GroupBy query.
    /// Useful for debugging and diagnostics.
    /// </summary>
    Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector);
    
    /// <summary>
    /// Filter groups by a predicate over aggregates (SQL HAVING).
    /// The predicate must reference the grouped projection only through
    /// <see cref="redb.Core.Query.Aggregation.Agg"/> helpers, e.g.:
    ///   <c>.Having(g =&gt; Agg.Count(g) &gt; 5 &amp;&amp; Agg.Sum(g, x =&gt; x.Salary) &gt; 1000)</c>.
    /// Multiple <c>Having</c> calls compose with logical AND.
    /// </summary>
    IRedbGroupedQueryable<TKey, TProps> Having(
        System.Linq.Expressions.Expression<Func<IRedbGrouping<TKey, TProps>, bool>> predicate);

    /// <summary>
    /// Apply window functions to grouped results.
    /// Allows ranking, running totals, and other analytics on aggregated data.
    /// </summary>
    /// <param name="windowConfig">Window specification (partition, order)</param>
    /// <returns>Grouped windowed queryable for further projection</returns>
    IGroupedWindowedQueryable<TKey, TProps> WithWindow(
        Action<IGroupedWindowSpec<TKey, TProps>> windowConfig);
}

