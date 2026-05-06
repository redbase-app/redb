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
    /// Apply window functions to grouped results.
    /// Allows ranking, running totals, and other analytics on aggregated data.
    /// </summary>
    /// <param name="windowConfig">Window specification (partition, order)</param>
    /// <returns>Grouped windowed queryable for further projection</returns>
    IGroupedWindowedQueryable<TKey, TProps> WithWindow(
        Action<IGroupedWindowSpec<TKey, TProps>> windowConfig);
}

