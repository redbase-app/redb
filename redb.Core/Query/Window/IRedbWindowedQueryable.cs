
using System.Linq.Expressions;
using redb.Core.Models.Entities;

namespace redb.Core.Query.Window;

/// <summary>
/// Interface for queries with window functions.
/// </summary>
public interface IRedbWindowedQueryable<TProps> where TProps : class, new()
{
    /// <summary>
    /// Projection with window functions.
    /// </summary>
    Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector);
    
    /// <summary>
    /// Returns SQL string for debugging (like EF Core ToQueryString).
    /// </summary>
    Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector);
}
