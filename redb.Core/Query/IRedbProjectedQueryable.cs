using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace redb.Core.Query;

/// <summary>
/// Interface for LINQ query projections in REDB.
/// </summary>
public interface IRedbProjectedQueryable<TResult>
{
    /// <summary>
    /// Additional filtering of projected results.
    /// </summary>
    IRedbProjectedQueryable<TResult> Where(Expression<Func<TResult, bool>> predicate);
    
    /// <summary>
    /// Sort projected results.
    /// </summary>
    IRedbProjectedQueryable<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector);
    
    /// <summary>
    /// Sort projected results descending.
    /// </summary>
    IRedbProjectedQueryable<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector);
    
    /// <summary>
    /// Limit number of results.
    /// </summary>
    IRedbProjectedQueryable<TResult> Take(int count);
    
    /// <summary>
    /// Skip results.
    /// </summary>
    IRedbProjectedQueryable<TResult> Skip(int count);
    
    /// <summary>
    /// Distinct values.
    /// </summary>
    IRedbProjectedQueryable<TResult> Distinct();
    
    /// <summary>
    /// Execute query and get list of results.
    /// </summary>
    Task<List<TResult>> ToListAsync();
    
    /// <summary>
    /// Count results.
    /// </summary>
    Task<int> CountAsync();
    
    /// <summary>
    /// Get first result or default value.
    /// </summary>
    Task<TResult?> FirstOrDefaultAsync();
    
    /// <summary>
    /// Get projection info including SQL function and structure_ids
    /// </summary>
    Task<string> GetProjectionInfoAsync();
}

/// <summary>
/// Extension methods for Task&lt;IRedbProjectedQueryable&lt;T&gt;&gt; to avoid double await.
/// </summary>
public static class RedbProjectedQueryableTaskExtensions
{
    public static async Task<IRedbProjectedQueryable<TResult>> Where<TResult>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask,
        Expression<Func<TResult, bool>> predicate)
    {
        var query = await queryTask;
        return query.Where(predicate);
    }
    
    public static async Task<IRedbProjectedQueryable<TResult>> OrderBy<TResult, TKey>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask,
        Expression<Func<TResult, TKey>> keySelector)
    {
        var query = await queryTask;
        return query.OrderBy(keySelector);
    }
    
    public static async Task<IRedbProjectedQueryable<TResult>> OrderByDescending<TResult, TKey>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask,
        Expression<Func<TResult, TKey>> keySelector)
    {
        var query = await queryTask;
        return query.OrderByDescending(keySelector);
    }
    
    public static async Task<IRedbProjectedQueryable<TResult>> Take<TResult>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask,
        int count)
    {
        var query = await queryTask;
        return query.Take(count);
    }
    
    public static async Task<IRedbProjectedQueryable<TResult>> Skip<TResult>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask,
        int count)
    {
        var query = await queryTask;
        return query.Skip(count);
    }
    
    public static async Task<IRedbProjectedQueryable<TResult>> Distinct<TResult>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask)
    {
        var query = await queryTask;
        return query.Distinct();
    }
    
    public static async Task<List<TResult>> ToListAsync<TResult>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask)
    {
        var query = await queryTask;
        return await query.ToListAsync();
    }
    
    public static async Task<int> CountAsync<TResult>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask)
    {
        var query = await queryTask;
        return await query.CountAsync();
    }
    
    public static async Task<TResult?> FirstOrDefaultAsync<TResult>(
        this Task<IRedbProjectedQueryable<TResult>> queryTask)
    {
        var query = await queryTask;
        return await query.FirstOrDefaultAsync();
    }
}