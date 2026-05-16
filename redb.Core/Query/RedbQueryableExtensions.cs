using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Base;

namespace redb.Core.Query;

/// <summary>
/// Extension methods for IRedbQueryable.
/// Adds ToSqlStringAsync() and ToAggregateSqlStringAsync() for SQL preview (debugging).
/// </summary>
public static class RedbQueryableExtensions
{
    /// <summary>
    /// Returns the SQL query that will be executed (for debugging).
    /// Similar to .ToQueryString() from EF Core, but for REDB faceted queries.
    /// Works for both standard search and tree queries.
    /// </summary>
    /// <typeparam name="TProps">Object properties type</typeparam>
    /// <param name="query">LINQ query</param>
    /// <returns>Final SQL query as text</returns>
    /// <exception cref="NotSupportedException">If provider does not support SQL preview</exception>
    public static async Task<string> ToSqlStringAsync<TProps>(
        this IRedbQueryable<TProps> query) 
        where TProps : class, new()
    {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        try
        {
            var queryType = query.GetType();
            
            var contextField = queryType.GetField("_context", BindingFlags.NonPublic | BindingFlags.Instance);
            
            if (contextField == null)
                throw new InvalidOperationException($"Cannot get _context from {queryType.Name}");
            
            var context = contextField.GetValue(query);
            
            if (context == null)
                throw new InvalidOperationException("Context is null");
            
            var providerField = queryType.GetField("_provider", BindingFlags.NonPublic | BindingFlags.Instance);
            
            if (providerField == null)
                throw new InvalidOperationException($"Cannot get _provider from {queryType.Name}");
            
            var provider = providerField.GetValue(query);
            
            if (provider == null)
                throw new InvalidOperationException("Provider is null");
            
            if (provider is QueryProviderBase queryProvider)
            {
                if (context is QueryContext<TProps> standardContext)
                    return await queryProvider.GetSqlPreviewAsync(standardContext);
                
                throw new InvalidOperationException($"Context has unexpected type: {context.GetType().Name}");
            }
            
            if (provider is TreeQueryProviderBase treeProvider)
            {
                if (context is TreeQueryContext<TProps> treeContext)
                    return await treeProvider.GetSqlPreviewAsync(treeContext);
                
                throw new InvalidOperationException($"Context has unexpected type: {context.GetType().Name}");
            }
            
            throw new NotSupportedException(
                $"Provider type {provider.GetType().Name} does not support SQL preview.");
        }
        catch (Exception ex) when (ex is not NotSupportedException)
        {
            throw new InvalidOperationException($"Error getting SQL preview: {ex.Message}", ex);
        }
    }
    
    /// <summary>
    /// Returns the SQL query for aggregation (for debugging).
    /// Similar to .ToQueryString() from EF Core, but for REDB aggregations.
    /// </summary>
    /// <typeparam name="TProps">Object properties type</typeparam>
    /// <typeparam name="TResult">Aggregation result type</typeparam>
    /// <param name="query">LINQ query</param>
    /// <param name="selector">Aggregation expression (same as in AggregateAsync)</param>
    /// <returns>SQL query that will be executed</returns>
    public static async Task<string> ToAggregateSqlStringAsync<TProps, TResult>(
        this IRedbQueryable<TProps> query,
        Expression<Func<RedbObject<TProps>, TResult>> selector) 
        where TProps : class, new()
    {
        if (query == null) throw new ArgumentNullException(nameof(query));
        if (selector == null) throw new ArgumentNullException(nameof(selector));

        try
        {
            var queryType = query.GetType();
            
            var contextField = queryType.GetField("_context", BindingFlags.NonPublic | BindingFlags.Instance);
            var providerField = queryType.GetField("_provider", BindingFlags.NonPublic | BindingFlags.Instance);
            
            if (contextField == null || providerField == null)
                throw new InvalidOperationException("Cannot get _context or _provider");
            
            var context = contextField.GetValue(query);
            var provider = providerField.GetValue(query);
            
            if (context == null || provider == null)
                throw new InvalidOperationException("Context or Provider is null");
            
            if (provider is QueryProviderBase queryProvider && 
                context is QueryContext<TProps> standardContext)
            {
                return await queryProvider.GetAggregateSqlPreviewAsync(standardContext, selector);
            }
            
            throw new NotSupportedException(
                $"Provider type {provider.GetType().Name} does not support SQL preview for aggregations.");
        }
        catch (Exception ex) when (ex is not NotSupportedException)
        {
            throw new InvalidOperationException($"Error getting SQL preview: {ex.Message}", ex);
        }
    }
}

