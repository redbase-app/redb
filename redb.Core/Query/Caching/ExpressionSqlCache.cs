using System.Linq.Expressions;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Caching.Memory;
using redb.Core.Query.Models;

namespace redb.Core.Query.Caching;

/// <summary>
/// Cache for compiled SQL templates.
/// Singleton per application with TTL and eviction.
/// Thread-safe.
/// </summary>
public class ExpressionSqlCache
{
    // Static Singleton per application
    private static readonly Lazy<ExpressionSqlCache> _instance = 
        new(() => new ExpressionSqlCache());
    
    /// <summary>
    /// Gets the singleton instance.
    /// </summary>
    public static ExpressionSqlCache Instance => _instance.Value;
    
    private readonly MemoryCache _cache;
    private readonly MemoryCacheEntryOptions _cacheOptions;
    
    private ExpressionSqlCache()
    {
        _cache = new MemoryCache(new MemoryCacheOptions
        {
            SizeLimit = 10_000  // Max 10K templates
        });
        
        _cacheOptions = new MemoryCacheEntryOptions
        {
            Size = 1,
            SlidingExpiration = TimeSpan.FromHours(1),  // TTL lazy - if not used for 1 hour
            AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(24)  // Max 24 hours
        };
    }
    
    /// <summary>
    /// Tries to get cached query.
    /// </summary>
    public bool TryGet(string key, out CompiledQuery? query)
        => _cache.TryGetValue(key, out query);
    
    /// <summary>
    /// Sets cached query.
    /// </summary>
    public void Set(string key, CompiledQuery query)
        => _cache.Set(key, query, _cacheOptions);
    
    /// <summary>
    /// Generates stable cache key from Expression (without constant values).
    /// Same structure = same key regardless of actual values.
    /// </summary>
    /// <typeparam name="TProps">Props type</typeparam>
    /// <param name="predicate">Filter expression</param>
    /// <param name="schemeId">Scheme ID</param>
    /// <returns>Cache key</returns>
    public string BuildCacheKey<TProps>(Expression<Func<TProps, bool>> predicate, long schemeId)
    {
        var exprKey = NormalizeExpression(predicate.Body);
        return $"{schemeId}:{exprKey}";
    }
    
    /// <summary>
    /// Generates cache key for ordering + filter combination.
    /// </summary>
    public string BuildCacheKey<TProps>(
        Expression<Func<TProps, bool>>? predicate,
        IEnumerable<(string field, bool desc)>? ordering,
        long schemeId)
    {
        var filterKey = predicate != null ? NormalizeExpression(predicate.Body) : "nofilter";
        var orderKey = ordering != null 
            ? string.Join(",", ordering.Select(o => $"{o.field}:{(o.desc ? "D" : "A")}"))
            : "noorder";
        return $"{schemeId}:{filterKey}:{orderKey}";
    }
    
    /// <summary>
    /// Normalizes Expression: replaces constants with placeholders.
    /// Same structure = same normalized string.
    /// </summary>
    private static string NormalizeExpression(Expression expression)
    {
        var str = expression.ToString();
        
        // Remove closure values like "value(Program+<>c__DisplayClass0_0).age"
        str = Regex.Replace(str, @"value\([^)]+\)\.\w+", "PARAM");
        
        // Remove numeric literals
        str = Regex.Replace(str, @"\b\d+(\.\d+)?\b", "N");
        
        // Remove string literals
        str = Regex.Replace(str, @"""[^""]*""", "S");
        
        // Remove GUID literals
        str = Regex.Replace(str, @"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "G");
        
        return str;
    }
    
    /// <summary>
    /// Cache statistics (for monitoring).
    /// </summary>
    public (long Count, long Size) GetStats() 
        => (_cache.Count, _cache.Count);
    
    /// <summary>
    /// Clears all cached queries. Useful for testing or after schema changes.
    /// </summary>
    public void Clear()
    {
        _cache.Compact(1.0); // Remove 100% of entries
    }
}

