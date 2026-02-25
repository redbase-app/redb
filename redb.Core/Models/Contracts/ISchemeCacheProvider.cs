using System;
using System.Threading.Tasks;
using redb.Core.Caching;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// Extension of ISchemeSyncProvider with metadata cache management methods
    /// Adds capabilities for hot switching, invalidation and cache warmup
    /// </summary>
    public interface ISchemeCacheProvider
    {
        // ===== CACHE STATE MANAGEMENT =====
        
        /// <summary>
        /// Enable/disable metadata caching on the fly (hot toggle)
        /// When disabled, automatically clears all cache
        /// </summary>
        /// <param name="enabled">true - enable cache, false - disable</param>
        void SetCacheEnabled(bool enabled);
        
        /// <summary>
        /// Check if caching is enabled
        /// </summary>
        bool IsCacheEnabled { get; }
        
        // ===== CACHE INVALIDATION =====
        
        /// <summary>
        /// Complete clearing of all metadata caches
        /// Resets statistics to zero
        /// </summary>
        void InvalidateCache();
        
        /// <summary>
        /// Clear metadata cache for specific C# type
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        void InvalidateSchemeCache<TProps>() where TProps : class;
        
        /// <summary>
        /// Clear metadata cache for scheme by ID
        /// </summary>
        /// <param name="schemeId">Scheme ID in database</param>
        void InvalidateSchemeCache(long schemeId);
        
        /// <summary>
        /// Clear metadata cache for scheme by name
        /// </summary>
        /// <param name="schemeName">Scheme name</param>
        void InvalidateSchemeCache(string schemeName);
        
        // ===== STATISTICS AND MONITORING =====
        
        /// <summary>
        /// Get detailed cache performance statistics
        /// </summary>
        CacheStatistics GetCacheStatistics();
        
        /// <summary>
        /// Reset cache statistics (zero out counters)
        /// The cache itself remains untouched
        /// </summary>
        void ResetCacheStatistics();
        
        // ===== CACHE PRELOADING (WARMUP) =====
        
        /// <summary>
        /// Preload metadata for C# type
        /// Useful for performance optimization during application startup
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        Task WarmupCacheAsync<TProps>() where TProps : class;
        
        /// <summary>
        /// Preload metadata for array of C# types
        /// </summary>
        /// <param name="types">Array of types to preload</param>
        Task WarmupCacheAsync(Type[] types);
        
        /// <summary>
        /// Preload metadata for all known schemes
        /// Use carefully - can be resource-intensive
        /// </summary>
        Task WarmupAllSchemesAsync();
        
        // ===== DIAGNOSTICS =====
        
        /// <summary>
        /// Get diagnostic information about cache state
        /// Includes optimization recommendations
        /// </summary>
        CacheDiagnosticInfo GetCacheDiagnosticInfo();
        
        /// <summary>
        /// Estimate current memory consumption by cache in bytes
        /// Approximate value for monitoring
        /// </summary>
        long EstimateMemoryUsage();
    }
}
