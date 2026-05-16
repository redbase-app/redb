using redb.Core.Models.Entities;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace redb.Core.Caching
{
    /// <summary>
    /// Specialized cache for object schemes.
    /// Fast access to scheme metadata by type, ID and name.
    /// </summary>
    public interface ISchemeMetadataCache
    {
        // === GET SCHEMES ===
        
        /// <summary>
        /// Get scheme by .NET type (most common case).
        /// </summary>
        Task<RedbScheme?> GetSchemeByTypeAsync<TProps>() where TProps : class;
        
        /// <summary>
        /// Get scheme by .NET type.
        /// </summary>
        Task<RedbScheme?> GetSchemeByTypeAsync(Type type);
        
        /// <summary>
        /// Get scheme by ID.
        /// </summary>
        Task<RedbScheme?> GetSchemeByIdAsync(long schemeId);
        
        /// <summary>
        /// Get scheme by name.
        /// </summary>
        Task<RedbScheme?> GetSchemeByNameAsync(string schemeName);
        
        // === SET/UPDATE SCHEMES ===
        
        /// <summary>
        /// Add or update scheme in cache.
        /// </summary>
        void SetScheme(RedbScheme scheme);
        
        /// <summary>
        /// Add or update scheme in cache with type binding.
        /// </summary>
        void SetSchemeForType<TProps>(RedbScheme scheme) where TProps : class;
        
        /// <summary>
        /// Add or update scheme in cache with type binding.
        /// </summary>
        void SetSchemeForType(Type type, RedbScheme scheme);
        
        // === INVALIDATION ===
        
        /// <summary>
        /// Remove scheme from cache by ID.
        /// </summary>
        void InvalidateScheme(long schemeId);
        
        /// <summary>
        /// Remove scheme from cache by name.
        /// </summary>
        void InvalidateScheme(string schemeName);
        
        /// <summary>
        /// Remove scheme from cache by type.
        /// </summary>
        void InvalidateSchemeForType<TProps>() where TProps : class;
        
        /// <summary>
        /// Remove scheme from cache by type.
        /// </summary>
        void InvalidateSchemeForType(Type type);
        
        /// <summary>
        /// Clear all scheme cache.
        /// </summary>
        void InvalidateAll();
        
        // === STATISTICS ===
        
        /// <summary>
        /// Get scheme cache statistics.
        /// </summary>
        SchemeCacheStatistics GetStatistics();
        
        /// <summary>
        /// Get all cached schemes (for diagnostics).
        /// </summary>
        Dictionary<long, RedbScheme> GetAllCachedSchemes();
    }
    
    /// <summary>
    /// Scheme cache statistics.
    /// </summary>
    public class SchemeCacheStatistics
    {
        /// <summary>
        /// Cache hits count.
        /// </summary>
        public long Hits { get; set; }
        
        /// <summary>
        /// Cache misses count.
        /// </summary>
        public long Misses { get; set; }
        
        /// <summary>
        /// Total requests count.
        /// </summary>
        public long TotalRequests => Hits + Misses;
        
        /// <summary>
        /// Cache hit ratio (0.0 - 1.0).
        /// </summary>
        public double HitRatio => TotalRequests > 0 ? (double)Hits / TotalRequests : 0.0;
        
        /// <summary>
        /// Cached schemes count.
        /// </summary>
        public int CachedSchemesCount { get; set; }
        
        /// <summary>
        /// Type mappings count.
        /// </summary>
        public int TypeMappingsCount { get; set; }
        
        /// <summary>
        /// Estimated cache size in bytes.
        /// </summary>
        public long EstimatedSizeBytes { get; set; }
        
        /// <summary>
        /// Last cache access time.
        /// </summary>
        public DateTime LastAccessTime { get; set; }
        
        /// <summary>
        /// Cache creation time.
        /// </summary>
        public DateTime CreatedTime { get; set; }
        
        /// <summary>
        /// Detailed statistics by request type.
        /// </summary>
        public Dictionary<string, long> RequestsByType { get; set; } = new();
    }
}
