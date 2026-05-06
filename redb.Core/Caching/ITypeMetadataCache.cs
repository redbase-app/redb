using redb.Core.Models.Entities;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace redb.Core.Caching
{
    /// <summary>
    /// Specialized cache for data types.
    /// Fast access to type metadata by ID and name.
    /// Types change very rarely, so this cache can live long.
    /// </summary>
    public interface ITypeMetadataCache
    {
        // === GET TYPES ===
        
        /// <summary>
        /// Get all data types (often used during initialization).
        /// </summary>
        Task<List<RedbType>> GetAllTypesAsync();
        
        /// <summary>
        /// Get type by ID.
        /// </summary>
        Task<RedbType?> GetTypeByIdAsync(long typeId);
        
        /// <summary>
        /// Get type by name (String, Long, DateTime, etc.).
        /// </summary>
        Task<RedbType?> GetTypeByNameAsync(string typeName);
        
        /// <summary>
        /// Get all types map ID -> type (optimization for bulk operations).
        /// </summary>
        Task<Dictionary<long, RedbType>> GetAllTypesMapAsync();
        
        /// <summary>
        /// Get all types map name -> type (optimization for name lookup).
        /// </summary>
        Task<Dictionary<string, RedbType>> GetTypesByNameMapAsync();
        
        /// <summary>
        /// Get type ID by name (fast access to frequently used info).
        /// </summary>
        Task<long?> GetTypeIdByNameAsync(string typeName);
        
        // === SET/UPDATE TYPES ===
        
        /// <summary>
        /// Add or update type in cache.
        /// </summary>
        void SetType(RedbType type);
        
        /// <summary>
        /// Add or update all types in cache (usually during initialization).
        /// </summary>
        void SetAllTypes(List<RedbType> types);
        
        // === INVALIDATION ===
        
        /// <summary>
        /// Remove type from cache by ID.
        /// </summary>
        void InvalidateType(long typeId);
        
        /// <summary>
        /// Remove type from cache by name.
        /// </summary>
        void InvalidateType(string typeName);
        
        /// <summary>
        /// Clear all type cache (rare operation).
        /// </summary>
        void InvalidateAll();
        
        // === STATISTICS ===
        
        /// <summary>
        /// Get type cache statistics.
        /// </summary>
        TypeCacheStatistics GetStatistics();
        
        /// <summary>
        /// Get all cached types (for diagnostics).
        /// </summary>
        Dictionary<long, RedbType> GetAllCachedTypes();
        
        // === HELPER METHODS ===
        
        /// <summary>
        /// Check if type supports arrays.
        /// </summary>
        Task<bool> TypeSupportsArraysAsync(long typeId);
        
        /// <summary>
        /// Check if type is nullable.
        /// </summary>
        Task<bool> TypeSupportsNullAsync(long typeId);
        
        /// <summary>
        /// Get .NET type for REDB type.
        /// </summary>
        Task<Type?> GetNetTypeAsync(long typeId);
    }
    
    /// <summary>
    /// Type cache statistics.
    /// </summary>
    public class TypeCacheStatistics
    {
        public long Hits { get; set; }
        public long Misses { get; set; }
        public long TotalRequests => Hits + Misses;
        public double HitRatio => TotalRequests > 0 ? (double)Hits / TotalRequests : 0.0;
        public int CachedTypesCount { get; set; }
        public long EstimatedSizeBytes { get; set; }
        public DateTime LastAccessTime { get; set; }
        public DateTime CreatedTime { get; set; }
        public DateTime LastFullRefreshTime { get; set; }
        public Dictionary<string, long> RequestsByType { get; set; } = new();
        public Dictionary<string, long> RequestsByTypeName { get; set; } = new();
        public Dictionary<string, TypeUsageInfo> TypeUsageStats { get; set; } = new();
    }
    
    /// <summary>
    /// Type usage information.
    /// </summary>
    public class TypeUsageInfo
    {
        public long TypeId { get; set; }
        public string TypeName { get; set; } = "";
        public long RequestCount { get; set; }
        public DateTime LastRequestTime { get; set; }
        public bool SupportsArrays { get; set; }
        public bool SupportsNull { get; set; }
    }
}
