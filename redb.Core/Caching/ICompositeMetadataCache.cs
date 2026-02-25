using redb.Core.Models.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace redb.Core.Caching
{
    /// <summary>
    /// Composite metadata cache combining schemes, structures and types.
    /// Provides unified interface for all metadata caching operations.
    /// </summary>
    public interface ICompositeMetadataCache
    {
        // === CACHE COMPONENTS ===
        
        /// <summary>
        /// Object schemes cache.
        /// </summary>
        ISchemeMetadataCache Schemes { get; }
        
        /// <summary>
        /// Field structures cache.
        /// </summary>
        IStructureMetadataCache Structures { get; }
        
        /// <summary>
        /// Data types cache.
        /// </summary>
        ITypeMetadataCache Types { get; }
        
        // === COMPOSITE OPERATIONS ===
        
        /// <summary>
        /// Get complete metadata for .NET type (scheme + structures + types).
        /// Most frequently used operation - get everything in one call.
        /// </summary>
        /// <typeparam name="TProps">Object properties type.</typeparam>
        /// <returns>Complete metadata or null if scheme not found.</returns>
        Task<CompleteSchemeMetadata?> GetCompleteMetadataAsync<TProps>() where TProps : class;
        
        /// <summary>
        /// Get complete metadata for .NET type.
        /// </summary>
        /// <param name="type">Object type.</param>
        /// <returns>Complete metadata or null if scheme not found.</returns>
        Task<CompleteSchemeMetadata?> GetCompleteMetadataAsync(Type type);
        
        /// <summary>
        /// Get complete metadata by scheme ID.
        /// </summary>
        /// <param name="schemeId">Scheme ID.</param>
        /// <returns>Complete metadata or null if scheme not found.</returns>
        Task<CompleteSchemeMetadata?> GetCompleteMetadataAsync(long schemeId);
        
        /// <summary>
        /// Set complete metadata in cache.
        /// </summary>
        /// <param name="metadata">Complete metadata to cache.</param>
        void SetCompleteMetadata(CompleteSchemeMetadata metadata);
        
        /// <summary>
        /// Set complete metadata in cache with type binding.
        /// </summary>
        /// <typeparam name="TProps">Type to bind.</typeparam>
        /// <param name="metadata">Complete metadata to cache.</param>
        void SetCompleteMetadataForType<TProps>(CompleteSchemeMetadata metadata) where TProps : class;
        
        // === BULK OPERATIONS ===
        
        /// <summary>
        /// Warm up cache for list of types.
        /// </summary>
        /// <param name="types">Types to preload.</param>
        /// <param name="loadFromDatabase">Function to load from DB.</param>
        Task WarmupCacheAsync(Type[] types, Func<Type, Task<CompleteSchemeMetadata?>> loadFromDatabase);
        
        /// <summary>
        /// Warm up cache for all schemes.
        /// </summary>
        /// <param name="loadFromDatabase">Function to load from DB.</param>
        Task WarmupAllSchemesAsync(Func<Task<List<CompleteSchemeMetadata>>> loadFromDatabase);
        
        // === INVALIDATION ===
        
        /// <summary>
        /// Invalidate all related caches for scheme.
        /// Removes scheme, its structures and clears type bindings.
        /// </summary>
        /// <param name="schemeId">Scheme ID.</param>
        void InvalidateSchemeCompletely(long schemeId);
        
        /// <summary>
        /// Invalidate cache for .NET type.
        /// </summary>
        /// <typeparam name="TProps">Type to invalidate.</typeparam>
        void InvalidateTypeCompletely<TProps>() where TProps : class;
        
        /// <summary>
        /// Invalidate cache for .NET type.
        /// </summary>
        /// <param name="type">Type to invalidate.</param>
        void InvalidateTypeCompletely(Type type);
        
        /// <summary>
        /// Clear all caches completely.
        /// </summary>
        void InvalidateAll();
        
        // === STATISTICS ===
        
        /// <summary>
        /// Get combined statistics from all caches.
        /// </summary>
        /// <returns>Summary statistics.</returns>
        CompositeMetadataCacheStatistics GetStatistics();
        
        /// <summary>
        /// Get detailed statistics for each component.
        /// </summary>
        /// <returns>Detailed statistics by component.</returns>
        DetailedCacheStatistics GetDetailedStatistics();
        
        // === DIAGNOSTICS ===
        
        /// <summary>
        /// Get cache state diagnostic information.
        /// </summary>
        /// <returns>Diagnostic info.</returns>
        CacheDiagnosticInfo GetDiagnosticInfo();
        
        /// <summary>
        /// Export cache state for analysis.
        /// </summary>
        /// <returns>Exported cache state.</returns>
        CacheExportData ExportCacheState();
    }
    
    /// <summary>
    /// Complete scheme metadata including scheme, structures and types.
    /// </summary>
    public class CompleteSchemeMetadata
    {
        /// <summary>
        /// Object scheme.
        /// </summary>
        public RedbScheme Scheme { get; set; } = null!;
        
        /// <summary>
        /// All scheme structures ordered by Order.
        /// </summary>
        public List<RedbStructure> Structures { get; set; } = new();
        
        /// <summary>
        /// Structure name -> structure map for fast lookup.
        /// </summary>
        public Dictionary<string, RedbStructure> StructuresByName { get; set; } = new();
        
        /// <summary>
        /// Structure ID -> structure map for fast lookup.
        /// </summary>
        public Dictionary<long, RedbStructure> StructuresById { get; set; } = new();
        
        /// <summary>
        /// All types used in structures.
        /// </summary>
        public Dictionary<long, RedbType> Types { get; set; } = new();
        
        /// <summary>
        /// .NET type associated with this scheme (if any).
        /// </summary>
        public Type? AssociatedType { get; set; }
        
        /// <summary>
        /// Metadata creation time.
        /// </summary>
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Last usage time.
        /// </summary>
        public DateTime LastUsedAt { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Usage count.
        /// </summary>
        public long UsageCount { get; set; }
        
        /// <summary>
        /// Approximate size in bytes.
        /// </summary>
        public long EstimatedSizeBytes { get; set; }
        
        /// <summary>
        /// Are metadata valid (for consistency check).
        /// </summary>
        public bool IsValid => Scheme != null && Structures.Any();
        
        /// <summary>
        /// Update last usage time.
        /// </summary>
        public void MarkAsUsed()
        {
            LastUsedAt = DateTime.UtcNow;
            UsageCount++;
        }
    }
    
    /// <summary>
    /// Composite cache summary statistics.
    /// </summary>
    public class CompositeMetadataCacheStatistics
    {
        /// <summary>
        /// Total cache hits across all caches.
        /// </summary>
        public long TotalHits { get; set; }
        
        /// <summary>
        /// Total cache misses across all caches.
        /// </summary>
        public long TotalMisses { get; set; }
        
        /// <summary>
        /// Total requests count.
        /// </summary>
        public long TotalRequests => TotalHits + TotalMisses;
        
        /// <summary>
        /// Overall cache hit ratio.
        /// </summary>
        public double OverallHitRatio => TotalRequests > 0 ? (double)TotalHits / TotalRequests : 0.0;
        
        /// <summary>
        /// Total size of all caches in bytes.
        /// </summary>
        public long TotalSizeBytes { get; set; }
        
        /// <summary>
        /// Total cached items count.
        /// </summary>
        public int TotalCachedItems { get; set; }
        
        /// <summary>
        /// Statistics generation time.
        /// </summary>
        public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Statistics by operation type.
        /// </summary>
        public Dictionary<string, long> OperationStats { get; set; } = new();
        
        /// <summary>
        /// Top most used schemes.
        /// </summary>
        public List<TopUsedScheme> TopUsedSchemes { get; set; } = new();
    }
    
    /// <summary>
    /// Detailed statistics by cache component.
    /// </summary>
    public class DetailedCacheStatistics
    {
        /// <summary>
        /// Scheme cache statistics.
        /// </summary>
        public SchemeCacheStatistics SchemeStats { get; set; } = new();
        
        /// <summary>
        /// Structure cache statistics.
        /// </summary>
        public StructureCacheStatistics StructureStats { get; set; } = new();
        
        /// <summary>
        /// Type cache statistics.
        /// </summary>
        public TypeCacheStatistics TypeStats { get; set; } = new();
        
        /// <summary>
        /// Composite statistics.
        /// </summary>
        public CompositeMetadataCacheStatistics CompositeStats { get; set; } = new();
    }
    
    /// <summary>
    /// Top used scheme information.
    /// </summary>
    public class TopUsedScheme
    {
        /// <summary>
        /// Scheme ID.
        /// </summary>
        public long SchemeId { get; set; }
        
        /// <summary>
        /// Scheme name.
        /// </summary>
        public string SchemeName { get; set; } = "";
        
        /// <summary>
        /// Associated .NET type name (if any).
        /// </summary>
        public string? AssociatedTypeName { get; set; }
        
        /// <summary>
        /// Usage count.
        /// </summary>
        public long UsageCount { get; set; }
        
        /// <summary>
        /// Last usage time.
        /// </summary>
        public DateTime LastUsedAt { get; set; }
    }
    
    /// <summary>
    /// Cache diagnostic information.
    /// </summary>
    public class CacheDiagnosticInfo
    {
        /// <summary>
        /// Cache health status.
        /// </summary>
        public CacheHealthStatus HealthStatus { get; set; }
        
        /// <summary>
        /// Potential issues.
        /// </summary>
        public List<string> Issues { get; set; } = new();
        
        /// <summary>
        /// Optimization recommendations.
        /// </summary>
        public List<string> Recommendations { get; set; } = new();
        
        /// <summary>
        /// Memory information.
        /// </summary>
        public MemoryUsageInfo MemoryInfo { get; set; } = new();
        
        /// <summary>
        /// Performance information.
        /// </summary>
        public PerformanceInfo PerformanceInfo { get; set; } = new();
    }
    
    /// <summary>
    /// Cache health status enumeration.
    /// </summary>
    public enum CacheHealthStatus
    {
        /// <summary>Cache is healthy.</summary>
        Healthy,
        /// <summary>Cache has warnings.</summary>
        Warning,
        /// <summary>Cache is in critical state.</summary>
        Critical,
        /// <summary>Cache status is unknown.</summary>
        Unknown
    }
    
    /// <summary>
    /// Cache memory usage information.
    /// </summary>
    public class MemoryUsageInfo
    {
        /// <summary>
        /// Used memory in bytes.
        /// </summary>
        public long UsedBytes { get; set; }
        
        /// <summary>
        /// Maximum memory in bytes (if limited).
        /// </summary>
        public long? MaxBytes { get; set; }
        
        /// <summary>
        /// Memory usage percentage.
        /// </summary>
        public double UsagePercentage { get; set; }
        
        /// <summary>
        /// Memory fragmentation percentage (if available).
        /// </summary>
        public double? FragmentationPercentage { get; set; }
    }
    
    /// <summary>
    /// Cache performance information.
    /// </summary>
    public class PerformanceInfo
    {
        /// <summary>
        /// Average cache access time in milliseconds.
        /// </summary>
        public double AverageAccessTimeMs { get; set; }
        
        /// <summary>
        /// Average data source load time in milliseconds.
        /// </summary>
        public double AverageLoadTimeMs { get; set; }
        
        /// <summary>
        /// Operations per second.
        /// </summary>
        public double OperationsPerSecond { get; set; }
        
        /// <summary>
        /// Peak access time in milliseconds.
        /// </summary>
        public double PeakAccessTimeMs { get; set; }
    }
    
    /// <summary>
    /// Exported cache state for analysis.
    /// </summary>
    public class CacheExportData
    {
        /// <summary>
        /// Export time.
        /// </summary>
        public DateTime ExportedAt { get; set; } = DateTime.UtcNow;
        
        /// <summary>
        /// Export format version.
        /// </summary>
        public string FormatVersion { get; set; } = "1.0";
        
        /// <summary>
        /// Exported schemes.
        /// </summary>
        public List<RedbScheme> Schemes { get; set; } = new();
        
        /// <summary>
        /// Exported structures.
        /// </summary>
        public List<RedbStructure> Structures { get; set; } = new();
        
        /// <summary>
        /// Exported types.
        /// </summary>
        public List<RedbType> Types { get; set; } = new();
        
        /// <summary>
        /// Type to scheme mappings.
        /// </summary>
        public Dictionary<string, long> TypeToSchemeMapping { get; set; } = new();
        
        /// <summary>
        /// Usage statistics.
        /// </summary>
        public DetailedCacheStatistics Statistics { get; set; } = new();
    }
}
