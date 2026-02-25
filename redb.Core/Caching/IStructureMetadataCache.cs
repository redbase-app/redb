using redb.Core.Models.Entities;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace redb.Core.Caching
{
    /// <summary>
    /// Specialized cache for field structures.
    /// Fast access to structure metadata by scheme, ID and type.
    /// </summary>
    public interface IStructureMetadataCache
    {
        // === GET STRUCTURES ===
        
        /// <summary>
        /// Get all structures for scheme (most common case).
        /// </summary>
        Task<List<RedbStructure>> GetStructuresBySchemeIdAsync(long schemeId);
        
        /// <summary>
        /// Get structures for .NET type.
        /// </summary>
        Task<List<RedbStructure>> GetStructuresByTypeAsync<TProps>() where TProps : class;
        
        /// <summary>
        /// Get structures for .NET type.
        /// </summary>
        Task<List<RedbStructure>> GetStructuresByTypeAsync(Type type);
        
        /// <summary>
        /// Get structure by ID.
        /// </summary>
        Task<RedbStructure?> GetStructureByIdAsync(long structureId);
        
        /// <summary>
        /// Get structure by name within scheme.
        /// </summary>
        Task<RedbStructure?> GetStructureByNameAsync(long schemeId, string structureName);
        
        /// <summary>
        /// Get name -> structure map for scheme (optimization for frequent lookups).
        /// </summary>
        Task<Dictionary<string, RedbStructure>> GetStructuresMapBySchemeIdAsync(long schemeId);
        
        // === SET/UPDATE STRUCTURES ===
        
        /// <summary>
        /// Add or update structure in cache.
        /// </summary>
        void SetStructure(RedbStructure structure);
        
        /// <summary>
        /// Add or update all structures for scheme in cache.
        /// </summary>
        void SetStructuresForScheme(long schemeId, List<RedbStructure> structures);
        
        /// <summary>
        /// Add or update structures for type in cache.
        /// </summary>
        void SetStructuresForType<TProps>(List<RedbStructure> structures) where TProps : class;
        
        /// <summary>
        /// Add or update structures for type in cache.
        /// </summary>
        void SetStructuresForType(Type type, List<RedbStructure> structures);
        
        // === INVALIDATION ===
        
        /// <summary>
        /// Remove structure from cache by ID.
        /// </summary>
        void InvalidateStructure(long structureId);
        
        /// <summary>
        /// Remove all structures for scheme from cache.
        /// </summary>
        void InvalidateStructuresForScheme(long schemeId);
        
        /// <summary>
        /// Remove structures for type from cache.
        /// </summary>
        void InvalidateStructuresForType<TProps>() where TProps : class;
        
        /// <summary>
        /// Remove structures for type from cache.
        /// </summary>
        void InvalidateStructuresForType(Type type);
        
        /// <summary>
        /// Clear all structure cache.
        /// </summary>
        void InvalidateAll();
        
        // === STATISTICS ===
        
        /// <summary>
        /// Get structure cache statistics.
        /// </summary>
        StructureCacheStatistics GetStatistics();
        
        /// <summary>
        /// Get all cached structures (for diagnostics).
        /// </summary>
        Dictionary<long, RedbStructure> GetAllCachedStructures();
        
        /// <summary>
        /// Get scheme to structures map (for diagnostics).
        /// </summary>
        Dictionary<long, List<RedbStructure>> GetSchemeToStructuresMap();
    }
    
    /// <summary>
    /// Structure cache statistics.
    /// </summary>
    public class StructureCacheStatistics
    {
        public long Hits { get; set; }
        public long Misses { get; set; }
        public long TotalRequests => Hits + Misses;
        public double HitRatio => TotalRequests > 0 ? (double)Hits / TotalRequests : 0.0;
        public int CachedStructuresCount { get; set; }
        public int CachedSchemesCount { get; set; }
        public int TypeMappingsCount { get; set; }
        public long EstimatedSizeBytes { get; set; }
        public DateTime LastAccessTime { get; set; }
        public DateTime CreatedTime { get; set; }
        public Dictionary<string, long> RequestsByType { get; set; } = new();
        public Dictionary<long, long> RequestsByScheme { get; set; } = new();
    }
}
