using redb.Core.Models.Entities;
using System;
using System.Collections.Concurrent;

namespace redb.Core.Caching
{
    /// <summary>
    /// Interface for static metadata cache in RedbObject.
    /// Implements user's suggestion for storing cache in static fields.
    /// </summary>
    public interface IStaticMetadataCache
    {
        /// <summary>
        /// Get scheme for .NET type from static cache.
        /// </summary>
        RedbScheme? GetSchemeForType<TProps>() where TProps : class;
        
        /// <summary>
        /// Get scheme for .NET type from static cache.
        /// </summary>
        RedbScheme? GetSchemeForType(Type type);
        
        /// <summary>
        /// Set scheme for .NET type in static cache.
        /// </summary>
        void SetSchemeForType<TProps>(RedbScheme scheme) where TProps : class;
        
        /// <summary>
        /// Set scheme for .NET type in static cache.
        /// </summary>
        void SetSchemeForType(Type type, RedbScheme scheme);
        
        /// <summary>
        /// Get structure by ID from static cache.
        /// </summary>
        RedbStructure? GetStructure(long structureId);
        
        /// <summary>
        /// Set structure in static cache.
        /// </summary>
        void SetStructure(RedbStructure structure);
        
        /// <summary>
        /// Get type by ID from static cache.
        /// </summary>
        RedbType? GetType(long typeId);
        
        /// <summary>
        /// Set type in static cache.
        /// </summary>
        void SetType(RedbType type);
        
        /// <summary>
        /// Get complete metadata for type from static cache.
        /// </summary>
        CompleteSchemeMetadata? GetCompleteMetadataForType<TProps>() where TProps : class;
        
        /// <summary>
        /// Set complete metadata for type in static cache.
        /// </summary>
        void SetCompleteMetadataForType<TProps>(CompleteSchemeMetadata metadata) where TProps : class;
        
        /// <summary>
        /// Clear all static cache (caution - affects all instances!).
        /// </summary>
        void ClearAll();
        
        /// <summary>
        /// Remove scheme for type from static cache.
        /// </summary>
        void RemoveSchemeForType<TProps>() where TProps : class;
        
        /// <summary>
        /// Get static cache statistics.
        /// </summary>
        StaticCacheStatistics GetStatistics();
    }
    
    /// <summary>
    /// Static metadata cache implementation.
    /// Uses thread-safe ConcurrentDictionary in static fields.
    /// </summary>
    public class StaticMetadataCache : IStaticMetadataCache
    {
        private static readonly ConcurrentDictionary<Type, RedbScheme> _schemesByType = new();
        private static readonly ConcurrentDictionary<long, RedbStructure> _structuresById = new();
        private static readonly ConcurrentDictionary<long, RedbType> _typesById = new();
        private static readonly ConcurrentDictionary<Type, CompleteSchemeMetadata> _completeMetadataByType = new();
            
        private static long _totalGets = 0;
        private static long _totalSets = 0;
        private static long _cacheHits = 0;
        private static long _cacheMisses = 0;
        
        public RedbScheme? GetSchemeForType<TProps>() where TProps : class
        {
            return GetSchemeForType(typeof(TProps));
        }
        
        public RedbScheme? GetSchemeForType(Type type)
        {
            System.Threading.Interlocked.Increment(ref _totalGets);
            
            if (_schemesByType.TryGetValue(type, out var scheme))
            {
                System.Threading.Interlocked.Increment(ref _cacheHits);
                return scheme;
            }
            
            System.Threading.Interlocked.Increment(ref _cacheMisses);
            return null;
        }
        
        public void SetSchemeForType<TProps>(RedbScheme scheme) where TProps : class
        {
            SetSchemeForType(typeof(TProps), scheme);
        }
        
        public void SetSchemeForType(Type type, RedbScheme scheme)
        {
            System.Threading.Interlocked.Increment(ref _totalSets);
            _schemesByType.TryAdd(type, scheme);
        }
        
        public RedbStructure? GetStructure(long structureId)
        {
            System.Threading.Interlocked.Increment(ref _totalGets);
            
            if (_structuresById.TryGetValue(structureId, out var structure))
            {
                System.Threading.Interlocked.Increment(ref _cacheHits);
                return structure;
            }
            
            System.Threading.Interlocked.Increment(ref _cacheMisses);
            return null;
        }
        
        public void SetStructure(RedbStructure structure)
        {
            System.Threading.Interlocked.Increment(ref _totalSets);
            _structuresById.TryAdd(structure.Id, structure);
        }
        
        public RedbType? GetType(long typeId)
        {
            System.Threading.Interlocked.Increment(ref _totalGets);
            
            if (_typesById.TryGetValue(typeId, out var type))
            {
                System.Threading.Interlocked.Increment(ref _cacheHits);
                return type;
            }
            
            System.Threading.Interlocked.Increment(ref _cacheMisses);
            return null;
        }
        
        public void SetType(RedbType type)
        {
            System.Threading.Interlocked.Increment(ref _totalSets);
            _typesById.TryAdd(type.Id, type);
        }
        
        public CompleteSchemeMetadata? GetCompleteMetadataForType<TProps>() where TProps : class
        {
            var type = typeof(TProps);
            System.Threading.Interlocked.Increment(ref _totalGets);
            
            if (_completeMetadataByType.TryGetValue(type, out var metadata))
            {
                System.Threading.Interlocked.Increment(ref _cacheHits);
                metadata.MarkAsUsed();
                return metadata;
            }
            
            System.Threading.Interlocked.Increment(ref _cacheMisses);
            return null;
        }
        
        public void SetCompleteMetadataForType<TProps>(CompleteSchemeMetadata metadata) where TProps : class
        {
            var type = typeof(TProps);
            metadata.AssociatedType = type;
            System.Threading.Interlocked.Increment(ref _totalSets);
            _completeMetadataByType.TryAdd(type, metadata);
        }
        
        public void ClearAll()
        {
            _schemesByType.Clear();
            _structuresById.Clear();
            _typesById.Clear();
            _completeMetadataByType.Clear();
            
            System.Threading.Interlocked.Exchange(ref _totalGets, 0);
            System.Threading.Interlocked.Exchange(ref _totalSets, 0);
            System.Threading.Interlocked.Exchange(ref _cacheHits, 0);
            System.Threading.Interlocked.Exchange(ref _cacheMisses, 0);
        }
        
        public void RemoveSchemeForType<TProps>() where TProps : class
        {
            var type = typeof(TProps);
            _schemesByType.TryRemove(type, out _);
            _completeMetadataByType.TryRemove(type, out _);
        }
        
        public StaticCacheStatistics GetStatistics()
        {
            return new StaticCacheStatistics
            {
                TotalGets = System.Threading.Interlocked.Read(ref _totalGets),
                TotalSets = System.Threading.Interlocked.Read(ref _totalSets),
                CacheHits = System.Threading.Interlocked.Read(ref _cacheHits),
                CacheMisses = System.Threading.Interlocked.Read(ref _cacheMisses),
                CachedSchemesCount = _schemesByType.Count,
                CachedStructuresCount = _structuresById.Count,
                CachedTypesCount = _typesById.Count,
                CachedCompleteMetadataCount = _completeMetadataByType.Count,
                EstimatedMemoryUsageBytes = EstimateMemoryUsage()
            };
        }
        
        private long EstimateMemoryUsage()
        {
            const int averageSchemeSize = 200;
            const int averageStructureSize = 300;
            const int averageTypeSize = 100;
            const int averageCompleteMetadataSize = 2000;
            
            return (_schemesByType.Count * averageSchemeSize) +
                   (_structuresById.Count * averageStructureSize) +
                   (_typesById.Count * averageTypeSize) +
                   (_completeMetadataByType.Count * averageCompleteMetadataSize);
        }
    }
    
    /// <summary>
    /// Static cache statistics.
    /// </summary>
    public class StaticCacheStatistics
    {
        public long TotalGets { get; set; }
        public long TotalSets { get; set; }
        public long CacheHits { get; set; }
        public long CacheMisses { get; set; }
        public double HitRatio => (CacheHits + CacheMisses) > 0 ? (double)CacheHits / (CacheHits + CacheMisses) : 0.0;
        public int CachedSchemesCount { get; set; }
        public int CachedStructuresCount { get; set; }
        public int CachedTypesCount { get; set; }
        public int CachedCompleteMetadataCount { get; set; }
        public int TotalCachedItems => CachedSchemesCount + CachedStructuresCount + CachedTypesCount + CachedCompleteMetadataCount;
        public long EstimatedMemoryUsageBytes { get; set; }
        public double MemoryUsageMB => EstimatedMemoryUsageBytes / (1024.0 * 1024.0);
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        public override string ToString()
        {
            return $"StaticCache: {TotalCachedItems} items, {HitRatio:P2} hit ratio, {MemoryUsageMB:F2}MB";
        }
    }
}
