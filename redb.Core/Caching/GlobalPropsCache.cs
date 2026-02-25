using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using redb.Core.Models.Entities;

namespace redb.Core.Caching
{
    /// <summary>
    /// Domain-isolated cache data for props/objects.
    /// </summary>
    internal class PropsCacheDomain
    {
        public IRedbObjectCache? Cache { get; set; }
    }
    
    /// <summary>
    /// Domain-isolated cache for WHOLE RedbObject (not just Props!).
    /// Transparent to business code: if disabled (Cache == null), everything goes through DB.
    /// Instance is bound to specific domain, static data is shared.
    /// </summary>
    public sealed class GlobalPropsCache
    {
        private static readonly ConcurrentDictionary<string, PropsCacheDomain> _domains = new();
        private static readonly object _lock = new();
        
        private readonly string _domain;
        
        /// <summary>
        /// Domain identifier for this cache instance.
        /// </summary>
        public string Domain => _domain;
        
        /// <summary>
        /// Create cache instance for specific domain.
        /// </summary>
        public GlobalPropsCache(string? domain = null)
        {
            _domain = domain ?? "default";
        }
        
        private PropsCacheDomain GetCache() => _domains.GetOrAdd(_domain, _ => new PropsCacheDomain());
        
        /// <summary>
        /// Get underlying cache instance for this domain.
        /// If null - cache is disabled, everything goes through DB.
        /// </summary>
        public IRedbObjectCache? Instance => GetCache().Cache;
        
        /// <summary>
        /// Initialize cache for this domain (called once at application startup per domain).
        /// </summary>
        public void Initialize(IRedbObjectCache cache)
        {
            lock (_lock)
            {
                GetCache().Cache = cache;
            }
        }
        
        /// <summary>
        /// Get WHOLE RedbObject with hash validation.
        /// </summary>
        public RedbObject<TProps>? Get<TProps>(long objectId, Guid hash) where TProps : class, new()
        {
            return Instance?.Get<TProps>(objectId, hash);
        }
        
        /// <summary>
        /// Get WHOLE RedbObject WITHOUT hash validation (for monolithic applications).
        /// </summary>
        public RedbObject<TProps>? GetWithoutHashValidation<TProps>(long objectId) where TProps : class, new()
        {
            return Instance?.GetWithoutHashValidation<TProps>(objectId);
        }
        
        /// <summary>
        /// Save WHOLE RedbObject to cache.
        /// </summary>
        public void Set<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            Instance?.Set(obj);
        }
        
        /// <summary>
        /// BULK: determine which objects need to be loaded from DB (set difference).
        /// Returns cached WHOLE RedbObject instances.
        /// </summary>
        public HashSet<long> FilterNeedToLoad<TProps>(
            List<(long objectId, Guid hash)> objects,
            out Dictionary<long, RedbObject<TProps>> fromCache) where TProps : class, new()
        {
            if (Instance != null)
            {
                return Instance.FilterNeedToLoad(objects, out fromCache);
            }
            
            // Cache is disabled - load everything from DB
            fromCache = new Dictionary<long, RedbObject<TProps>>();
            return objects.Select(o => o.objectId).ToHashSet();
        }
        
        /// <summary>
        /// Remove from cache.
        /// </summary>
        public void Remove(long objectId)
        {
            Instance?.Remove(objectId);
        }
        
        /// <summary>
        /// Clear cache for this domain.
        /// </summary>
        public void Clear()
        {
            Instance?.Clear();
        }
        
        /// <summary>
        /// Get cache statistics for this domain.
        /// </summary>
        public PropsCacheStatistics GetStats()
        {
            return Instance?.GetStats() ?? new PropsCacheStatistics();
        }
    }
}
