using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Models.Entities;

namespace redb.Core.Caching
{
    /// <summary>
    /// Domain-isolated cache data for props/objects.
    /// </summary>
    internal class PropsCacheDomain
    {
        public IRedbObjectCache? Cache { get; set; }

        /// <summary>
        /// V4 (review): the loader every reference inside a cached object carries - one that opens
        /// its own scope per load, because a cached instance is shared by every scope. Null without DI.
        /// </summary>
        public Providers.DetachedLazyPropsLoader? DetachedLoader { get; set; }
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
        public void Initialize(IRedbObjectCache cache, IServiceScopeFactory? scopeFactory = null)
        {
            lock (_lock)
            {
                var domain = GetCache();
                domain.Cache = cache;
                domain.DetachedLoader = scopeFactory != null ? new Providers.DetachedLazyPropsLoader(scopeFactory) : null;
            }
        }
        
        /// <summary>
        /// Get WHOLE RedbObject with hash validation.
        /// </summary>
        public RedbObject<TProps>? Get<TProps>(long objectId, Guid hash) where TProps : class, new()
        {
            return DetachOnHandOut(Instance?.Get<TProps>(objectId, hash));
        }
        
        /// <summary>
        /// Get WHOLE RedbObject WITHOUT hash validation (for monolithic applications).
        /// </summary>
        public RedbObject<TProps>? GetWithoutHashValidation<TProps>(long objectId) where TProps : class, new()
        {
            return DetachOnHandOut(Instance?.GetWithoutHashValidation<TProps>(objectId));
        }
        
        /// <summary>
        /// Save WHOLE RedbObject to cache.
        /// </summary>
        public void Set<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            var domain = GetCache();
            if (domain.Cache == null) return;
            domain.Cache.Set(obj);
            // The writer keeps its scoped loaders, wrapped with a dead-scope fallback: while its
            // scope lives every reference touch rides the writer's own connection (detaching here
            // made each touch rent a fresh scope and drained the pool - tsum, 2026-09-09); once
            // the scope dies the wrapper falls through to the detached loader. A graph handed OUT
            // of the cache still gets the pure detached loader - see DetachOnHandOut.
            if (domain.DetachedLoader != null)
                Utils.LazyReferenceInstaller.InstallForCacheSet(obj, domain.DetachedLoader);
            // A (re-)Set may bring fresh stubs carrying the writer's loader: drop the served-once
            // mark so the next hand-out detaches the graph again.
            _detached.Remove(obj);
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
                var missing = Instance.FilterNeedToLoad(objects, out fromCache);
                foreach (var served in fromCache.Values)
                    DetachOnHandOut(served);
                return missing;
            }
            
            // Cache is disabled - load everything from DB
            fromCache = new Dictionary<long, RedbObject<TProps>>();
            return objects.Select(o => o.objectId).ToHashSet();
        }
        
        // Served-once marker: the reflection walk over a graph is not free, and a hot cache
        // serves the same instance thousands of times. The table entry lives exactly as long
        // as the cached instance does.
        private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<object, object> _detached = new();

        /// <summary>
        /// A graph leaving the cache is shared between scopes from that moment on: swap its lazy
        /// loaders to the domain's <see cref="Providers.DetachedLazyPropsLoader"/> (each lazy access
        /// then borrows a fresh scope). The WRITER's copy is deliberately not touched on Set - only
        /// what the cache hands out gets detached, once per instance.
        /// </summary>
        private T? DetachOnHandOut<T>(T? obj) where T : class, Models.Contracts.IRedbObject
        {
            if (obj == null) return null;
            var detachedLoader = GetCache().DetachedLoader;
            if (detachedLoader == null) return obj;
            if (!_detached.TryGetValue(obj, out _))
            {
                Utils.LazyReferenceInstaller.Install(obj, detachedLoader);
                _detached.AddOrUpdate(obj, string.Empty);
            }
            return obj;
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
