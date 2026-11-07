using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using redb.Core.Models.Contracts;
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
    /// <remarks>
    /// A cached graph is one instance shared by every scope. It carries nothing scope-bound (owner decision 2026-09-15, plan
    /// docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md §4.1): its reference stubs hold the scope-free loader of their database and
    /// load on the live scope of whoever reads them; <see cref="Set{TProps}"/> only marks the graph shared, so a lazy load
    /// inside the reader's transaction keeps nothing. The detached loader swapped in on hand-out, and the dead-scope
    /// fallback installed on Set, are gone with the scope-bound loaders they compensated for.
    /// </remarks>
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
            CollectibleContextCleanup.EnsureWatching();
            lock (_lock)
            {
                GetCache().Cache = cache;
            }
        }

        /// <summary>
        /// Get WHOLE RedbObject with hash validation.
        /// </summary>
        public RedbObject<TProps>? Get<TProps>(long objectId, Guid hash) where TProps : class, new()
            => Instance?.Get<TProps>(objectId, hash);

        /// <summary>
        /// Get WHOLE RedbObject WITHOUT hash validation (for monolithic applications).
        /// </summary>
        public RedbObject<TProps>? GetWithoutHashValidation<TProps>(long objectId) where TProps : class, new()
            => Instance?.GetWithoutHashValidation<TProps>(objectId);

        /// <summary>
        /// Save WHOLE RedbObject to cache. From here on the graph is shared by every scope.
        /// </summary>
        public void Set<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            var cache = Instance;
            if (cache == null || !obj.hash.HasValue) return;
            cache.Set(obj);
            Utils.LazyReferenceInstaller.MarkShared(obj);
        }

        /// <summary>
        /// Save WHOLE RedbObject and every loaded object under it. One walk marks the graph shared and collects the
        /// nested entries, where a Set followed by <see cref="SetNested"/> walked the same Props twice.
        /// </summary>
        public void SetGraph<TProps>(RedbObject<TProps> obj) where TProps : class, new()
        {
            var cache = Instance;
            if (cache == null || !obj.hash.HasValue) return;
            cache.Set(obj);
            foreach (var nested in Utils.LazyReferenceInstaller.MarkSharedAndCollect(obj))
            {
                if (ReferenceEquals(nested, obj)) continue;
                CacheDelegates.Set(cache, nested);
            }
        }

        /// <summary>
        /// Caches every loaded RedbObject found under <paramref name="props"/> by its own id, so a nested object is
        /// served directly by id as well; the entries only (the Set of the root marked the graph shared already).
        /// Stubs are neither cached nor walked.
        /// </summary>
        public void SetNested(object props)
        {
            var cache = Instance;
            if (cache == null || props == null) return;
            CacheNestedObjectsInternal(cache, props, new HashSet<object>(ReferenceEqualityComparer.Instance));
        }

        /// <summary>
        /// Internal method for recursive caching with tracking of visited objects
        /// </summary>
        private static void CacheNestedObjectsInternal(IRedbObjectCache cache, object obj, HashSet<object> visited)
        {
            if (obj == null) return;

            var objType = obj.GetType();

            // CRITICAL: Check TYPE IMMEDIATELY before visited.Add()!
            // This prevents infinite boxing for value types (DateTime, int, etc.)
            if (objType.IsPrimitive || objType.IsValueType || objType.Namespace?.StartsWith("System") == true)
            {
                return;
            }

            // A list item is a leaf: reading its lazy Object property IS a database load, so the
            // reflective property walk below must never descend into one (stand, 2026-09-09).
            if (redb.Core.Utils.LazyReferenceInstaller.IsWalkLeaf(obj))
            {
                return;
            }

            // Only AFTER type check add to visited (protection against circular references)
            if (!visited.Add(obj))
            {
                return;
            }

            // If this is a RedbObject<T> (or a TreeRedbObject<T>) → cache it
            var genericObjType = Utils.RedbObjectTypes.GenericOf(objType);
            if (genericObjType != null)
            {
                var redbObj = obj as IRedbObject;
                // V4 (L.3): a reference stub is neither cached nor walked - reading its Props here
                // would BE the lazy load: synchronous, and for the whole reachable graph (review).
                if (redbObj != null && redbObj.Hash.HasValue && obj is RedbObject { IsPropsLoaded: true })
                {
                    // The entry only: the Set of the root marked the whole graph shared already, and a nested Set
                    // through GlobalPropsCache walked the subtree again for every nested object (review after 4.0.0).
                    var propsType = genericObjType.GetGenericArguments()[0];
                    var setMethod = typeof(IRedbObjectCache).GetMethod("Set")?.MakeGenericMethod(propsType);
                    setMethod?.Invoke(cache, new[] { obj });

                    // Raw Props, never the getter
                    var propsValue = objType.GetMethod("GetPropsDirectly")?.Invoke(obj, null);
                    if (propsValue != null)
                    {
                        CacheNestedObjectsInternal(cache, propsValue, visited);
                    }
                }
                return;
            }

            // Process arrays
            if (obj is Array array)
            {
                foreach (var item in array)
                {
                    if (item != null)
                    {
                        CacheNestedObjectsInternal(cache, item, visited);
                    }
                }
                return;
            }

            // Process collections (IEnumerable, except string)
            if (obj is System.Collections.IEnumerable enumerable && obj is not string)
            {
                foreach (var item in enumerable)
                {
                    if (item != null)
                    {
                        CacheNestedObjectsInternal(cache, item, visited);
                    }
                }
                return;
            }

            // Process business class properties (Address, Contact, Details, etc.)
            var properties = objType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var prop in properties)
            {
                // The save's property set: not an indexer, not [RedbIgnore].
                if (!Utils.LazyReferenceInstaller.IsWalkedProperty(prop)) continue;

                var propValue = prop.GetValue(obj);
                if (propValue != null)
                {
                    CacheNestedObjectsInternal(cache, propValue, visited);
                }
            }
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
                return Instance.FilterNeedToLoad(objects, out fromCache);

            // Cache is disabled - load everything from DB
            fromCache = new Dictionary<long, RedbObject<TProps>>();
            return objects.Select(o => o.objectId).ToHashSet();
        }

        /// <summary>
        /// Remove from cache.
        /// </summary>
        /// <summary>
        /// Drops, in every domain, the cached objects whose type <paramref name="unloading"/> answers true for: the
        /// types of a collectible AssemblyLoadContext that unloads (<see cref="CollectibleContextCleanup"/>).
        /// </summary>
        internal static void RemoveObjectsOfTypes(Func<Type, bool> unloading)
        {
            foreach (var domain in _domains.Values)
                domain.Cache?.RemoveObjectsOfTypes(unloading);
        }

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
