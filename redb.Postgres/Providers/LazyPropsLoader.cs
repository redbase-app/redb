using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Caching;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Exceptions;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Query;
using redb.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace redb.Postgres.Providers
{
    /// <summary>
    /// OpenSource implementation of lazy Props loading for PostgreSQL.
    /// Uses get_object_json SQL function for simple and efficient loading.
    /// Pro version uses PropsMaterializer with PVT optimizations.
    /// </summary>
    public class LazyPropsLoader : ILazyPropsLoader
    {
        // get_object_json depth the batch uses when the caller names none (query pages, tree loads).
        private const int DefaultBatchDepth = 10;
        private readonly ConcurrentDictionary<long, byte> _loadingInProgress = new();
        
        private readonly IRedbContext _context;
        private readonly IRedbObjectSerializer _serializer;
        private readonly RedbServiceConfiguration _config;
        private readonly ISqlDialect _sql;
        private readonly GlobalPropsCache _propsCache;

        /// <inheritdoc />
        public string? CacheDomain => _propsCache.Domain;

        /// <inheritdoc />
        public IRedbContext? ScopeContext => _context;
        private readonly ILogger? _logger;
        
        public LazyPropsLoader(
            IRedbContext context,
            ISchemeSyncProvider schemeSync,
            IRedbObjectSerializer serializer,
            RedbServiceConfiguration config,
            IListProvider? listProvider = null,
            ILogger? logger = null)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _propsCache = schemeSync?.PropsCache ?? throw new ArgumentNullException(nameof(schemeSync));
            _sql = new Sql.PostgreSqlDialect();
            _logger = logger;
        }
        
        /// <summary>
        /// Synchronous Props loading (for getter).
        /// </summary>
        public TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new()
        {
            if (_config.LazyReferenceAccess == LazyReferenceAccessMode.Throw)
                throw new RedbSynchronousLazyLoadException(objectId, schemeId);

            // Blocking by design (the getter is synchronous), but never on the caller's context: the load
            // runs on the thread pool, so a host with a SynchronizationContext (Blazor Server, WPF, MAUI)
            // waits instead of deadlocking on its own continuations (review). The caller blocks meanwhile,
            // so the context stays single-threaded; ExecutionContext (the AsyncLocal scope) flows in.
            return Task.Run(() => LoadPropsAsync<TProps>(objectId, schemeId)).GetAwaiter().GetResult();
        }
        
        /// <summary>
        /// Async Props loading for single object via get_object_json.
        /// Simple and efficient for OpenSource version.
        /// </summary>
        public async Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            if (_context.IsDisposed)
                throw new RedbLazyLoadScopeEndedException(objectId, schemeId);

            // 1. Check cache first
            if (_config.EnablePropsCache && _propsCache.Instance != null)
            {
                var hashes = await _context.QueryScalarListAsync<Guid>(
                    _sql.LazyLoader_SelectObjectHash(), new object[] { objectId }, cancellationToken);
                
                if (hashes.Count > 0)
                {
                    var hash = hashes[0];
                    var cached = _propsCache.Get<TProps>(objectId, hash);
                    if (cached != null)
                    {
                        var propsFromCache = cached.GetPropsDirectly();
                        if (propsFromCache != null)
                            return propsFromCache;
                    }
                }
            }

            // 2. Load via get_object_json — ONE query, simple!
            var jsonResults = await _context.QueryScalarListAsync<string>(
                _sql.LazyLoader_GetObjectJson(), new object[] { objectId, 1 }, cancellationToken); // V4 (L.3): the reloaded object's own references are stubs again
            var json = jsonResults.FirstOrDefault();

            if (string.IsNullOrEmpty(json))
                return null; // V4 (L.3): a reference whose target left for the trash reloads as null Props, not an exception

            // 3. Deserialize
            var obj = _serializer.Deserialize<TProps>(json);
            redb.Core.Utils.LazyReferenceInstaller.Install(obj, this); // V4 (L.3): nested stubs get their loader

            // 4. Cache the result
            if (_config.EnablePropsCache && obj.hash.HasValue)
            {
                redb.Core.Caching.CachePublication.AfterCommit(_context, () => _propsCache.Set(obj));
            }

            // Raw access instead of the getter: for an object with properties:null (values wiped by a


            // race, or a bare object) Install has attached the loader to the root itself, so the Props


            // getter would start a new load - endless recursion eating a pool thread per turn.
            return obj.GetPropsDirectly();
        }
        
        /// <summary>
        /// BULK Props loading for multiple objects via get_object_json batch.
        /// Uses unnest for efficient batch loading.
        /// </summary>
        public async Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var depth = propsDepth ?? DefaultBatchDepth;
            if (objects.Count == 0) return;
            
            // Protection from infinite recursion
            var uniqueIds = objects.Select(o => o.id).Distinct().ToList();
            var alreadyLoading = uniqueIds.Where(id => _loadingInProgress.ContainsKey(id)).ToList();
            
            if (alreadyLoading.Any())
            {
                objects = objects.Where(o => !alreadyLoading.Contains(o.id)).ToList();
                uniqueIds = uniqueIds.Except(alreadyLoading).ToList();
                if (objects.Count == 0) return;
            }
            
            foreach (var id in uniqueIds)
                _loadingInProgress.TryAdd(id, 0);
            
            try
            {
            HashSet<long> needToLoad;
            Dictionary<long, RedbObject<TProps>> fromCache;
            
                // 1. Check cache
            if (_config.EnablePropsCache && _propsCache.Instance != null)
            {
                var objectsData = objects
                    .Where(o => o.hash.HasValue)
                        .Select(o => (o.id, o.hash!.Value))
                    .ToList();
                
                needToLoad = _propsCache.FilterNeedToLoad<TProps>(objectsData, out fromCache);

                // An object without a hash cannot be matched by the cache: it loads from the database, never dropped.
                foreach (var obj in objects)
                    if (!obj.hash.HasValue)
                        needToLoad.Add(obj.id);
                
                foreach (var obj in objects)
                {
                    if (fromCache.TryGetValue(obj.id, out var cachedObj))
                    {
                        obj.Props = cachedObj.Props;
                        obj._propsLoaded = true;
                        obj._lazyLoader = null;
                    }
                }
            }
            else
            {
                needToLoad = objects.Select(o => o.id).ToHashSet();
                fromCache = new Dictionary<long, RedbObject<TProps>>();
            }
            
                // 2. Load from DB via get_object_json batch
            if (needToLoad.Count > 0)
                {
                    var idsToLoad = needToLoad.ToArray();
                    
                    // Batch query via unnest + get_object_json
                    var results = await _context.QueryAsync<ObjectJsonResult>(
                        _sql.LazyLoader_GetObjectJsonBatch(), new object[] { idsToLoad, depth }, cancellationToken);

                    var jsonById = results.ToDictionary(r => r.Id, r => r.JsonData);

                    foreach (var obj in objects.Where(o => needToLoad.Contains(o.id)))
                    {
                        if (jsonById.TryGetValue(obj.id, out var json) && !string.IsNullOrEmpty(json))
                        {
                            try
                            {
                                var loaded = _serializer.Deserialize<TProps>(json);
                                
                                // Copy Props from loaded object
                                obj.Props = loaded.Props;
                                obj._propsLoaded = true;
                                obj._lazyLoader = null;
                                redb.Core.Utils.LazyReferenceInstaller.Install(obj, this); // V4 (L.3)

                                // Cache
                        if (_config.EnablePropsCache && obj.hash.HasValue)
                        {
                            redb.Core.Caching.CachePublication.AfterCommit(_context, () => _propsCache.Set(obj));
                                }
                            }
                            catch (Exception)
                            {
                                // Skip failed deserialization, leave Props as default
                            }
                        }
                    }
            }
            }
            finally
            {
                foreach (var id in uniqueIds)
                    _loadingInProgress.TryRemove(id, out _);
            }
        }

        /// <summary>
        /// BULK Props loading with projection filter (for Select projections).
        /// In OpenSource version, projection is ignored — full objects loaded via get_object_json.
        /// </summary>
        public Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects, 
            HashSet<long>? projectedStructureIds, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            // OpenSource: ignore projection, load full objects
            return LoadPropsForManyAsync(objects, propsDepth: null);
        }

        /// <summary>
        /// BULK Props loading at the default batch depth; the depth form above is the core.
        /// A stub reload passes 1 (transitive laziness), query pages their PropsDepth.
        /// </summary>
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new()
            => LoadPropsForManyAsync(objects, propsDepth: null);

        /// <summary>
        /// BULK Props loading with projection filter and custom depth.
        /// In OpenSource version the projection is ignored (full objects); the depth is honoured.
        /// </summary>
        public Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects,
            HashSet<long>? projectedStructureIds,
            int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            // OpenSource: ignore projection; the depth is honoured
            return LoadPropsForManyAsync(objects, propsDepth);
        }

        /// <summary>
        /// BULK loading for polymorphic objects (different schemes).
        /// Each object is deserialized to its own type based on scheme_id.
        /// </summary>
        public async Task LoadPropsForManyPolymorphicAsync(List<IRedbObject> objects, CancellationToken cancellationToken = default)
        {
            if (objects.Count == 0) return;

            var uniqueIds = objects.Select(o => o.Id).Distinct().ToList();
            var alreadyLoading = uniqueIds.Where(id => _loadingInProgress.ContainsKey(id)).ToList();

            if (alreadyLoading.Any())
            {
                objects = objects.Where(o => !alreadyLoading.Contains(o.Id)).ToList();
                uniqueIds = uniqueIds.Except(alreadyLoading).ToList();
                if (objects.Count == 0) return;
            }

            foreach (var id in uniqueIds)
                _loadingInProgress.TryAdd(id, 0);

            try
            {
                var idsToLoad = uniqueIds.ToArray();
                
                var depth = DefaultBatchDepth;
                
                // Batch query
                var results = await _context.QueryAsync<ObjectJsonResult>(
                    _sql.LazyLoader_GetObjectJsonBatch(), new object[] { idsToLoad, depth }, cancellationToken);

                var jsonById = results.ToDictionary(r => r.Id, r => r.JsonData);

                foreach (var obj in objects)
                {
                    if (jsonById.TryGetValue(obj.Id, out var json) && !string.IsNullOrEmpty(json))
                    {
                        try
                        {
                            // Get Props type from object's generic parameter (the type or its RedbObject<T> base)
                            var objType = obj.GetType();
                            var genericObjType = redb.Core.Utils.RedbObjectTypes.GenericOf(objType);
                            if (genericObjType != null)
                            {
                                var propsType = genericObjType.GetGenericArguments()[0];
                                var loaded = _serializer.DeserializeDynamic(json, propsType);
                                
                                // Copy Props via reflection
                                var propsProperty = objType.GetProperty("Props");
                                var loadedProps = loaded.GetType().GetProperty("Props")?.GetValue(loaded);
                                propsProperty?.SetValue(obj, loadedProps);
                                
                                // Mark as loaded
                                var loadedField = objType.GetField("_propsLoaded");
                                loadedField?.SetValue(obj, true);
                                
                                var loaderField = objType.GetField("_lazyLoader");
                                loaderField?.SetValue(obj, null);
                            }
                        }
                        catch (Exception)
                        {
                            // Skip failed deserialization
                        }
                    }
                }
            }
            finally
            {
                foreach (var id in uniqueIds)
                    _loadingInProgress.TryRemove(id, out _);
            }
        }
    }

    /// <summary>
    /// DTO for batch get_object_json results.
    /// </summary>
    internal class ObjectJsonResult
    {
        public long Id { get; set; }
        public string JsonData { get; set; } = string.Empty;
    }
}
