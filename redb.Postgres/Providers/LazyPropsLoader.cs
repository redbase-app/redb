using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using redb.Core.Caching;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
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
        private readonly ConcurrentDictionary<long, byte> _loadingInProgress = new();
        
        private readonly IRedbContext _context;
        private readonly IRedbObjectSerializer _serializer;
        private readonly RedbServiceConfiguration _config;
        private readonly ISqlDialect _sql;
        private readonly GlobalPropsCache _propsCache;
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
        public TProps LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new()
        {
            return LoadPropsAsync<TProps>(objectId, schemeId)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }
        
        /// <summary>
        /// Async Props loading for single object via get_object_json.
        /// Simple and efficient for OpenSource version.
        /// </summary>
        public async Task<TProps> LoadPropsAsync<TProps>(long objectId, long schemeId) where TProps : class, new()
        {
            // 1. Check cache first
            if (_config.EnablePropsCache && _propsCache.Instance != null)
            {
                var hashes = await _context.QueryScalarListAsync<Guid>(
                    _sql.LazyLoader_SelectObjectHash(), objectId);
                
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
                _sql.LazyLoader_GetObjectJson(), objectId, 10);
            var json = jsonResults.FirstOrDefault();

            if (string.IsNullOrEmpty(json))
                throw new InvalidOperationException($"Object {objectId} not found");

            // 3. Deserialize
            var obj = _serializer.Deserialize<TProps>(json);

            // 4. Cache the result
            if (_config.EnablePropsCache && obj.hash.HasValue)
            {
                _propsCache.Set(obj);
            }

            return obj.Props;
        }
        
        /// <summary>
        /// BULK Props loading for multiple objects via get_object_json batch.
        /// Uses unnest for efficient batch loading.
        /// </summary>
        public async Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects) where TProps : class, new()
        {
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
                        _sql.LazyLoader_GetObjectJsonBatch(), idsToLoad);

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

                                // Cache
                        if (_config.EnablePropsCache && obj.hash.HasValue)
                        {
                            _propsCache.Set(obj);
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
            HashSet<long>? projectedStructureIds) where TProps : class, new()
        {
            // OpenSource: ignore projection, load full objects
            return LoadPropsForManyAsync(objects);
        }

        /// <summary>
        /// BULK Props loading with custom depth for nested RedbObject.
        /// In OpenSource version, propsDepth is ignored — uses get_object_json with default depth.
        /// </summary>
        public Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects,
            int? propsDepth) where TProps : class, new()
        {
            // OpenSource: ignore propsDepth, use default get_object_json
            return LoadPropsForManyAsync(objects);
        }

        /// <summary>
        /// BULK Props loading with projection filter and custom depth.
        /// In OpenSource version, both parameters are ignored — full objects loaded via get_object_json.
        /// </summary>
        public Task LoadPropsForManyAsync<TProps>(
            List<RedbObject<TProps>> objects,
            HashSet<long>? projectedStructureIds,
            int? propsDepth) where TProps : class, new()
        {
            // OpenSource: ignore projection and depth, load full objects
            return LoadPropsForManyAsync(objects);
        }

        /// <summary>
        /// BULK loading for polymorphic objects (different schemes).
        /// Each object is deserialized to its own type based on scheme_id.
        /// </summary>
        public async Task LoadPropsForManyPolymorphicAsync(List<IRedbObject> objects)
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
                
                // Batch query
                var results = await _context.QueryAsync<ObjectJsonResult>(
                    _sql.LazyLoader_GetObjectJsonBatch(), idsToLoad);

                var jsonById = results.ToDictionary(r => r.Id, r => r.JsonData);

                foreach (var obj in objects)
                {
                    if (jsonById.TryGetValue(obj.Id, out var json) && !string.IsNullOrEmpty(json))
                    {
                        try
                        {
                            // Get Props type from object's generic parameter
                            var objType = obj.GetType();
                            if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(RedbObject<>))
                            {
                                var propsType = objType.GetGenericArguments()[0];
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
