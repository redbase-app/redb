using redb.Core.Providers;
using redb.Core.Data;
using redb.Core.Utils;
using redb.Core.Extensions;
using redb.Core.Serialization;
using redb.Core.Query;
using redb.Core.Services;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Encodings.Web;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Configuration;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using redb.Core.Exceptions;
using System;
using redb.Core.Caching;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Base abstract class for ObjectStorageProvider with database-agnostic business logic.
    /// SQL queries are abstracted via ISqlDialect for PostgreSQL/MSSQL/etc support.
    /// </summary>
    public abstract partial class ObjectStorageProviderBase : IObjectStorageProvider
    {
        protected readonly IRedbContext _context;
        private readonly IRedbObjectSerializer _serializer;
        private readonly IPermissionProvider _permissionProvider;
        private readonly IRedbSecurityContext _securityContext;
        private readonly ISchemeSyncProvider _schemeSync;
        private readonly RedbServiceConfiguration _configuration;
        private readonly IListProvider? _listProvider;
        private readonly ISqlDialect _sql;
        private readonly ILogger? _logger;

        /// <summary>
        /// Domain-bound metadata cache for this provider.
        /// </summary>
        protected GlobalMetadataCache Cache => _schemeSync.Cache;

        /// <summary>
        /// Domain-bound props cache for this provider.
        /// </summary>
        protected GlobalPropsCache PropsCache => _schemeSync.PropsCache;

        /// <summary>
        /// Creates a new ObjectStorageProviderBase instance.
        /// </summary>
        protected ObjectStorageProviderBase(
            IRedbContext context,
            IRedbObjectSerializer serializer,
            IPermissionProvider permissionProvider,
            IRedbSecurityContext securityContext,
            ISchemeSyncProvider schemeSync,
            RedbServiceConfiguration configuration,
            ISqlDialect sql,
            IListProvider? listProvider = null,
            ILogger? logger = null,
            IEnumerable<Interception.IRedbSaveInterceptor>? saveInterceptors = null)
        {
            _context = context;
            _serializer = serializer;
            _permissionProvider = permissionProvider;
            _securityContext = securityContext;
            _schemeSync = schemeSync;
            _configuration = configuration ?? new RedbServiceConfiguration();
            _sql = sql;
            _listProvider = listProvider;
            _logger = logger;
            // Materialized once: interceptors run on every save, in registration order.
            _saveInterceptors = saveInterceptors?.ToArray() ?? [];
        }

        /// <summary>Save-pipeline interceptors (discussion #12, п.3-4). Empty array = zero cost.</summary>
        private readonly Interception.IRedbSaveInterceptor[] _saveInterceptors;

        /// <summary>True when at least one interceptor is registered — gates the change-feed collection.</summary>
        protected bool HasSaveInterceptors => _saveInterceptors.Length > 0;

        /// <summary>
        /// The change feed for the current save, or null when nobody listens. The base opens it
        /// before the strategy runs (ChangeTracking only); the Pro diff fills it; the base hands
        /// it to <see cref="Interception.RedbSavedContext.Changes"/>. Single-threaded by the
        /// one-scope-one-save contract (CT-4).
        /// </summary>
        protected List<Interception.RedbValueChange>? InterceptorChangeSink;

        protected async Task InvokeSavingInterceptorsAsync(Interception.RedbSavingContext ctx, CancellationToken cancellationToken)
        {
            foreach (var interceptor in _saveInterceptors)
                await interceptor.SavingAsync(ctx, cancellationToken);
        }

        protected async Task InvokeSavedInterceptorsAsync(Interception.RedbSavedContext ctx, CancellationToken cancellationToken)
        {
            foreach (var interceptor in _saveInterceptors)
                await interceptor.SavedAsync(ctx, cancellationToken);
        }

        private async Task InvokeDeletingInterceptorsAsync(Interception.RedbDeletingContext ctx, CancellationToken cancellationToken)
        {
            foreach (var interceptor in _saveInterceptors)
                await interceptor.DeletingAsync(ctx, cancellationToken);
        }

        private async Task InvokeDeletedInterceptorsAsync(Interception.RedbDeletedContext ctx, CancellationToken cancellationToken)
        {
            foreach (var interceptor in _saveInterceptors)
                await interceptor.DeletedAsync(ctx, cancellationToken);
        }

        // Protected properties for derived classes
        protected IRedbContext Context => _context;
        protected IRedbObjectSerializer Serializer => _serializer;
        protected ISchemeSyncProvider SchemeSyncProvider => _schemeSync;
        protected RedbServiceConfiguration Configuration => _configuration;
        protected IListProvider? ListProvider => _listProvider;
        protected ISqlDialect Sql => _sql;
        protected IPermissionProvider PermissionProvider => _permissionProvider;
        protected IRedbSecurityContext SecurityContext => _securityContext;
        protected ILogger? Logger => _logger;

        /// <summary>
        /// Creates a LazyPropsLoader instance. Override in derived classes for custom implementations (e.g., ProLazyPropsLoader).
        /// </summary>
        protected abstract ILazyPropsLoader CreateLazyPropsLoader();

        // ===== BASIC METHODS (use _securityContext and configuration) =====

        /// <summary>
        /// Load object from EAV by ID (uses _securityContext and config.DefaultCheckPermissionsOnLoad)
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false
        /// </summary>
        public async Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadAsync<TProps>(objectId, effectiveUser, depth, cancellationToken);
        }

        /// <summary>
        /// Load object from EAV (uses _securityContext and config.DefaultCheckPermissionsOnLoad)
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false
        /// </summary>
        public async Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadAsync<TProps>(obj.Id, effectiveUser, depth, cancellationToken);
        }

        /// <summary>
        /// Load object from EAV with explicitly specified user (uses config.DefaultCheckPermissionsOnLoad)
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false
        /// </summary>
        public async Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            return await LoadAsync<TProps>(obj.Id, user, depth, cancellationToken);
        }

        // ===== OVERLOADS WITH EXPLICIT USER =====

        /// <summary>
        /// MAIN loading method - all other LoadAsync methods call it
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false
        /// </summary>
        /// <summary>
        /// Checks whether a loaded object's scheme matches the scheme <typeparamref name="TProps"/> maps to.
        /// Returns <c>true</c> when it matches (or when the type has no known scheme yet — nothing to compare
        /// against). On a mismatch the behaviour depends on <c>ThrowOnSchemeMismatch</c>: by default returns
        /// <c>false</c> so the caller returns <c>null</c> (a soft-deleted object, scheme <c>-10</c>, reads as
        /// <c>null</c>); when the flag is set, throws <see cref="Exceptions.RedbSchemeMismatchException"/>.
        /// Either way garbage never reaches the cache — the check runs before any caching.
        /// </summary>
        private async Task<bool> IsSchemeValidForLoadAsync<TProps>(long objectId, long actualSchemeId) where TProps : class, new()
        {
            // Expected scheme_id for TProps: fast per-domain projection first, then a lazy resolve that
            // also warms the projection. Null → the type has no scheme yet; accept rather than block loads.
            var expected = Cache.GetSchemeIdByClrType(typeof(TProps));
            if (expected == null)
            {
                var scheme = await _schemeSync.GetSchemeByTypeAsync<TProps>();
                expected = scheme?.Id;
            }

            if (!expected.HasValue || expected.Value == actualSchemeId)
                return true;

            if (_configuration.ThrowOnSchemeMismatch)
                throw new Exceptions.RedbSchemeMismatchException(objectId, typeof(TProps), expected.Value, actualSchemeId);

            return false;   // soft default: caller returns null instead of garbage
        }

        public async Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            // Permission check according to configuration
            if (_configuration.DefaultCheckPermissionsOnLoad)
            {
                var canRead = await _permissionProvider.CanUserSelectObject(objectId, user.Id);
                if (!canRead)
                {
                    throw new UnauthorizedAccessException($"User {user.Id} has no read permission for object {objectId}");
                }
            }

            // === EAGER LOADING: Full loading via get_object_json with cache ===

            // OPTIMIZATION for SkipHashValidationOnCacheCheck: check cache first without DB query.
            // NEVER inside an active transaction (explicit or ambient): a load there is the read of
            // a read-modify-write - the caller has just taken LockForUpdateAsync and expects the
            // database's current row, and a pre-lock cached copy is exactly the lost update the
            // lock exists to prevent (bug report п.1, 2026-09-02). Inside a transaction the load
            // falls through to the hash-validated path: one indexed SELECT _id,_hash under the lock.
            if (_configuration.EnablePropsCache && _configuration.SkipHashValidationOnCacheCheck && PropsCache.Instance != null
                && !_context.IsInTransaction)
            {
                var cachedObj = PropsCache.GetWithoutHashValidation<TProps>(objectId);
                if (cachedObj != null)
                {
                    // Cache HIT WITHOUT DB query - return cached object
                    return cachedObj;
                }
            }

            // Cache MISS or hash check enabled - query ID + Hash (+ scheme, for the scheme-match guard
            // below) to distinguish "object not found" and "hash=null". The probe stays this narrow
            // on purpose: _hash covers the whole row (header included, see RedbHash), so a match
            // vouches for the cached object as a whole - no need to read _note/_value_bytes here.
            var eagerBaseObj = await _context.QueryFirstOrDefaultAsync<RedbObjectRow>(
                Sql.ObjectStorage_SelectIdHashScheme(), new object[] { objectId }, cancellationToken);

            if (eagerBaseObj == null)
            {
                if (_configuration.ThrowOnObjectNotFound)
                    throw new InvalidOperationException($"Object with ID {objectId} not found");
                return null;
            }

            // Guard against loading an object under the wrong Props type (garbage + cache poisoning).
            // Mismatch → null by default (soft-deleted scheme -10 reads as null), or throws under the flag.
            if (!await IsSchemeValidForLoadAsync<TProps>(objectId, eagerBaseObj.IdScheme))
                return null;

            // Object found, but hash may be null
            var objectHash = eagerBaseObj.Hash;

            // Check cache only if hash is NOT null AND validation is not skipped
            if (_configuration.EnablePropsCache && !_configuration.SkipHashValidationOnCacheCheck && PropsCache.Instance != null && objectHash.HasValue)
            {
                var cachedObj = PropsCache.Get<TProps>(objectId, objectHash.Value);
                if (cachedObj != null)
                {
                    // Cache HIT - the hash vouches for header and Props alike (cluster review,
                    // 2026-09-11: a save on another node moves _hash whichever part it touched)
                    return cachedObj;
                }
            }

            // Cache MISS - load via get_object_json
            return await LoadEagerAsync<TProps>(objectId, depth, cancellationToken);
        }

        public Task<string?> LoadJsonAsync(long objectId, int depth = 10, CancellationToken cancellationToken = default)
            => LoadJsonAsync(objectId, _securityContext.GetEffectiveUser(), depth, cancellationToken);

        public async Task<string?> LoadJsonAsync(long objectId, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default)
        {
            if (_configuration.DefaultCheckPermissionsOnLoad)
            {
                var canRead = await _permissionProvider.CanUserSelectObject(objectId, user.Id);
                if (!canRead)
                    throw new UnauthorizedAccessException($"User {user.Id} has no read permission for object {objectId}");
            }

            // Straight to the in-database materializer. No PropsCache (it is keyed by CLR type,
            // and there is none here) and no scheme guard (nothing to compare a scheme against);
            // Pro keeps this path too - _values stays the source of truth on every tier.
            var json = await _context.ExecuteJsonAsync(Sql.ObjectStorage_GetObjectJson(), new object[] { objectId, depth }, cancellationToken);

            if (string.IsNullOrEmpty(json))
            {
                if (_configuration.ThrowOnObjectNotFound)
                    throw new InvalidOperationException($"Object with ID {objectId} not found");
                return null;
            }

            return json;
        }

        /// <summary>
        /// Virtual method for EAGER loading of Props.
        /// Open Source: uses get_object_json
        /// Pro: overrides for PVT approach
        /// </summary>
        protected virtual async Task<RedbObject<TProps>?> LoadEagerAsync<TProps>(long objectId, int depth, CancellationToken cancellationToken) where TProps : class, new()
        {

            var json = await _context.ExecuteJsonAsync(
                Sql.ObjectStorage_GetObjectJson(), new object[] { objectId, depth }, cancellationToken);

            return MaterializeLoadedJson<TProps>(objectId, json);
        }

        /// <summary>
        /// The CPU half of an eager load, shared by the async path and the sync (thread-pool-free)
        /// path: deserialize, install lazy loaders, cache.
        /// </summary>
        private RedbObject<TProps>? MaterializeLoadedJson<TProps>(long objectId, string? json) where TProps : class, new()
        {
            if (string.IsNullOrEmpty(json))
            {
                if (_configuration.ThrowOnObjectNotFound)
                    throw new InvalidOperationException($"Object with ID {objectId} not found");
                return null;
            }

            // Deserialize JSON into RedbObject<TProps>
            var loadedObj = _serializer.Deserialize<TProps>(json);

            // V4 (L.3): boundary stubs get their loader - BEFORE the cache Set, so Set sees the
            // writer's scoped loader on every stub and wraps it with the dead-scope fallback
            // (a bare stub at Set time would be detached outright and the writer would pay a
            // fresh scope per touch - tsum, 2026-09-09).
            Utils.LazyReferenceInstaller.Install(loadedObj, CreateLazyPropsLoader());

            // Put main object + ALL nested RedbObject<T> into cache
            if (_configuration.EnablePropsCache && PropsCache.Instance != null && loadedObj.hash.HasValue)
            {
                PropsCache.Set(loadedObj);

                // Recursively cache all nested objects (they are already in memory after deserialization!)
                if (loadedObj.Props != null)
                {
                    CacheNestedObjects(loadedObj.Props);
                }
            }

            return loadedObj;
        }

        /// <summary>
        /// Synchronous eager load for the thread-pool-free lazy path (the sync getter of
        /// RedbListItem.Object): the same sequence as the main LoadAsync - permission check,
        /// props-cache probes, id/hash/scheme row, get_object_json - but every database call runs
        /// on the calling thread down to ADO.NET, so a saturated thread pool cannot slow or
        /// deadlock the load.
        /// </summary>
        public virtual RedbObject<TProps>? Load<TProps>(long objectId, int depth = 10) where TProps : class, new()
        {
            var user = _securityContext.GetEffectiveUser();
            if (_configuration.DefaultCheckPermissionsOnLoad)
            {
                if (!_permissionProvider.CanUserSelectObjectSync(objectId, user.Id))
                    throw new UnauthorizedAccessException($"User {user.Id} has no read permission for object {objectId}");
            }

            // Same cache discipline as the async path, including the in-transaction bypass.
            if (_configuration.EnablePropsCache && _configuration.SkipHashValidationOnCacheCheck && PropsCache.Instance != null
                && !_context.IsInTransaction)
            {
                var cachedObj = PropsCache.GetWithoutHashValidation<TProps>(objectId);
                if (cachedObj != null)
                    return cachedObj;
            }

            var eagerBaseObj = _context.QueryFirstOrDefault<RedbObjectRow>(
                Sql.ObjectStorage_SelectIdHashScheme(), objectId);

            if (eagerBaseObj == null)
            {
                if (_configuration.ThrowOnObjectNotFound)
                    throw new InvalidOperationException($"Object with ID {objectId} not found");
                return null;
            }

            // Scheme guard, cache-only: the async path would resolve a cold CLR-type projection
            // through the scheme-sync provider; here the only caller is the linked-object loader,
            // which picked TProps FROM the object's scheme id, so on a cold projection the pair
            // matches by construction and the load is accepted.
            var expected = Cache.GetSchemeIdByClrType(typeof(TProps));
            if (expected.HasValue && expected.Value != eagerBaseObj.IdScheme)
            {
                if (_configuration.ThrowOnSchemeMismatch)
                    throw new Exceptions.RedbSchemeMismatchException(objectId, typeof(TProps), expected.Value, eagerBaseObj.IdScheme);
                return null;
            }

            var objectHash = eagerBaseObj.Hash;
            if (_configuration.EnablePropsCache && !_configuration.SkipHashValidationOnCacheCheck && PropsCache.Instance != null && objectHash.HasValue)
            {
                var cachedObj = PropsCache.Get<TProps>(objectId, objectHash.Value);
                if (cachedObj != null)
                    return cachedObj;
            }

            var json = _context.ExecuteJson(Sql.ObjectStorage_GetObjectJson(), objectId, depth);
            return MaterializeLoadedJson<TProps>(objectId, json);
        }

        /// <summary>
        /// Recursively caches all nested RedbObject found in Props
        /// </summary>
        protected void CacheNestedObjects(object obj)
        {
            var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
            CacheNestedObjectsInternal(obj, visited);
        }

        /// <summary>
        /// Internal method for recursive caching with tracking of visited objects
        /// </summary>
        private void CacheNestedObjectsInternal(object obj, HashSet<object> visited)
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
            if (obj is Models.Contracts.IRedbListItem)
            {
                return;
            }

            // Only AFTER type check add to visited (protection against circular references)
            if (!visited.Add(obj))
            {
                return;
            }

            // If this is RedbObject<T> itself → cache it
            if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(RedbObject<>))
            {
                var redbObj = obj as IRedbObject;
                // V4 (L.3): a reference stub is neither cached nor walked - reading its Props here
                // would BE the lazy load: synchronous, and for the whole reachable graph (review).
                if (redbObj != null && redbObj.Hash.HasValue && obj is RedbObject { IsPropsLoaded: true })
                {
                    // Dynamically call PropsCache.Set<TProps>(redbObj)
                    var propsType = objType.GetGenericArguments()[0];
                    var setMethod = typeof(GlobalPropsCache).GetMethod("Set")?.MakeGenericMethod(propsType);
                    setMethod?.Invoke(PropsCache, new[] { obj });

                    // Raw Props, never the getter
                    var propsValue = objType.GetMethod("GetPropsDirectly")?.Invoke(obj, null);
                    if (propsValue != null)
                    {
                        CacheNestedObjectsInternal(propsValue, visited);
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
                        CacheNestedObjectsInternal(item, visited);
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
                        CacheNestedObjectsInternal(item, visited);
                    }
                }
                return;
            }

            // Process business class properties (Address, Contact, Details, etc.)
            var properties = objType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var prop in properties)
            {
                if (!prop.CanRead) continue;

                // Skip indexers (e.g. Dictionary<K,V>.Item[key]) - they require index parameters
                if (prop.GetIndexParameters().Length > 0) continue;

                var propValue = prop.GetValue(obj);
                if (propValue != null)
                {
                    CacheNestedObjectsInternal(propValue, visited);
                }
            }
        }


        /// <summary>
        /// Save single object via interface. Type determined internally.
        /// </summary>
        public async Task<long> SaveAsync(IRedbObject obj, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await SaveAsync(obj, effectiveUser, cancellationToken);
        }

        /// <summary>
        /// Save single object via interface with explicit user.
        /// </summary>
        public async Task<long> SaveAsync(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        {
            var results = await SaveAsync(new[] { obj }, user, cancellationToken);
            return results.FirstOrDefault();
        }

        public async Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await SaveAsync(obj, effectiveUser, cancellationToken);
        }


        public async Task<bool> DeleteAsync(IRedbObject obj, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await DeleteAsync(obj, effectiveUser, cancellationToken);
        }

        /// <summary>
        /// Deletes an object from the database using atomic ExecuteDeleteAsync.
        /// Thread-safe and handles concurrent delete attempts gracefully.
        /// </summary>
        public async Task<bool> DeleteAsync(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default)
        {
            // Permission check according to configuration
            if (_configuration.DefaultCheckPermissionsOnDelete)
            {
                var canDelete = await _permissionProvider.CanUserDeleteObject(obj, user);
                if (!canDelete)
                {
                    throw new UnauthorizedAccessException($"User {user.Id} has no delete permission for object {obj.Id}");
                }
            }

            // Interceptors (owner decision: deletes are covered too). Deleting before the SQL -
            // an exception cancels the delete; Deleted after it, only when a row actually went.
            if (HasSaveInterceptors)
            {
                await InvokeDeletingInterceptorsAsync(new Interception.RedbDeletingContext
                {
                    ObjectIds = [obj.Id],
                    KnownObjects = [obj],
                    EffectiveUser = user,
                }, cancellationToken);
            }

            // Atomic delete via SQL - single query, no race condition
            var deletedCount = await _context.ExecuteAsync(
                Sql.ObjectStorage_DeleteById(), new object[] { obj.Id }, cancellationToken);

            if (deletedCount == 0)
            {
                return false;  // Object did not exist or was already deleted
            }

            if (HasSaveInterceptors)
            {
                await InvokeDeletedInterceptorsAsync(new Interception.RedbDeletedContext
                {
                    ObjectIds = [obj.Id],
                    KnownObjects = [obj],
                    EffectiveUser = user,
                    DeletedCount = deletedCount,
                }, CancellationToken.None); // the row is gone - cancelling the notification would lie (s3.3)
            }

            // === CACHE INVALIDATION ===
            if (_configuration.EnablePropsCache && PropsCache.Instance != null)
            {
                PropsCache.Remove(obj.Id);
            }

            // === ID RESET STRATEGY ===
            if (_configuration.IdResetStrategy == redb.Core.Models.Configuration.ObjectIdResetStrategy.AutoResetOnDelete)
            {
                obj.ResetId(); // Automatically reset ID
            }

            return true;
        }


        // ===== DELETION BY ID =====

        /// <summary>
        /// Delete object by ID (uses _securityContext)
        /// </summary>
        public async Task<bool> DeleteAsync(long objectId, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await DeleteAsync(objectId, effectiveUser, cancellationToken);
        }

        /// <summary>
        /// Delete object by ID with explicit user
        /// </summary>
        public async Task<bool> DeleteAsync(long objectId, IRedbUser user, CancellationToken cancellationToken = default)
        {
            var deletedCount = await DeleteAsync(new[] { objectId }, user, cancellationToken);
            return deletedCount > 0;
        }

        // ===== BULK DELETION BY ID =====

        /// <summary>
        /// Bulk deletion of objects by ID (uses _securityContext)
        /// </summary>
        public async Task<int> DeleteAsync(IEnumerable<long> objectIds, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await DeleteAsync(objectIds, effectiveUser, cancellationToken);
        }

        /// <summary>
        /// Bulk deletion of objects by ID with explicit user
        /// </summary>
        public async Task<int> DeleteAsync(IEnumerable<long> objectIds, IRedbUser user, CancellationToken cancellationToken = default)
            => await DeleteByIdsCoreAsync(objectIds.ToList(), user, knownObjects: [], cancellationToken);

        /// <summary>
        /// The id-batch funnel. <paramref name="knownObjects"/> carries the instances when the
        /// caller had them (the delete-by-object overload), so interceptors see more than ids.
        /// </summary>
        private async Task<int> DeleteByIdsCoreAsync(List<long> ids, IRedbUser user, IReadOnlyList<IRedbObject> knownObjects, CancellationToken cancellationToken)
        {
            if (ids.Count == 0) return 0;

            // Permission check according to configuration
            if (_configuration.DefaultCheckPermissionsOnDelete)
            {
                foreach (var id in ids)
                {
                    var canDelete = await _permissionProvider.CanUserDeleteObject(id, user.Id);
                    if (!canDelete)
                    {
                        throw new UnauthorizedAccessException($"User {user.Id} has no delete permission for object {id}");
                    }
                }
            }

            if (HasSaveInterceptors)
            {
                await InvokeDeletingInterceptorsAsync(new Interception.RedbDeletingContext
                {
                    ObjectIds = ids,
                    KnownObjects = knownObjects,
                    EffectiveUser = user,
                }, cancellationToken);
            }

            // Bulk delete via SQL (single query with ANY)
            var deletedCount = await _context.ExecuteAsync(
                Sql.ObjectStorage_DeleteByIds(), new object[] { ids.ToArray() }, cancellationToken);

            // === CACHE INVALIDATION (ALWAYS) ===
            if (PropsCache.Instance != null)
            {
                foreach (var id in ids)
                {
                    PropsCache.Remove(id);
                }
            }

            if (HasSaveInterceptors && deletedCount > 0)
            {
                await InvokeDeletedInterceptorsAsync(new Interception.RedbDeletedContext
                {
                    ObjectIds = ids,
                    KnownObjects = knownObjects,
                    EffectiveUser = user,
                    DeletedCount = deletedCount,
                }, CancellationToken.None); // rows are gone - cancelling the notification would lie (s3.3)
            }

            return deletedCount;
        }

        // ===== BULK DELETION BY INTERFACE =====

        /// <summary>
        /// Bulk deletion of objects by interface (uses _securityContext)
        /// </summary>
        public async Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await DeleteAsync(objects, effectiveUser, cancellationToken);
        }

        /// <summary>
        /// Bulk deletion of objects by interface with explicit user
        /// </summary>
        public async Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, CancellationToken cancellationToken = default)
        {
            var objList = objects.ToList();
            if (objList.Count == 0) return 0;

            var ids = objList.Select(o => o.Id).ToList();

            // The id funnel, with the instances handed through so interceptors see the objects.
            var deletedCount = await DeleteByIdsCoreAsync(ids, user, objList, cancellationToken);

            // === ID RESET STRATEGY ===
            if (_configuration.IdResetStrategy == redb.Core.Models.Configuration.ObjectIdResetStrategy.AutoResetOnDelete)
            {
                foreach (var obj in objList)
                {
                    obj.ResetId();
                }
            }

            return deletedCount;
        }

        // ===== SOFT DELETE (BACKGROUND DELETION) =====

        /// <summary>
        /// Mark objects for soft-deletion (uses _securityContext).
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        public async Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, long? trashParentId = null, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await SoftDeleteAsync(objectIds, effectiveUser, trashParentId, cancellationToken);
        }

        /// <summary>
        /// Mark objects for soft-deletion with explicit user.
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        public async Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, IRedbUser user, long? trashParentId = null, CancellationToken cancellationToken = default)
        {
            var ids = objectIds.ToArray();
            if (ids.Length == 0)
                return new DeletionMark(0, 0);

            // Check permissions if configured
            if (_configuration.DefaultCheckPermissionsOnDelete)
            {
                foreach (var id in ids)
                {
                    var canDelete = await _permissionProvider.CanUserDeleteObject(id, user.Id);
                    if (!canDelete)
                    {
                        throw new UnauthorizedAccessException($"User {user.Id} does not have permission to delete object {id}");
                    }
                }
            }

            // Call the SQL function to mark objects for deletion
            var results = await _context.QueryAsync<MarkForDeletionResult>(
                Sql.SoftDelete_MarkForDeletion(),
                new object[] { ids, user.Id, trashParentId! },
                cancellationToken);

            var result = results.FirstOrDefault()
                ?? throw new InvalidOperationException("mark_for_deletion function returned no result");

            // Invalidate cache for all marked objects
            if (_configuration.EnablePropsCache && PropsCache.Instance != null)
            {
                foreach (var id in ids)
                {
                    PropsCache.Remove(id);
                }
            }

            _logger?.LogInformation(
                "Soft-deleted {Count} objects (including descendants). TrashId={TrashId}, User={UserId}",
                result.marked_count, result.trash_id, user.Id);

            return new DeletionMark(result.trash_id, (int)result.marked_count);
        }

        /// <summary>
        /// Mark objects for soft-deletion (uses _securityContext).
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        public Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, long? trashParentId = null, CancellationToken cancellationToken = default)
        {
            return SoftDeleteAsync(objects.Select(o => o.Id), trashParentId, cancellationToken);
        }

        /// <summary>
        /// Mark objects for soft-deletion with explicit user.
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        public Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, long? trashParentId = null, CancellationToken cancellationToken = default)
        {
            return SoftDeleteAsync(objects.Select(o => o.Id), user, trashParentId, cancellationToken);
        }

        /// <summary>
        /// Delete objects with background purge and progress reporting.
        /// Marks objects for deletion, then purges them in batches with progress callback.
        /// </summary>
        public async Task DeleteWithPurgeAsync(
            IEnumerable<long> objectIds,
            int batchSize = 10,
            IProgress<PurgeProgress>? progress = null,
            CancellationToken cancellationToken = default,
            long? trashParentId = null)
        {
            // Step 1: Mark for deletion (fast, atomic)
            var mark = await SoftDeleteAsync(objectIds, trashParentId, cancellationToken);

            if (mark.MarkedCount == 0)
                return;

            // Step 2: Purge in batches with progress
            await PurgeTrashAsync(mark.TrashId, mark.MarkedCount, batchSize, progress, cancellationToken);
        }

        /// <summary>
        /// Purge a trash container created by SoftDeleteAsync.
        /// Physically deletes objects in batches with progress callback.
        /// </summary>
        public async Task PurgeTrashAsync(
            long trashId,
            int totalCount,
            int batchSize = 10,
            IProgress<PurgeProgress>? progress = null,
            CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            var deleted = 0;
            var startedAt = DateTimeOffset.UtcNow;

            // Unified cancellation semantics (owner decision, 2026-09-08): cancellation is OCE
            // here like everywhere else - the historical soft return is gone. The token gates
            // every batch boundary and rides into the purge command itself; a farewell progress
            // report with PurgeStatus.Cancelled still fires so a UI shows where the purge
            // stopped. Each purge_trash batch commits on its own, so nothing is lost - the
            // remainder stays queued in the database and the background worker (or a retry)
            // picks it up.
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Call purge_trash SQL function
                    var results = await _context.QueryAsync<PurgeTrashResult>(
                        Sql.SoftDelete_PurgeTrash(),
                        new object[] { trashId, batchSize },
                        cancellationToken);

                    var result = results.FirstOrDefault();
                    if (result == null || result.deleted_count == 0)
                        break;

                    deleted += (int)result.deleted_count;
                    var remaining = (int)result.remaining_count;

                    var status = remaining == 0 ? PurgeStatus.Completed : PurgeStatus.Running;
                    progress?.Report(new PurgeProgress(
                        trashId, deleted, remaining, status, startedAt, effectiveUser.Id));

                    if (remaining == 0)
                        break;
                }
            }
            catch (OperationCanceledException)
            {
                progress?.Report(new PurgeProgress(
                    trashId, deleted, totalCount - deleted, PurgeStatus.Cancelled, startedAt, effectiveUser.Id));
                throw;
            }

            // LogDebug, not LogInformation — each Identity-level DELETE produces its own
            // trash container with 1 object, so this fires per-operation and floods INF
            // logs with "Deleted=1" entries (worker restart compounds it via
            // RecoverOrphanedTasksAsync draining the accumulated backlog one-by-one).
            // Operators who need per-purge visibility enable DBG for this category.
            _logger?.LogDebug(
                "PurgeTrash completed. TrashId={TrashId}, Deleted={Deleted}, User={UserId}",
                trashId, deleted, effectiveUser.Id);
        }

        /// <summary>
        /// Gets deletion progress for a specific trash container from database.
        /// Returns null if trash container not found or already deleted.
        /// </summary>
        public async Task<PurgeProgress?> GetDeletionProgressAsync(long trashId, CancellationToken cancellationToken = default)
        {
            var results = await _context.QueryAsync<TrashProgressRow>(
                Sql.SoftDelete_GetDeletionProgress(), new object[] { trashId }, cancellationToken);

            var row = results.FirstOrDefault();
            if (row == null) return null;

            return new PurgeProgress(
                row.trash_id,
                (int)row.deleted,
                (int)(row.total - row.deleted),
                ParsePurgeStatus(row.status),
                row.started_at,
                row.owner_id);
        }

        /// <summary>
        /// Gets all active (pending/running) deletions for a user from database.
        /// </summary>
        public async Task<List<PurgeProgress>> GetUserActiveDeletionsAsync(long userId, CancellationToken cancellationToken = default)
        {
            var results = await _context.QueryAsync<TrashProgressRow>(
                Sql.SoftDelete_GetUserActiveDeletions(), new object[] { userId }, cancellationToken);

            return results.Select(row => new PurgeProgress(
                row.trash_id,
                (int)row.deleted,
                (int)(row.total - row.deleted),
                ParsePurgeStatus(row.status),
                row.started_at,
                row.owner_id)).ToList();
        }

        private static PurgeStatus ParsePurgeStatus(string status) => status switch
        {
            "pending" => PurgeStatus.Pending,
            "running" => PurgeStatus.Running,
            "completed" => PurgeStatus.Completed,
            "failed" => PurgeStatus.Failed,
            "cancelled" => PurgeStatus.Cancelled,
            _ => PurgeStatus.Pending
        };

        /// <summary>
        /// Gets orphaned deletion tasks for recovery at startup.
        /// CLUSTER-SAFE: Returns 'pending' OR 'running' with stale _date_modify.
        /// </summary>
        public async Task<List<OrphanedTask>> GetOrphanedDeletionTasksAsync(int timeoutMinutes = 30, CancellationToken cancellationToken = default)
        {
            var results = await _context.QueryAsync<OrphanedTaskRow>(
                Sql.SoftDelete_GetOrphanedTasks(), new object[] { timeoutMinutes }, cancellationToken);

            return results.Select(row => new OrphanedTask(
                row.trash_id,
                (int)row.total,
                (int)row.deleted,
                row.status,
                row.owner_id)).ToList();
        }

        /// <summary>
        /// Atomically claim an orphaned task for processing.
        /// CLUSTER-SAFE: Uses atomic UPDATE to prevent race conditions.
        /// </summary>
        public async Task<bool> TryClaimOrphanedTaskAsync(long trashId, int timeoutMinutes = 30, CancellationToken cancellationToken = default)
        {
            var affected = await _context.ExecuteAsync(
                Sql.SoftDelete_ClaimOrphanedTask(), new object[] { trashId, timeoutMinutes }, cancellationToken);
            return affected > 0;
        }

        /// <summary>
        /// Internal DTO for orphaned task query results.
        /// </summary>
        private class OrphanedTaskRow
        {
            public long trash_id { get; set; }
            public long total { get; set; }
            public long deleted { get; set; }
            public string status { get; set; } = "";
            public long owner_id { get; set; }
        }

        // ===== BULK LOADING BY ID =====

        /// <summary>
        /// Bulk load objects by ID (uses _securityContext)
        /// Supports polymorphic loading of objects from different schemes
        /// </summary>
        public async Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, int depth = 10, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadAsync(objectIds, effectiveUser, depth, cancellationToken);
        }

        /// <summary>
        /// Bulk loading of objects by ID with explicit user
        /// Supports two modes: EAGER (get_object_json) and LAZY (_objects + LoadPropsForManyAsync)
        /// </summary>
        public async Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default)
        {
            var ids = objectIds.ToList();
            if (ids.Count == 0) return new List<IRedbObject>();

            var result = new List<IRedbObject>();
            var idsToLoad = new List<long>();

            // === STEP 1: Cache check (if enabled) ===
            if (_configuration.EnablePropsCache && PropsCache.Instance != null)
            {
                foreach (var id in ids)
                {
                    IRedbObject? cachedObj = null;

                    if (_configuration.SkipHashValidationOnCacheCheck)
                    {
                        // Fast check without hash - but we need type for GetWithoutHashValidation<T>
                        // Therefore for polymorphic case we skip cache without hash
                        idsToLoad.Add(id);
                    }
                    else
                    {
                        // Get hash from DB to check cache
                        var hashInfo = await _context.QueryFirstOrDefaultAsync<RedbObjectRow>(
                            Sql.ObjectStorage_SelectIdHashScheme(), new object[] { id }, cancellationToken);

                        if (hashInfo != null && hashInfo.Hash.HasValue)
                        {
                            // Get type through AutomaticTypeRegistry
                            var propsType = Cache.GetClrType(hashInfo.IdScheme);
                            if (propsType != null)
                            {
                                // Dynamically call Get<TProps>(id, hash)
                                var getMethod = typeof(GlobalPropsCache).GetMethod("Get")?.MakeGenericMethod(propsType);
                                if (getMethod != null)
                                {
                                    cachedObj = getMethod.Invoke(PropsCache, new object[] { id, hashInfo.Hash.Value }) as IRedbObject;
                                }
                            }
                        }

                        if (cachedObj != null)
                        {
                            result.Add(cachedObj);  // Cache HIT
                        }
                        else
                        {
                            idsToLoad.Add(id);  // Cache MISS
                        }
                    }
                }
            }
            else
            {
                idsToLoad = ids;
            }

            if (idsToLoad.Count == 0)
            {
                return result;  // All from cache!
            }

            // === STEP 2: Permission check ===
            if (_configuration.DefaultCheckPermissionsOnLoad)
            {
                foreach (var id in idsToLoad)
                {
                    var canRead = await _permissionProvider.CanUserSelectObject(id, user.Id);
                    if (!canRead)
                    {
                        throw new UnauthorizedAccessException($"User {user.Id} has no read permission for object {id}");
                    }
                }
            }

            // === STEP 3: Loading missing objects ===
            // V4 (L.0): the old per-call lazy switch is gone. Free loads eagerly via
            // get_object_json; Pro overrides LoadObjectsEagerAsync to its batch materializer.
            var loadedObjects = await LoadObjectsEagerAsync(idsToLoad, depth, cancellationToken);

            result.AddRange(loadedObjects);

            // === STEP 4: Check if all objects are found ===
            if (_configuration.ThrowOnObjectNotFound)
            {
                var loadedIds = result.Select(o => o.Id).ToHashSet();
                var missingIds = ids.Where(id => !loadedIds.Contains(id)).ToList();
                if (missingIds.Count > 0)
                {
                    throw new InvalidOperationException($"Objects with ID [{string.Join(", ", missingIds)}] not found");
                }
            }

            return result;
        }

        /// <summary>
        /// LAZY loading: base fields from _objects + LoadPropsForManyAsync
        /// protected for access from Pro version
        /// </summary>
        protected async Task<List<IRedbObject>> LoadObjectsLazyAsync(List<long> objectIds, int depth, CancellationToken cancellationToken = default)
        {
            if (objectIds.Count == 0) return new List<IRedbObject>();

            // Load base fields from _objects via SQL
            var baseObjects = await _context.QueryAsync<RedbObjectRow>(
                Sql.ObjectStorage_SelectObjectsByIds(), new object[] { objectIds.ToArray() }, cancellationToken);

            if (baseObjects.Count == 0) return new List<IRedbObject>();

            // Group by scheme_id for polymorphic deserialization
            var result = new List<IRedbObject>();
            var objectsByScheme = baseObjects.GroupBy(o => o.IdScheme);

            foreach (var schemeGroup in objectsByScheme)
            {
                var schemeId = schemeGroup.Key;
                var objectsForScheme = schemeGroup.ToList();

                // Resolve scheme_id → Type: lazy, self-healing, cross-domain-safe (loads the scheme by
                // id and derives via the global ClrSchemeTypeIndex if this domain hasn't seen it yet).
                // Null means the scheme genuinely has no CLR type → a legitimately non-generic object.
                var propsType = await Cache.ResolveClrTypeAsync(schemeId, SchemeSyncProvider);

                if (propsType == null)
                {
                    // Non-generic RedbObject (Object scheme) - create without Props
                    foreach (var baseObj in objectsForScheme)
                    {
                        result.Add(CreateNonGenericRedbObject(baseObj));
                    }
                    continue;
                }

                // Create typed RedbObject<TProps>
                var createMethod = typeof(ObjectStorageProviderBase)
                    .GetMethod(nameof(CreateRedbObjectsFromBaseObjects), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)?
                    .MakeGenericMethod(propsType);

                if (createMethod != null)
                {
                    var createTask = (Task)createMethod.Invoke(this, new object[] { objectsForScheme, depth })!;
                    await createTask;
                    var typedObjects = createTask.GetType().GetProperty("Result")?.GetValue(createTask) as System.Collections.IEnumerable;
                    if (typedObjects != null)
                    {
                        foreach (var obj in typedObjects)
                        {
                            if (obj is IRedbObject redbObj)
                            {
                                result.Add(redbObj);
                            }
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Create non-generic RedbObject from RedbObjectRow (Object scheme, no Props).
        /// </summary>
        private RedbObject CreateNonGenericRedbObject(RedbObjectRow baseObj)
        {
            var obj = new RedbObject();
            baseObj.ApplyHeaderTo(obj);
            return obj;
        }

        /// <summary>
        /// Creates RedbObject&lt;TProps&gt; from base RedbObjectRow fields with LazyPropsLoader and LoadPropsForManyAsync
        /// </summary>
        private async Task<List<RedbObject<TProps>>> CreateRedbObjectsFromBaseObjects<TProps>(List<RedbObjectRow> baseObjects, int depth) where TProps : class, new()
        {
            var redbObjects = baseObjects.Select(baseObj =>
            {
                var obj = baseObj.ToRedbObject<TProps>();
                obj._lazyLoader = CreateLazyPropsLoader();
                return obj;
            }).ToList();

            // Bulk load Props via LoadPropsForManyAsync at the requested depth (V4: no sync-over-async)
            var lazyLoader = CreateLazyPropsLoader();
            await lazyLoader.LoadPropsForManyAsync(redbObjects, propsDepth: depth);

            // Cache loaded objects
            if (_configuration.EnablePropsCache && PropsCache.Instance != null)
            {
                foreach (var obj in redbObjects.Where(o => o.hash.HasValue))
                {
                    PropsCache.Set(obj);
                }
            }

            return redbObjects;
        }

        /// <summary>
        /// EAGER loading: get_object_json for all IDs
        /// Open Source: uses get_object_json
        /// Pro: overrides for PVT approach
        /// </summary>
        protected virtual async Task<List<IRedbObject>> LoadObjectsEagerAsync(List<long> objectIds, int depth, CancellationToken cancellationToken)
        {
            if (objectIds.Count == 0) return new List<IRedbObject>();

            // Use unnest for bulk get_object_json call
            var idsArray = objectIds.ToArray();

            // Execute get_object_json for each ID via SQL
            var objectsJsonList = await _context.ExecuteJsonListAsync(
                Sql.ObjectStorage_GetObjectsJsonBulk(), new object[] { idsArray, depth }, cancellationToken);

            var result = new List<IRedbObject>();

            foreach (var objectJson in objectsJsonList)
            {
                if (string.IsNullOrEmpty(objectJson)) continue;

                // Extract scheme_id from JSON for polymorphic deserialization
                using var jsonDoc = System.Text.Json.JsonDocument.Parse(objectJson);
                var schemeId = jsonDoc.RootElement.GetProperty("scheme_id").GetInt64();

                // Get type through AutomaticTypeRegistry
                var propsType = Cache.GetClrType(schemeId);
                if (propsType == null)
                {
                    // Non-generic RedbObject (Object scheme) - deserialize base fields only
                    var baseObj = System.Text.Json.JsonSerializer.Deserialize<RedbObject>(objectJson);
                    if (baseObj != null)
                    {
                        result.Add(baseObj);
                    }
                    continue;
                }

                // Dynamic deserialization
                var redbObj = _serializer.DeserializeRedbDynamic(objectJson, propsType);
                if (redbObj != null)
                {
                    Utils.LazyReferenceInstaller.Install(redbObj, CreateLazyPropsLoader()); // V4 (L.3)
                    // Cache main + nested objects
                    if (_configuration.EnablePropsCache && PropsCache.Instance != null && redbObj.Hash.HasValue)
                    {
                        var setMethod = typeof(GlobalPropsCache).GetMethod("Set")?.MakeGenericMethod(propsType);
                        setMethod?.Invoke(PropsCache, new[] { redbObj });

                        // Recursively cache nested objects
                        var propsProperty = redbObj.GetType().GetProperty("Props");
                        if (propsProperty != null)
                        {
                            var propsValue = propsProperty.GetValue(redbObj);
                            if (propsValue != null)
                            {
                                CacheNestedObjects(propsValue);
                            }
                        }
                    }

                    result.Add(redbObj);
                }
            }

            return result;
        }

        // ===== LAZY REFERENCE RELOAD (V4, LAZY Л2 §4.6) =====

        /// <inheritdoc cref="IObjectStorageProvider.LoadReferencesAsync{TProps,TRef}(RedbObject{TProps},Func{TProps,System.Collections.Generic.IEnumerable{RedbObject{TRef}}},CancellationToken)"/>
        public async Task LoadReferencesAsync<TProps, TRef>(RedbObject<TProps> parent,
            Func<TProps, IEnumerable<RedbObject<TRef>?>?> references,
            CancellationToken cancellationToken = default)
            where TProps : class, new() where TRef : class, new()
        {
            var props = parent?.GetPropsDirectly();
            if (props == null) return;

            var stubs = (references(props) ?? Enumerable.Empty<RedbObject<TRef>?>())
                .Where(r => r is { id: > 0 } && !r.IsPropsLoaded)
                .Select(r => r!)
                .ToList();
            if (stubs.Count == 0) return;

            // One batch: the loader checks the props cache first, then fetches the misses together.
            // Depth 1, exactly like a stub's first Props access: the reloaded objects' own references
            // are stubs again - laziness stays transitive through the batch form (§4.7).
            await CreateLazyPropsLoader().LoadPropsForManyAsync(stubs, propsDepth: 1, cancellationToken: cancellationToken);
        }

        /// <inheritdoc cref="IObjectStorageProvider.LoadReferencesAsync{TProps,TRef}(RedbObject{TProps},Func{TProps,RedbObject{TRef}},CancellationToken)"/>
        public Task LoadReferencesAsync<TProps, TRef>(RedbObject<TProps> parent,
            Func<TProps, RedbObject<TRef>?> reference,
            CancellationToken cancellationToken = default)
            where TProps : class, new() where TRef : class, new()
            => LoadReferencesAsync(parent, p => new[] { reference(p) }, cancellationToken);

        // ===== LOAD WITH PARENT CHAIN =====

        /// <summary>
        /// Load object by ID with parent chain to root (uses _securityContext).
        /// </summary>
        public async Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(long objectId, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadWithParentsAsync<TProps>(objectId, effectiveUser, depth, cancellationToken);
        }

        /// <summary>
        /// Load object with parent chain to root (uses _securityContext).
        /// </summary>
        public async Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(IRedbObject obj, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadWithParentsAsync<TProps>(obj.Id, effectiveUser, depth, cancellationToken);
        }

        /// <summary>
        /// Load object by ID with parent chain to root with explicit user.
        /// </summary>
        public async Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            return await LoadWithParentsAsync<TProps>(obj.Id, user, depth, cancellationToken);
        }

        /// <summary>
        /// MAIN LoadWithParentsAsync - loads single object with parent chain.
        /// Delegates to bulk method for implementation reuse.
        /// </summary>
        public async Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(long objectId, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var list = await LoadWithParentsAsync<TProps>(new[] { objectId }, user, depth, cancellationToken);
            return list.FirstOrDefault();
        }

        /// <summary>
        /// Bulk load objects with parent chains (uses _securityContext).
        /// </summary>
        public async Task<List<TreeRedbObject<TProps>>> LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadWithParentsAsync<TProps>(objectIds, effectiveUser, depth, cancellationToken);
        }

        /// <summary>
        /// MAIN bulk LoadWithParentsAsync - loads objects with parent chains to root.
        /// Uses recursive CTE to get all ancestor IDs, then bulk loads all objects.
        /// Parents are loaded polymorphically (each with its real Props type).
        /// </summary>
        public async Task<List<TreeRedbObject<TProps>>> LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            var ids = objectIds.ToList();
            if (ids.Count == 0) return new List<TreeRedbObject<TProps>>();

            // 1. Get IDs of all objects and their ancestors via recursive CTE
            var idsString = string.Join(",", ids);
            var sql = _sql.Query_GetIdsWithAncestorsSql(idsString);
            var allIds = await _context.QueryScalarListAsync<long>(sql, System.Array.Empty<object>(), cancellationToken);

            // 2. Load all objects polymorphically (target + parents with real types)
            var loadedObjects = await LoadAsync(allIds, user, depth, cancellationToken);

            // 3. Convert to ITreeRedbObject (polymorphic - each keeps its real Props type)
            var treeObjects = new Dictionary<long, ITreeRedbObject>();
            foreach (var obj in loadedObjects)
            {
                treeObjects[obj.Id] = Utils.TreeObjectConverter.ToTreeObjectDynamic(obj);
            }

            // 4. Build Parent relationships (polymorphic)
            Utils.TreeObjectConverter.BuildParentRelationships(treeObjects.Values);

            // 5. Return only requested objects that match TProps type
            return ids
                .Where(id => treeObjects.ContainsKey(id) && treeObjects[id] is TreeRedbObject<TProps>)
                .Select(id => (TreeRedbObject<TProps>)treeObjects[id])
                .ToList();
        }

        /// <summary>
        /// Bulk polymorphic load with parent chains (uses _securityContext).
        /// </summary>
        public async Task<List<ITreeRedbObject>> LoadWithParentsAsync(IEnumerable<long> objectIds, int depth = 10, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadWithParentsAsync(objectIds, effectiveUser, depth, cancellationToken);
        }

        /// <summary>
        /// MAIN bulk polymorphic LoadWithParentsAsync.
        /// Preserves actual Props types for each object.
        /// </summary>
        public async Task<List<ITreeRedbObject>> LoadWithParentsAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default)
        {
            var ids = objectIds.ToList();
            if (ids.Count == 0) return new List<ITreeRedbObject>();

            // 1. Get IDs of all objects and their ancestors
            var idsString = string.Join(",", ids);
            var sql = _sql.Query_GetIdsWithAncestorsSql(idsString);
            var allIds = await _context.QueryScalarListAsync<long>(sql, System.Array.Empty<object>(), cancellationToken);

            // 2. Load all objects
            var loadedObjects = await LoadAsync(allIds, user, depth, cancellationToken);

            // 3. Convert to ITreeRedbObject and build dictionary
            var treeObjects = new Dictionary<long, ITreeRedbObject>();
            foreach (var obj in loadedObjects)
            {
                treeObjects[obj.Id] = Utils.TreeObjectConverter.ToTreeObjectDynamic(obj);
            }

            // 4. Build Parent relationships
            Utils.TreeObjectConverter.BuildParentRelationships(treeObjects.Values);

            // 5. Return only requested objects
            return ids
                .Where(id => treeObjects.ContainsKey(id))
                .Select(id => treeObjects[id])
                .ToList();
        }

        public async Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            // Route through Batch pipeline — same TX + FOR UPDATE + DeadlockRetry for all strategies
            var results = await SaveAsync(new[] { (IRedbObject)obj }, user, cancellationToken);
            return results.FirstOrDefault();
        }

        /// <inheritdoc />
        public Task<int> LockForUpdateAsync(params long[] objectIds)
            => LockForUpdateAsync(objectIds, CancellationToken.None);

        /// <inheritdoc />
        public async Task<int> LockForUpdateAsync(long[] objectIds, CancellationToken cancellationToken)
        {
            if (objectIds == null || objectIds.Length == 0) return 0;

            // SELECT _id ... FOR UPDATE/UPDLOCK: the rows that exist come back (and are locked);
            // the count is the caller\x27s signal. A deleted id locks nothing and MUST NOT be taken
            // for locked (BR-9, 2026-09-02: with the default AutoSwitchToInsert a CAS save after
            // a silent no-op lock resurrects the deleted object).
            var locked = await _context.QueryScalarListAsync<long>(
                _sql.ObjectStorage_LockObjectsForUpdate(), new object[] { objectIds }, cancellationToken).ConfigureAwait(false);
            return locked.Count;
        }

        /// <inheritdoc />
        public Task LockForUpdateRequiredAsync(params long[] objectIds)
            => LockForUpdateRequiredAsync(objectIds, CancellationToken.None);

        /// <inheritdoc />
        public async Task LockForUpdateRequiredAsync(long[] objectIds, CancellationToken cancellationToken)
        {
            if (objectIds == null || objectIds.Length == 0) return;

            if (!_context.IsInTransaction)
                throw new InvalidOperationException(
                    "LockForUpdateRequiredAsync must run inside an active transaction: a row lock " +
                    "lives only until its transaction ends, so a lock taken outside one guarantees " +
                    "nothing. Wrap the lock and the work it protects in ExecuteAtomicAsync or " +
                    "BeginTransactionAsync.");

            var requested = objectIds.Distinct().ToArray();
            var locked = await _context.QueryScalarListAsync<long>(
                _sql.ObjectStorage_LockObjectsForUpdate(), new object[] { requested }, cancellationToken).ConfigureAwait(false);
            if (locked.Count == requested.Length)
                return;

            throw new RedbLockNotAcquiredException(
                requested, requested.Except(locked).OrderBy(x => x).ToArray());
        }

        // CT-3 (ревью 2026-09-03, снесено 2026-09-04): здесь жил мёртвый одиночный путь
        // сохранения свойств (SavePropertiesAsync со своим Delete/Insert и ВТОРЫМ, расходящимся
        // алгоритмом ChangeTracking) - точка входа не имела ни одного вызова. Все сохранения V4
        // идут батч-конвейером SaveAsync; Free заявляет ChangeTracking как NotSupported в
        // ExecuteBatchByStrategy, живой CT - в redb.Core.Pro. Вместе с кластером удалены его
        // хелперы (Methods.cs целиком) и пара мёртвых SQL-методов диалектов.

        /// <summary>
        /// Get scheme structures with full metadata including _store_null
        /// </summary>
        private async Task<List<StructureMetadata>> GetStructuresWithMetadataAsync(long schemeId)
        {
            // Load structures with type info via SQL join
            var rows = await _context.QueryAsync<StructureMetadataRow>(
                Sql.ObjectStorage_SelectStructuresWithMetadata(), schemeId);

            return rows.Select(r => new StructureMetadata
            {
                Id = r.Id,
                IdParent = r.IdParent,
                Name = r.Name,
                DbType = r.DbType,
                IsArray = r.CollectionType != null,
                CollectionType = r.CollectionType,
                KeyType = r.KeyType,
                StoreNull = r.StoreNull,
                Unique = r.Unique,
                UniqueScope = r.UniqueScope,
                TypeSemantic = r.TypeSemantic
            }).ToList();
        }

        /// <summary>
        /// Helper class for mapping structure metadata query results.
        /// </summary>
        private class StructureMetadataRow
        {
            public long Id { get; set; }
            public long? IdParent { get; set; }
            public string Name { get; set; } = string.Empty;
            public string DbType { get; set; } = "String";
            public long? CollectionType { get; set; }
            public long? KeyType { get; set; }
            public bool StoreNull { get; set; }
            public bool Unique { get; set; }
            public long? UniqueScope { get; set; }
            public string TypeSemantic { get; set; } = "string";
        }

        /// <summary>
        /// AUTOSAVE: Processes nested RedbObjects, saving them recursively
        /// </summary>
        private async Task<object?> ProcessNestedObjectsAsync(object rawValue, string dbType, bool isArray, long parentObjectId = 0)
        {
            if (rawValue == null) return null;



            // Process arrays
            if (isArray && rawValue is System.Collections.IEnumerable enumerable && rawValue is not string)
            {

                var processedList = new List<object>();
                foreach (var item in enumerable)
                {
                    if (IsRedbObjectWithoutId(item))
                    {
                        var nestedObj = (IRedbObject)item;
                        // SET PARENT: If nested object has no parent, set base one
                        if ((nestedObj.ParentId == 0 || nestedObj.ParentId == null) && parentObjectId > 0)
                        {
                            nestedObj.ParentId = parentObjectId;
                        }
                        var savedId = await SaveAsync((dynamic)item);
                        processedList.Add((long)savedId);
                    }
                    else if (IsRedbObjectWithId(item))
                    {
                        processedList.Add(((IRedbObject)item).Id);
                    }
                    else if (item is IRedbListItem listItemInArray)
                    {
                        // Process IRedbListItem in array - extract Id
                        processedList.Add(listItemInArray.Id);
                    }
                    else
                    {
                        processedList.Add(item);
                    }
                }
                return processedList;
            }

            // Process single objects
            if (IsRedbObjectWithoutId(rawValue))
            {
                var nestedObj = (IRedbObject)rawValue;
                // SET PARENT: If nested object has no parent, set base one
                if ((nestedObj.ParentId == 0 || nestedObj.ParentId == null) && parentObjectId > 0)
                {
                    nestedObj.ParentId = parentObjectId;
                }
                var savedId = await SaveAsync((dynamic)rawValue);
                return (long)savedId;
            }

            if (IsRedbObjectWithId(rawValue))
            {
                return ((IRedbObject)rawValue).Id;
            }

            // Process IRedbListItem - extract Id
            if (rawValue is IRedbListItem listItem)
            {
                return listItem.Id;
            }

            return rawValue;
        }

        /// <summary>
        /// Checks if object is IRedbObject with Id = 0 (needs saving)
        /// </summary>
        private static bool IsRedbObjectWithoutId(object? value)
        {
            if (value is IRedbObject redbObj)
            {
                return redbObj.Id == 0;
            }
            return false;
        }

        /// <summary>
        /// Checks if object is IRedbObject with Id != 0 (already saved)
        /// </summary>
        private static bool IsRedbObjectWithId(object? value)
        {
            if (value is IRedbObject redbObj)
            {
                return redbObj.Id != 0;
            }
            return false;
        }

        /// <summary>
        /// UPDATED VERSION: Removed JSON arrays, only simple types
        /// </summary>
        private static void SetSimpleValueByType(RedbValue valueRecord, string dbType, object? processedValue)
        {
            if (processedValue == null) return;

            // ARRAYS NOT PROCESSED - they go through SaveArrayFieldAsync

            // Direct assignment of typed values
            switch (dbType)
            {
                case "String":
                case "Text":
                    // TimeOnly and TimeSpan carry db_type "String" (_types seed). Their plain
                    // ToString() uses the CURRENT culture — "2:30 PM" under en-US, "14:30" under
                    // ru-RU — so a value written under one culture failed or silently mis-parsed
                    // under another. Route them through the invariant round-trip form, the same
                    // one the JSON converters use.
                    valueRecord.String = processedValue switch
                    {
                        TimeOnly timeOnly => Core.Utils.RedbTemporalFormat.ToText(timeOnly),
                        TimeSpan timeSpan => Core.Utils.RedbTemporalFormat.ToText(timeSpan),
                        DateOnly dateOnly => Core.Utils.RedbTemporalFormat.ToText(dateOnly),
                        _ => processedValue?.ToString()
                    };
                    break;
                case "Long":
                case "bigint":
                    if (processedValue is long longVal)
                        valueRecord.Long = longVal;
                    else if (processedValue is int intVal)
                        valueRecord.Long = intVal;
                    else if (long.TryParse(processedValue?.ToString(), out var parsedLong))
                        valueRecord.Long = parsedLong;
                    break;
                case "Double":
                    if (processedValue is double doubleVal)
                        valueRecord.Double = doubleVal;
                    else if (processedValue is float floatVal)
                        valueRecord.Double = floatVal;
                    else if (double.TryParse(processedValue?.ToString(), out var parsedDouble))
                        valueRecord.Double = parsedDouble;
                    break;
                case "Numeric":
                    // Precise decimal numbers for financial calculations
                    if (processedValue is decimal decimalVal)
                        valueRecord.Numeric = decimalVal;
                    else if (processedValue is double doubleVal2)
                        valueRecord.Numeric = (decimal)doubleVal2;
                    else if (processedValue is float floatVal2)
                        valueRecord.Numeric = (decimal)floatVal2;
                    else if (decimal.TryParse(processedValue?.ToString(), out var parsedDecimal))
                        valueRecord.Numeric = parsedDecimal;
                    break;
                case "Boolean":
                    if (processedValue is bool boolVal)
                        valueRecord.Boolean = boolVal;
                    else if (bool.TryParse(processedValue?.ToString(), out var parsedBool))
                        valueRecord.Boolean = parsedBool;
                    break;
                // db_type "DateTime" belongs to DateOnly (the CLR DateTime type carries db_type
                // "DateTimeOffset" and is handled in the branch below, discriminated by the
                // runtime type of the value).
                case "DateTime":
                    if (processedValue is DateOnly dateOnlyValue)
                    {
                        // Midnight of that date, as a zone-less clock reading. Was previously
                        // DateTime.TryParse(dateOnly.ToString()) — a round-trip through the
                        // current culture's short date pattern.
                        valueRecord.DateTimeOffset = Core.Utils.DateTimeConverter.NormalizeForStorage(
                            dateOnlyValue.ToDateTime(TimeOnly.MinValue));
                    }
                    else if (processedValue is DateTime dateTime)
                    {
                        // Use centralized converter: DateTime → UTC
                        valueRecord.DateTimeOffset = Core.Utils.DateTimeConverter.NormalizeForStorage(dateTime);
                    }
                    else if (Core.Utils.RedbTemporalFormat.TryParseDateOnly(processedValue?.ToString(), out var parsedDateOnly))
                    {
                        valueRecord.DateTimeOffset = Core.Utils.DateTimeConverter.NormalizeForStorage(
                            parsedDateOnly.ToDateTime(TimeOnly.MinValue));
                    }
                    break;
                case "DateTimeOffset":
                    if (processedValue is DateTimeOffset dateTimeOffset)
                        valueRecord.DateTimeOffset = dateTimeOffset;
                    else if (processedValue is DateTime dt)
                        valueRecord.DateTimeOffset = Core.Utils.DateTimeConverter.NormalizeForStorage(dt);
                    // DateOnly stores midnight of that date as a zone-less reading. It carries
                    // db_type "DateTimeOffset" so that every JSON projection, PVT builder and the
                    // SQLite native extension handle it through the branch they already have.
                    else if (processedValue is DateOnly dateOnlyForOffset)
                        valueRecord.DateTimeOffset = Core.Utils.DateTimeConverter.NormalizeForStorage(
                            dateOnlyForOffset.ToDateTime(TimeOnly.MinValue));
                    else if (DateTimeOffset.TryParse(processedValue?.ToString(),
                                 System.Globalization.CultureInfo.InvariantCulture,
                                 System.Globalization.DateTimeStyles.RoundtripKind, out var parsedDate))
                        valueRecord.DateTimeOffset = parsedDate;
                    break;
                case "ByteArray":
                    if (processedValue is byte[] byteArray)
                        valueRecord.ByteArray = byteArray;
                    break;
                case "Object":
                    // Object references are stored in separate _object field
                    if (processedValue is long objectId)
                        valueRecord.Object = objectId;
                    else if (long.TryParse(processedValue?.ToString(), out var parsedObjId))
                        valueRecord.Object = parsedObjId;
                    break;
                case "ListItem":
                    // ListItem references are stored in separate _listitem field
                    if (processedValue is IRedbListItem listItem)
                    {
                        valueRecord.ListItem = listItem.Id;
                    }
                    else if (processedValue is long listItemId)
                    {
                        valueRecord.ListItem = listItemId;
                    }
                    else if (long.TryParse(processedValue?.ToString(), out var parsedListId))
                    {
                        valueRecord.ListItem = parsedListId;
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            $"ListItem value cannot be processed: value='{processedValue}', type='{processedValue?.GetType().FullName}'.");
                    }
                    break;
                case "Guid":
                    if (processedValue is Guid guidVal)
                        valueRecord.Guid = guidVal;
                    else if (Guid.TryParse(processedValue?.ToString(), out var parsedGuid))
                        valueRecord.Guid = parsedGuid;
                    break;
                default:
                    valueRecord.String = processedValue?.ToString();
                    break;
            }
        }


  /// <summary>
  /// Converts IRedbStructure to StructureMetadata getting type info from cache
  /// </summary>
  private async Task<List<StructureMetadata>> ConvertStructuresToMetadataAsync(IEnumerable<IRedbStructure> structures)
  {
      var result = new List<StructureMetadata>();

      foreach (var structure in structures)
      {
          // Get type info by IdType via cache or DB
          var typeInfo = await GetTypeInfoAsync(structure.IdType);

          result.Add(new StructureMetadata
          {
              Id = structure.Id,
              IdParent = structure.IdParent,
              Name = structure.Name,
              DbType = typeInfo.DbType,
              IsArray = structure.CollectionType != null,
              StoreNull = structure.StoreNull ?? false,
              Unique = structure.Unique ?? false,
              TypeSemantic = typeInfo.TypeSemantic
          });
      }

      return result;
  }
        /// <summary>
        /// Gets type info by IdType.
        /// </summary>
        private async Task<(string DbType, string TypeSemantic)> GetTypeInfoAsync(long typeId)
        {
            // Direct SQL query for type info
            var typeEntity = await _context.QueryFirstOrDefaultAsync<RedbType>(
                Sql.ObjectStorage_SelectTypeById(), typeId);

            return typeEntity != null
                ? (typeEntity.DbType ?? "String", typeEntity.Type1 ?? "string")
                : ("String", "string");
        }

        /// <summary>
        /// OPTIMIZATION: Get scheme from cache WITHOUT hash validation
        /// Used inside SaveAsync transactions where scheme is guaranteed not to change
        /// </summary>
        protected async Task<IRedbScheme?> GetSchemeFromCacheOrDbAsync(long schemeId)
        {
            // First check cache WITHOUT DB query for hash validation
            var cachedScheme = Cache.GetScheme(schemeId);
            if (cachedScheme != null)
            {
                return cachedScheme; // Cache HIT - return without validation
            }

            // Cache MISS - load via provider (it will cache)
            return await _schemeSync.GetSchemeByIdAsync(schemeId);
        }

    }

    /// <summary>
    /// Structure metadata with extended info including _store_null
    /// </summary>
    internal class StructureMetadata
    {
        public long Id { get; set; }
        public long? IdParent { get; set; }  // Add field for structure hierarchy
        public string Name { get; set; } = string.Empty;
        public string DbType { get; set; } = "String";
        public bool IsArray { get; set; }
        public long? CollectionType { get; set; }  // Array, Dictionary, etc.
        public long? KeyType { get; set; }         // Key type for Dictionary
        public bool StoreNull { get; set; }

        /// <summary>The field is a [RedbUnique] key: its root scalar values carry a hash in _unique.</summary>
        public bool Unique { get; set; }

        /// <summary>S3: element-key scope of a collection key (null = default reading).</summary>
        public long? UniqueScope { get; set; }
        public string TypeSemantic { get; set; } = "string";

        /// <summary>
        /// True if this is a Dictionary field
        /// </summary>
        public bool IsDictionary => CollectionType == RedbTypeIds.Dictionary;
    }

    /// <summary>
    /// DTO for mark_for_deletion SQL function result.
    /// </summary>
    internal class MarkForDeletionResult
    {
        public long trash_id { get; set; }
        public long marked_count { get; set; }
    }

    /// <summary>
    /// DTO for purge_trash SQL function result.
    /// </summary>
    internal class PurgeTrashResult
    {
        public long deleted_count { get; set; }
        public long remaining_count { get; set; }
    }

    /// <summary>
    /// DTO for deletion progress SQL query result.
    /// </summary>
    internal class TrashProgressRow
    {
        public long trash_id { get; set; }
        public long total { get; set; }
        public long deleted { get; set; }
        public string status { get; set; } = "pending";
        public DateTimeOffset started_at { get; set; }
        public long owner_id { get; set; }
    }
}
