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
            ILogger? logger = null)
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
        public async Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadAsync<TProps>(objectId, effectiveUser, depth, lazyLoadProps);
        }

        /// <summary>
        /// Load object from EAV (uses _securityContext and config.DefaultCheckPermissionsOnLoad)
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false
        /// </summary>
        public async Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadAsync<TProps>(obj.Id, effectiveUser, depth, lazyLoadProps);
        }

        /// <summary>
        /// Load object from EAV with explicitly specified user (uses config.DefaultCheckPermissionsOnLoad)
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false
        /// </summary>
        public async Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            return await LoadAsync<TProps>(obj.Id, user, depth, lazyLoadProps);
        }

        // ===== OVERLOADS WITH EXPLICIT USER =====

        /// <summary>
        /// MAIN loading method - all other LoadAsync methods call it
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false
        /// </summary>
        public async Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
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

            // ✅ If lazyLoadProps is not specified, take from configuration
            var shouldLazyLoad = lazyLoadProps ?? _configuration.EnableLazyLoadingForProps;
            

            // === NEW LOGIC: Lazy loading of Props ===
            if (shouldLazyLoad)
            {
                // ✅ OPTIMIZATION for SkipHashValidationOnCacheCheck: first check cache without DB query
                if (_configuration.EnablePropsCache && _configuration.SkipHashValidationOnCacheCheck && PropsCache.Instance != null)
                {
                    var cachedObj = PropsCache.GetWithoutHashValidation<TProps>(objectId);
                    if (cachedObj != null)
                    {
                        // 🚀 Cache HIT WITHOUT DB query - return cached object
                        return cachedObj;
                    }
                }
                
                // Cache MISS or hash check enabled - load all base fields at once (1 query)
                var baseObj = await _context.QueryFirstOrDefaultAsync<RedbObjectRow>(
                    Sql.ObjectStorage_SelectObjectById(), objectId);
                
                if (baseObj == null)
                {
                    if (_configuration.ThrowOnObjectNotFound)
                        throw new InvalidOperationException($"Object with ID {objectId} not found");
                    return null;
                }
                
                // Check cache with obtained hash (if validation is not skipped)
                if (_configuration.EnablePropsCache && !_configuration.SkipHashValidationOnCacheCheck && PropsCache.Instance != null && baseObj.Hash.HasValue)
                {
                    var cachedObj = PropsCache.Get<TProps>(objectId, baseObj.Hash.Value);
                    if (cachedObj != null)
                    {
                        // 🚀 Cache HIT - return cached object
                        return cachedObj;
                    }
                }
                
                // Cache MISS - create RedbObject with base fields
                var result = new RedbObject<TProps>
                {
                    id = baseObj.Id,
                    name = baseObj.Name,
                    scheme_id = baseObj.IdScheme,
                    parent_id = baseObj.IdParent,
                    owner_id = baseObj.IdOwner,
                    who_change_id = baseObj.IdWhoChange,
                    date_create = baseObj.DateCreate,
                    date_modify = baseObj.DateModify,
                    date_begin = baseObj.DateBegin,
                    date_complete = baseObj.DateComplete,
                    key = baseObj.Key,
                    value_long = baseObj.ValueLong,
                    value_string = baseObj.ValueString,
                    value_guid = baseObj.ValueGuid,
                    value_bool = baseObj.ValueBool,
                    value_double = baseObj.ValueDouble,
                    value_numeric = baseObj.ValueNumeric,
                    value_datetime = baseObj.ValueDatetime,
                    value_bytes = baseObj.ValueBytes,
                    note = baseObj.Note,
                    hash = baseObj.Hash
                };
                
                // Set lazy loader
                result._lazyLoader = CreateLazyPropsLoader();
                
                // ⚠️ DON'T CACHE OBJECT WITHOUT Props!
                // Object will be cached by LazyPropsLoader AFTER Props materialization (line 330 in LazyPropsLoader.cs)
                // Otherwise we get recursive deadlock when accessing Props from cache
                
                return result;
            }
            
            // === EAGER LOADING: Full loading via get_object_json with cache ===
            
            // ✅ OPTIMIZATION for SkipHashValidationOnCacheCheck: check cache first without DB query
            if (_configuration.EnablePropsCache && _configuration.SkipHashValidationOnCacheCheck && PropsCache.Instance != null)
            {
                var cachedObj = PropsCache.GetWithoutHashValidation<TProps>(objectId);
                if (cachedObj != null)
                {
                    // 🚀 Cache HIT WITHOUT DB query - return cached object
                    return cachedObj;
                }
            }
            
            // Cache MISS or hash check enabled - query ID + Hash to distinguish "object not found" and "hash=null"
            var eagerBaseObj = await _context.QueryFirstOrDefaultAsync<RedbObjectRow>(
                Sql.ObjectStorage_SelectIdHash(), objectId);
            
            if (eagerBaseObj == null)
            {
                if (_configuration.ThrowOnObjectNotFound)
                    throw new InvalidOperationException($"Object with ID {objectId} not found");
                return null;
            }
            
            // Object found, but hash may be null
            var objectHash = eagerBaseObj.Hash;

            // Check cache only if hash is NOT null AND validation is not skipped
            if (_configuration.EnablePropsCache && !_configuration.SkipHashValidationOnCacheCheck && PropsCache.Instance != null && objectHash.HasValue)
            {
                var cachedObj = PropsCache.Get<TProps>(objectId, objectHash.Value);
                if (cachedObj != null)
                {
                    // 🚀 Cache HIT - return cached object
                    return cachedObj;
                }
            }
            
            // Cache MISS - load via get_object_json
            return await LoadEagerAsync<TProps>(objectId, depth);
        }
        
        /// <summary>
        /// ✅ Virtual method for EAGER loading of Props.
        /// Open Source: uses get_object_json
        /// Pro: overrides for PVT approach
        /// </summary>
        protected virtual async Task<RedbObject<TProps>?> LoadEagerAsync<TProps>(long objectId, int depth) where TProps : class, new()
        {
            
            var json = await _context.ExecuteJsonAsync(
                Sql.ObjectStorage_GetObjectJson(), objectId, depth);

            if (string.IsNullOrEmpty(json))
            {
                if (_configuration.ThrowOnObjectNotFound)
                    throw new InvalidOperationException($"Object with ID {objectId} not found");
                return null;
            }

            // Deserialize JSON into RedbObject<TProps>
            var loadedObj = _serializer.Deserialize<TProps>(json);
            
            // ✅ Put main object + ALL nested RedbObject<T> into cache
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
            
            // ✅ CRITICAL: Check TYPE IMMEDIATELY before visited.Add()!
            // This prevents infinite boxing for value types (DateTime, int, etc.)
            if (objType.IsPrimitive || objType.IsValueType || objType.Namespace?.StartsWith("System") == true)
            {
                return;
            }

            // ✅ Only AFTER type check add to visited (protection against circular references)
            if (!visited.Add(obj))
            {
                return;
            }
            
            // If this is RedbObject<T> itself → cache it
            if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(RedbObject<>))
            {
                var redbObj = obj as IRedbObject;
                if (redbObj != null && redbObj.Hash.HasValue)
                {
                    // Dynamically call PropsCache.Set<TProps>(redbObj)
                    var propsType = objType.GetGenericArguments()[0];
                    var setMethod = typeof(GlobalPropsCache).GetMethod("Set")?.MakeGenericMethod(propsType);
                    setMethod?.Invoke(PropsCache, new[] { obj });
                    
                    // Get Props via reflection
                    var propsProperty = objType.GetProperty("Props");
                    if (propsProperty != null)
                    {
                        var propsValue = propsProperty.GetValue(obj);
                        if (propsValue != null)
                        {
                            CacheNestedObjectsInternal(propsValue, visited);
                        }
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
        public async Task<long> SaveAsync(IRedbObject obj)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await SaveAsync(obj, effectiveUser);
        }
        
        /// <summary>
        /// Save single object via interface with explicit user.
        /// </summary>
        public async Task<long> SaveAsync(IRedbObject obj, IRedbUser user)
        {
            var results = await SaveAsync(new[] { obj }, user);
            return results.FirstOrDefault();
        }
        
        public async Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await SaveAsync(obj, effectiveUser);
        }


        public async Task<bool> DeleteAsync(IRedbObject obj)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await DeleteAsync(obj, effectiveUser);
        }

        /// <summary>
        /// Deletes an object from the database using atomic ExecuteDeleteAsync.
        /// Thread-safe and handles concurrent delete attempts gracefully.
        /// </summary>
        public async Task<bool> DeleteAsync(IRedbObject obj, IRedbUser user)
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

            // Atomic delete via SQL - single query, no race condition
            var deletedCount = await _context.ExecuteAsync(
                Sql.ObjectStorage_DeleteById(), obj.Id);

            if (deletedCount == 0)
            {
                return false;  // Object did not exist or was already deleted
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
        public async Task<bool> DeleteAsync(long objectId)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await DeleteAsync(objectId, effectiveUser);
        }

        /// <summary>
        /// Delete object by ID with explicit user
        /// </summary>
        public async Task<bool> DeleteAsync(long objectId, IRedbUser user)
        {
            var deletedCount = await DeleteAsync(new[] { objectId }, user);
            return deletedCount > 0;
        }

        // ===== BULK DELETION BY ID =====

        /// <summary>
        /// Bulk deletion of objects by ID (uses _securityContext)
        /// </summary>
        public async Task<int> DeleteAsync(IEnumerable<long> objectIds)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await DeleteAsync(objectIds, effectiveUser);
        }

        /// <summary>
        /// Bulk deletion of objects by ID with explicit user
        /// </summary>
        public async Task<int> DeleteAsync(IEnumerable<long> objectIds, IRedbUser user)
        {
            var ids = objectIds.ToList();
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

            // Bulk delete via SQL (single query with ANY)
            var deletedCount = await _context.ExecuteAsync(
                Sql.ObjectStorage_DeleteByIds(), ids.ToArray());

            // === CACHE INVALIDATION (ALWAYS) ===
            if (PropsCache.Instance != null)
            {
                foreach (var id in ids)
                {
                    PropsCache.Remove(id);
                }
            }

            return deletedCount;
        }

        // ===== BULK DELETION BY INTERFACE =====

        /// <summary>
        /// Bulk deletion of objects by interface (uses _securityContext)
        /// </summary>
        public async Task<int> DeleteAsync(IEnumerable<IRedbObject> objects)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await DeleteAsync(objects, effectiveUser);
        }

        /// <summary>
        /// Bulk deletion of objects by interface with explicit user
        /// </summary>
        public async Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user)
        {
            var objList = objects.ToList();
            if (objList.Count == 0) return 0;

            var ids = objList.Select(o => o.Id).ToList();
            
            // Call the base deletion method by ID
            var deletedCount = await DeleteAsync(ids, user);

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
        public async Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, long? trashParentId = null)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await SoftDeleteAsync(objectIds, effectiveUser, trashParentId);
        }
        
        /// <summary>
        /// Mark objects for soft-deletion with explicit user.
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        public async Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, IRedbUser user, long? trashParentId = null)
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
                ids, 
                user.Id,
                trashParentId);
            
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
        public Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, long? trashParentId = null)
        {
            return SoftDeleteAsync(objects.Select(o => o.Id), trashParentId);
        }
        
        /// <summary>
        /// Mark objects for soft-deletion with explicit user.
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        public Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, long? trashParentId = null)
        {
            return SoftDeleteAsync(objects.Select(o => o.Id), user, trashParentId);
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
            var mark = await SoftDeleteAsync(objectIds, trashParentId);
            
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
            
            while (!cancellationToken.IsCancellationRequested)
            {
                // Call purge_trash SQL function
                var results = await _context.QueryAsync<PurgeTrashResult>(
                    Sql.SoftDelete_PurgeTrash(), 
                    trashId, 
                    batchSize);
                
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
            
            if (cancellationToken.IsCancellationRequested)
            {
                progress?.Report(new PurgeProgress(
                    trashId, deleted, totalCount - deleted, PurgeStatus.Cancelled, startedAt, effectiveUser.Id));
            }
            
            _logger?.LogInformation(
                "PurgeTrash completed. TrashId={TrashId}, Deleted={Deleted}, User={UserId}", 
                trashId, deleted, effectiveUser.Id);
        }
        
        /// <summary>
        /// Gets deletion progress for a specific trash container from database.
        /// Returns null if trash container not found or already deleted.
        /// </summary>
        public async Task<PurgeProgress?> GetDeletionProgressAsync(long trashId)
        {
            var results = await _context.QueryAsync<TrashProgressRow>(
                Sql.SoftDelete_GetDeletionProgress(), trashId);
            
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
        public async Task<List<PurgeProgress>> GetUserActiveDeletionsAsync(long userId)
        {
            var results = await _context.QueryAsync<TrashProgressRow>(
                Sql.SoftDelete_GetUserActiveDeletions(), userId);
            
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
        public async Task<List<OrphanedTask>> GetOrphanedDeletionTasksAsync(int timeoutMinutes = 30)
        {
            var results = await _context.QueryAsync<OrphanedTaskRow>(
                Sql.SoftDelete_GetOrphanedTasks(), timeoutMinutes);
            
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
        public async Task<bool> TryClaimOrphanedTaskAsync(long trashId, int timeoutMinutes = 30)
        {
            var affected = await _context.ExecuteAsync(
                Sql.SoftDelete_ClaimOrphanedTask(), trashId, timeoutMinutes);
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
        public async Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, int depth = 10, bool? lazyLoadProps = null)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadAsync(objectIds, effectiveUser, depth, lazyLoadProps);
        }

        /// <summary>
        /// Bulk loading of objects by ID with explicit user
        /// Supports two modes: EAGER (get_object_json) and LAZY (_objects + LoadPropsForManyAsync)
        /// </summary>
        public async Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, bool? lazyLoadProps = null)
        {
            var ids = objectIds.ToList();
            if (ids.Count == 0) return new List<IRedbObject>();

            // ✅ If lazyLoadProps is not specified, take from configuration (default is true for bulk load if not set in config)
            var shouldLazyLoad = lazyLoadProps ?? _configuration.EnableLazyLoadingForProps;

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
                            Sql.ObjectStorage_SelectIdHashScheme(), id);
                        
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
            List<IRedbObject> loadedObjects;
            
            if (shouldLazyLoad)
            {
                // LAZY mode: base fields + LoadPropsForManyAsync
                loadedObjects = await LoadObjectsLazyAsync(idsToLoad);
            }
            else
            {
                // EAGER mode: get_object_json
                loadedObjects = await LoadObjectsEagerAsync(idsToLoad, depth);
            }

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
        /// ✅ protected for access from Pro version
        /// </summary>
        protected async Task<List<IRedbObject>> LoadObjectsLazyAsync(List<long> objectIds)
        {
            if (objectIds.Count == 0) return new List<IRedbObject>();

            // Load base fields from _objects via SQL
            var baseObjects = await _context.QueryAsync<RedbObjectRow>(
                Sql.ObjectStorage_SelectObjectsByIds(), objectIds.ToArray());

            if (baseObjects.Count == 0) return new List<IRedbObject>();

            // Group by scheme_id for polymorphic deserialization
            var result = new List<IRedbObject>();
            var objectsByScheme = baseObjects.GroupBy(o => o.IdScheme);

            foreach (var schemeGroup in objectsByScheme)
            {
                var schemeId = schemeGroup.Key;
                var objectsForScheme = schemeGroup.ToList();

                // Get type through AutomaticTypeRegistry
                var propsType = Cache.GetClrType(schemeId);
                
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
                    var typedObjects = createMethod.Invoke(this, new object[] { objectsForScheme }) as System.Collections.IEnumerable;
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
            return new RedbObject
            {
                id = baseObj.Id,
                name = baseObj.Name,
                scheme_id = baseObj.IdScheme,
                parent_id = baseObj.IdParent,
                owner_id = baseObj.IdOwner,
                who_change_id = baseObj.IdWhoChange,
                date_create = baseObj.DateCreate,
                date_modify = baseObj.DateModify,
                date_begin = baseObj.DateBegin,
                date_complete = baseObj.DateComplete,
                key = baseObj.Key,
                value_long = baseObj.ValueLong,
                value_string = baseObj.ValueString,
                value_guid = baseObj.ValueGuid,
                value_bool = baseObj.ValueBool,
                value_double = baseObj.ValueDouble,
                value_numeric = baseObj.ValueNumeric,
                value_datetime = baseObj.ValueDatetime,
                value_bytes = baseObj.ValueBytes,
                note = baseObj.Note,
                hash = baseObj.Hash
            };
        }

        /// <summary>
        /// Creates RedbObject&lt;TProps&gt; from base RedbObjectRow fields with LazyPropsLoader and LoadPropsForManyAsync
        /// </summary>
        private List<RedbObject<TProps>> CreateRedbObjectsFromBaseObjects<TProps>(List<RedbObjectRow> baseObjects) where TProps : class, new()
        {
            var redbObjects = baseObjects.Select(baseObj => new RedbObject<TProps>
            {
                id = baseObj.Id,
                name = baseObj.Name,
                scheme_id = baseObj.IdScheme,
                parent_id = baseObj.IdParent,
                owner_id = baseObj.IdOwner,
                who_change_id = baseObj.IdWhoChange,
                date_create = baseObj.DateCreate,
                date_modify = baseObj.DateModify,
                date_begin = baseObj.DateBegin,
                date_complete = baseObj.DateComplete,
                key = baseObj.Key,
                value_long = baseObj.ValueLong,
                value_string = baseObj.ValueString,
                value_guid = baseObj.ValueGuid,
                value_bool = baseObj.ValueBool,
                value_double = baseObj.ValueDouble,
                value_numeric = baseObj.ValueNumeric,
                value_datetime = baseObj.ValueDatetime,
                value_bytes = baseObj.ValueBytes,
                note = baseObj.Note,
                hash = baseObj.Hash,
                _lazyLoader = CreateLazyPropsLoader()
            }).ToList();

            // Bulk load Props via LoadPropsForManyAsync
            var lazyLoader = CreateLazyPropsLoader();
            lazyLoader.LoadPropsForManyAsync(redbObjects).Wait();  // Wait synchronously

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
        protected virtual async Task<List<IRedbObject>> LoadObjectsEagerAsync(List<long> objectIds, int depth)
        {
            if (objectIds.Count == 0) return new List<IRedbObject>();

            // Use unnest for bulk get_object_json call
            var idsArray = objectIds.ToArray();
            
            // Execute get_object_json for each ID via SQL
            var objectsJsonList = await _context.ExecuteJsonListAsync(
                Sql.ObjectStorage_GetObjectsJsonBulk(), idsArray, depth);

            var result = new List<IRedbObject>();

            foreach (var objectJson in objectsJsonList)
            {
                if (string.IsNullOrEmpty(objectJson)) continue;

                try
                {
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
                catch (Exception ex)
                {
                   throw ex;
                }
            }

            return result;
        }

        // ===== LOAD WITH PARENT CHAIN =====

        /// <summary>
        /// Load object by ID with parent chain to root (uses _securityContext).
        /// </summary>
        public async Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(long objectId, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadWithParentsAsync<TProps>(objectId, effectiveUser, depth, lazyLoadProps);
        }

        /// <summary>
        /// Load object with parent chain to root (uses _securityContext).
        /// </summary>
        public async Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(IRedbObject obj, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadWithParentsAsync<TProps>(obj.Id, effectiveUser, depth, lazyLoadProps);
        }

        /// <summary>
        /// Load object by ID with parent chain to root with explicit user.
        /// </summary>
        public async Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            return await LoadWithParentsAsync<TProps>(obj.Id, user, depth, lazyLoadProps);
        }

        /// <summary>
        /// MAIN LoadWithParentsAsync - loads single object with parent chain.
        /// Delegates to bulk method for implementation reuse.
        /// </summary>
        public async Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(long objectId, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            var list = await LoadWithParentsAsync<TProps>(new[] { objectId }, user, depth, lazyLoadProps);
            return list.FirstOrDefault();
        }

        /// <summary>
        /// Bulk load objects with parent chains (uses _securityContext).
        /// </summary>
        public async Task<List<TreeRedbObject<TProps>>> LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadWithParentsAsync<TProps>(objectIds, effectiveUser, depth, lazyLoadProps);
        }

        /// <summary>
        /// MAIN bulk LoadWithParentsAsync - loads objects with parent chains to root.
        /// Uses recursive CTE to get all ancestor IDs, then bulk loads all objects.
        /// Parents are loaded polymorphically (each with its real Props type).
        /// </summary>
        public async Task<List<TreeRedbObject<TProps>>> LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new()
        {
            var ids = objectIds.ToList();
            if (ids.Count == 0) return new List<TreeRedbObject<TProps>>();

            // 1. Get IDs of all objects and their ancestors via recursive CTE
            var idsString = string.Join(",", ids);
            var sql = _sql.Query_GetIdsWithAncestorsSql(idsString);
            var allIds = await _context.QueryScalarListAsync<long>(sql);

            // 2. Load all objects polymorphically (target + parents with real types)
            var loadedObjects = await LoadAsync(allIds, user, depth, lazyLoadProps);

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
        public async Task<List<ITreeRedbObject>> LoadWithParentsAsync(IEnumerable<long> objectIds, int depth = 10, bool? lazyLoadProps = null)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await LoadWithParentsAsync(objectIds, effectiveUser, depth, lazyLoadProps);
        }

        /// <summary>
        /// MAIN bulk polymorphic LoadWithParentsAsync.
        /// Preserves actual Props types for each object.
        /// </summary>
        public async Task<List<ITreeRedbObject>> LoadWithParentsAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, bool? lazyLoadProps = null)
        {
            var ids = objectIds.ToList();
            if (ids.Count == 0) return new List<ITreeRedbObject>();

            // 1. Get IDs of all objects and their ancestors
            var idsString = string.Join(",", ids);
            var sql = _sql.Query_GetIdsWithAncestorsSql(idsString);
            var allIds = await _context.QueryScalarListAsync<long>(sql);

            // 2. Load all objects
            var loadedObjects = await LoadAsync(allIds, user, depth, lazyLoadProps);

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

        public async Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, IRedbUser user) where TProps : class, new()
        {
            
            // Strategy selection based on configuration
            var strategy = _configuration.EavSaveStrategy;
            
            
            // Each strategy manages its own transaction internally
            long result;
            switch (strategy)
            {
                case EavSaveStrategy.DeleteInsert:
                    result = await SaveAsyncDeleteInsertBulk(obj, user);
                    break;
                    
                case EavSaveStrategy.ChangeTracking:
                    result = await SaveAsyncNew(obj, user);
                    break;
                    
                default:
                    throw new NotSupportedException($"Strategy {strategy} is not supported");
            }
            
            return result;
        }

        /// <summary>
        /// Saving properties with strategy selection based on configuration
        /// </summary>
        private async Task SavePropertiesAsync<TProps>(long objectId, long schemeId, TProps properties) where TProps : class
        {
            var strategy = _configuration.EavSaveStrategy;

            switch (strategy)
            {
                case EavSaveStrategy.DeleteInsert:
                    await SavePropertiesWithDeleteInsert(objectId, schemeId, properties);
                    break;

                case EavSaveStrategy.ChangeTracking:
                    await SavePropertiesWithChangeTracking(objectId, schemeId, properties);
                    break;

                default:
                    throw new NotSupportedException($"Strategy {strategy} is not supported");
            }
        }

        /// <summary>
        /// DELETE + INSERT strategy - removes all existing values and creates new ones
        /// </summary>
        private async Task SavePropertiesWithDeleteInsert<TProps>(long objectId, long schemeId, TProps properties) where TProps : class
        {
            // Get scheme structures with extended data including _store_null
            var structures = await GetStructuresWithMetadataAsync(schemeId);

            // Remove existing values
            await DeleteExistingValuesAsync(objectId);

            // Save new values
            await SavePropertiesFromObjectAsync(objectId, schemeId, structures, properties);
        }

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
            public string TypeSemantic { get; set; } = "string";
        }

        /// <summary>
        /// Delete all existing values for object.
        /// </summary>
        private async Task DeleteExistingValuesAsync(long objectId)
        {
            await _context.Bulk.BulkDeleteValuesByObjectIdsAsync(new[] { objectId });
        }

        /// <summary>
        /// Save object Props according to scheme structures
        /// </summary>
        private async Task SavePropertiesFromObjectAsync<TProps>(long objectId, long schemeId, List<StructureMetadata> structures, TProps properties) where TProps : class
        {
            var propertiesType = typeof(TProps);

            foreach (var structure in structures)
            {
                var property = propertiesType.GetProperty(structure.Name);
                if (property == null || property.GetIndexParameters().Length > 0)
                {
                    throw new InvalidOperationException(
                        $"Property '{structure.Name}' not found in type '{propertiesType.Name}' or is an indexer.");
                }

                // 🚫 IGNORE fields with [JsonIgnore] or [RedbIgnore] attribute
                if (property.ShouldIgnoreForRedb())
                {
                    continue;
                }

                var rawValue = property.GetValue(properties);

                // ✅ NEW NULL SEMANTICS: check _store_null
                if (!ObjectStorageProviderExtensions.ShouldCreateValueRecord(rawValue, structure.StoreNull))
                {
                    continue;
                }

                // ✅ NEW ARCHITECTURE: different strategies for different field types
                if (structure.IsDictionary)
                {
                    await SaveDictionaryFieldAsync(objectId, structure, rawValue, schemeId);
                }
                else if (structure.IsArray)
                {
                    await SaveArrayFieldAsync(objectId, structure, rawValue, schemeId);
                }
                else if (ObjectStorageProviderExtensions.IsClassType(structure.TypeSemantic))
                {
                    await SaveClassFieldAsync(objectId, structure, rawValue, schemeId);
                }
                else
                {
                    await SaveSimpleFieldAsync(objectId, structure, rawValue);
                }
            }
        }

        /// <summary>
        /// ChangeTracking strategy - compares with DB and updates only changed Props
        /// </summary>
        private async Task SavePropertiesWithChangeTracking<TProps>(long objectId, long schemeId, TProps properties) where TProps : class
        {
            // Get scheme structures from cache
            var scheme = await _schemeSync.GetSchemeByIdAsync(schemeId);
            if (scheme == null)
                throw new InvalidOperationException($"Scheme with ID {schemeId} not found");

            // Load existing values from DB
            var existingValues = await LoadExistingValuesAsync(objectId, scheme.Structures);

            // Extract current object properties
            var currentProperties = await ExtractCurrentPropertiesAsync(properties, scheme.Structures);

            // Determine what needs to be changed
            await ApplyPropertyChangesAsync(objectId, existingValues, currentProperties);
        }

        /// <summary>
        /// Load existing values from DB for object
        /// </summary>
        private async Task<Dictionary<string, ExistingValueInfo>> LoadExistingValuesAsync(long objectId, IReadOnlyCollection<IRedbStructure> structures)
        {
            var structureIds = structures.Select(s => s.Id).ToList();

            // Load existing values with type info via SQL JOIN
            var existingValuesWithTypes = await _context.QueryAsync<ExistingValueRow>(
                Sql.ObjectStorage_SelectValuesWithTypes(), objectId, structureIds.ToArray());

            var result = new Dictionary<string, ExistingValueInfo>();

            foreach (var item in existingValuesWithTypes)
            {
                var structure = structures.First(s => s.Id == item.IdStructure);

                var valueRecord = new RedbValue
                {
                    Id = item.Id, IdStructure = item.IdStructure, IdObject = item.IdObject,
                    String = item.String, Long = item.Long, Guid = item.Guid, Double = item.Double,
                    DateTimeOffset = item.DateTimeOffset, Boolean = item.Boolean, ByteArray = item.ByteArray,
                    Numeric = item.Numeric, ListItem = item.ListItem, Object = item.Object,
                    ArrayParentId = item.ArrayParentId, ArrayIndex = item.ArrayIndex
                };

                result[structure.Name] = new ExistingValueInfo
                {
                    ValueRecord = valueRecord,
                    StructureId = structure.Id,
                    DbType = item.DbType,
                    IsArray = structure.CollectionType != null,
                    ExtractedValue = ExtractValueFromRecord(valueRecord, item.DbType, structure.CollectionType != null)
                };
            }

            return result;
        }
        
        /// <summary>
        /// Helper class for mapping existing value query results.
        /// </summary>
        private class ExistingValueRow
        {
            public long Id { get; set; }
            public long IdStructure { get; set; }
            public long IdObject { get; set; }
            public string? String { get; set; }
            public long? Long { get; set; }
            public Guid? Guid { get; set; }
            public double? Double { get; set; }
            public DateTimeOffset? DateTimeOffset { get; set; }
            public bool? Boolean { get; set; }
            public byte[]? ByteArray { get; set; }
            public decimal? Numeric { get; set; }
            public long? ListItem { get; set; }
            public long? Object { get; set; }
            public long? ArrayParentId { get; set; }
            public string? ArrayIndex { get; set; }
            public string DbType { get; set; } = "String";
        }
        
        /// <summary>
        /// Helper class for mapping structure type query results.
        /// </summary>
        private class StructureTypeRow
        {
            public long StructureId { get; set; }
            public string DbType { get; set; } = "String";
        }

        /// <summary>
        /// Extract current properties from object via reflection.
        /// </summary>
        private async Task<Dictionary<string, CurrentPropertyInfo>> ExtractCurrentPropertiesAsync<TProps>(TProps properties, IReadOnlyCollection<IRedbStructure> structures) where TProps : class
        {
            var result = new Dictionary<string, CurrentPropertyInfo>();
            var propertiesType = typeof(TProps);

            // Get type info from DB for all structures via SQL
            var structureIds = structures.Select(s => s.Id).ToList();
            var structureTypeRows = await _context.QueryAsync<StructureTypeRow>(
                Sql.ObjectStorage_SelectStructureTypes(), structureIds.ToArray());
            var structureTypes = structureTypeRows.ToDictionary(x => x.StructureId, x => x.DbType);

            foreach (var structure in structures)
            {
                var property = propertiesType.GetProperty(structure.Name);
                if (property == null || property.GetIndexParameters().Length > 0) continue;

                // Ignore fields with [JsonIgnore] or [RedbIgnore] attribute
                if (property.ShouldIgnoreForRedb()) continue;

                var rawValue = property.GetValue(properties);
                var dbType = structureTypes.GetValueOrDefault(structure.Id, "String");

                result[structure.Name] = new CurrentPropertyInfo
                {
                    Value = rawValue,
                    StructureId = structure.Id,
                    DbType = dbType,
                    IsArray = structure.CollectionType != null
                };
            }

            return result;
        }

        /// <summary>
        /// Apply changes - INSERT/UPDATE/DELETE only for changed properties
        /// </summary>
        private async Task ApplyPropertyChangesAsync(long objectId, Dictionary<string, ExistingValueInfo> existing, Dictionary<string, CurrentPropertyInfo> current)
        {
            var allFieldNames = existing.Keys.Union(current.Keys).ToList();

            foreach (var fieldName in allFieldNames)
            {
                var hasExisting = existing.TryGetValue(fieldName, out var existingInfo);
                var hasCurrent = current.TryGetValue(fieldName, out var currentInfo);

                if (!hasExisting && hasCurrent && currentInfo.Value != null)
                {
                    // INSERT new value
                    await InsertNewValueAsync(objectId, currentInfo);
                }
                else if (hasExisting && (!hasCurrent || currentInfo.Value == null))
                {
                    // DELETE removed value
                    await _context.Bulk.BulkDeleteValuesAsync(new[] { existingInfo!.ValueRecord.Id });
                }
                else if (hasExisting && hasCurrent && currentInfo.Value != null && !ValuesAreEqual(existingInfo.ExtractedValue, currentInfo.Value))
                {
                    // UPDATE changed value
                    await UpdateExistingValueAsync(existingInfo.ValueRecord, currentInfo);
                }
                // else: value not changed - skip
            }
        }

        /// <summary>
        /// INSERT new value into _values
        /// </summary>
        private async Task InsertNewValueAsync(long objectId, CurrentPropertyInfo currentInfo)
        {
            var valueRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = objectId,
                IdStructure = currentInfo.StructureId
            };

            var processedValue = await ProcessNestedObjectsAsync(currentInfo.Value, currentInfo.DbType ?? "String", currentInfo.IsArray, objectId);
            SetSimpleValueByType(valueRecord, currentInfo.DbType ?? "String", processedValue);

            await _context.Bulk.BulkInsertValuesAsync(new[] { valueRecord });
        }

        /// <summary>
        /// UPDATE existing value in _values.
        /// </summary>
        private async Task UpdateExistingValueAsync(RedbValue existingRecord, CurrentPropertyInfo currentInfo)
        {
            // Clear all fields
            ClearValueRecord(existingRecord);

            var processedValue = await ProcessNestedObjectsAsync(currentInfo.Value, currentInfo.DbType ?? "String", currentInfo.IsArray, existingRecord.IdObject);
            SetSimpleValueByType(existingRecord, currentInfo.DbType ?? "String", processedValue);
        }

        /// <summary>
        /// Compare two values for equality
        /// </summary>
        private bool ValuesAreEqual(object? existing, object? current)
        {
            if (existing == null && current == null) return true;
            if (existing == null || current == null) return false;

            // Element-wise comparison for arrays
            if (existing is Array arrayA && current is Array arrayB)
            {
                if (arrayA.Length != arrayB.Length) return false;

                for (int i = 0; i < arrayA.Length; i++)
                {
                    if (!Equals(arrayA.GetValue(i), arrayB.GetValue(i)))
                        return false;
                }
                return true;
            }

            return Equals(existing, current);
        }

        /// <summary>
        /// Extract value from RedbValue record
        /// </summary>
        private object? ExtractValueFromRecord(RedbValue valueRecord, string? dbType, bool isArray)
        {
            if (isArray)
                return null;  // ✅ Arrays are now stored relationally, not in JSON

            return dbType switch
            {
                "String" => valueRecord.String,
                "Long" => valueRecord.Long,
                "Double" => valueRecord.Double,
                "Boolean" => valueRecord.Boolean,
                "DateTimeOffset" => valueRecord.DateTimeOffset,
                "Guid" => valueRecord.Guid,
                "ByteArray" => valueRecord.ByteArray,
                _ => valueRecord.String
            };
        }

        /// <summary>
        /// Clear all fields of RedbValue record
        /// </summary>
        private void ClearValueRecord(RedbValue valueRecord)
        {
            valueRecord.String = null;
            valueRecord.Long = null;
            valueRecord.Guid = null;
            valueRecord.Double = null;
            valueRecord.DateTimeOffset = null;
            valueRecord.Boolean = null;
            valueRecord.ByteArray = null;

        }

        /// <summary>
        /// 🚀 AUTOSAVE: Processes nested RedbObjects, saving them recursively
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
                        // 🎯 SET PARENT: If nested object has no parent, set base one
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
                        // ✅ Process IRedbListItem in array - extract Id
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
                // 🎯 SET PARENT: If nested object has no parent, set base one
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

            // ✅ Process IRedbListItem - extract Id
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
        /// ✅ UPDATED VERSION: Removed JSON arrays, only simple types
        /// </summary>
        private static void SetSimpleValueByType(RedbValue valueRecord, string dbType, object? processedValue)
        {
            if (processedValue == null) return;

            // ❌ ARRAYS NOT PROCESSED - they go through SaveArrayFieldAsync
            
            // Direct assignment of typed values
            switch (dbType)
            {
                case "String":
                case "Text":
                    valueRecord.String = processedValue?.ToString();
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
                case "DateTime":
                    if (processedValue is DateTime dateTime)
                    {
                        // ✅ Use centralized converter: DateTime → UTC
                        valueRecord.DateTimeOffset = Core.Utils.DateTimeConverter.NormalizeForStorage(dateTime);
                    }
                    else if (DateTime.TryParse(processedValue?.ToString(), out var parsedDate))
                    {
                        // ✅ Use centralized converter: DateTime → UTC
                        valueRecord.DateTimeOffset = Core.Utils.DateTimeConverter.NormalizeForStorage(parsedDate);
                    }
                    break;
                case "DateTimeOffset":
                    if (processedValue is DateTimeOffset dateTimeOffset)
                        valueRecord.DateTimeOffset = dateTimeOffset;
                    else if (processedValue is DateTime dt)
                        valueRecord.DateTimeOffset = Core.Utils.DateTimeConverter.NormalizeForStorage(dt);
                    else if (DateTimeOffset.TryParse(processedValue?.ToString(), out var parsedDate))
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
        /// 🚀 OPTIMIZATION: Get scheme from cache WITHOUT hash validation
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
    /// Information about existing value from DB
    /// </summary>
    internal class ExistingValueInfo
    {
        public RedbValue ValueRecord { get; set; } = null!;
        public long StructureId { get; set; }
        public string? DbType { get; set; }
        public bool IsArray { get; set; }
        public object? ExtractedValue { get; set; }
    }

    /// <summary>
    /// Information about current object property
    /// </summary>
    internal class CurrentPropertyInfo
    {
        public object? Value { get; set; }
        public long StructureId { get; set; }
        public string? DbType { get; set; }
        public bool IsArray { get; set; }
    }

    /// <summary>
    /// Structure metadata with extended info including _store_null
    /// </summary>
    internal class StructureMetadata
    {
        public long Id { get; set; }
        public long? IdParent { get; set; }  // ✅ Add field for structure hierarchy
        public string Name { get; set; } = string.Empty;
        public string DbType { get; set; } = "String";
        public bool IsArray { get; set; }
        public long? CollectionType { get; set; }  // Array, Dictionary, etc.
        public long? KeyType { get; set; }         // Key type for Dictionary
        public bool StoreNull { get; set; }
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
