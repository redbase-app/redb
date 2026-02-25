using redb.Core.Providers;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Utils;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Configuration;
using redb.Core.Models.Security;
using redb.Core.Caching;
using redb.Core.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// 🚀 BATCH SAVE - high-performance saving of multiple objects
    /// Supports two strategies: DeleteInsert (bulk operations) and ChangeTracking (EF diff)
    /// </summary>
    public abstract partial class ObjectStorageProviderBase
    {
        /// <summary>
        /// Convert IRedbObject to RedbObjectRow for bulk operations
        /// Uses 'as' optimization for direct access to RedbObject fields.
        /// </summary>
        protected RedbObjectRow ConvertToObjectRecord(IRedbObject obj)
        {
            var redbObj = obj as RedbObject;  // Optimization: direct access to fields
            
            var record = new RedbObjectRow
            {
                Id = redbObj?.id ?? obj.Id,
                IdScheme = redbObj?.scheme_id ?? obj.SchemeId,
                IdParent = (redbObj?.parent_id ?? obj.ParentId) == 0 ? null : (redbObj?.parent_id ?? obj.ParentId),
                IdOwner = redbObj?.owner_id ?? obj.OwnerId,
                IdWhoChange = redbObj?.who_change_id ?? obj.WhoChangeId,
                Name = redbObj?.name ?? obj.Name,
                Hash = redbObj?.hash ?? obj.Hash,
                DateBegin = redbObj?.date_begin ?? obj.DateBegin,
                DateComplete = redbObj?.date_complete ?? obj.DateComplete,
                Key = redbObj?.key ?? obj.Key,
                ValueLong = redbObj?.value_long ?? obj.ValueLong,
                ValueString = redbObj?.value_string ?? obj.ValueString,
                ValueGuid = redbObj?.value_guid ?? obj.ValueGuid,
                ValueBool = redbObj?.value_bool ?? obj.ValueBool,
                ValueDouble = redbObj?.value_double ?? obj.ValueDouble,
                ValueNumeric = redbObj?.value_numeric ?? obj.ValueNumeric,
                ValueDatetime = redbObj?.value_datetime ?? obj.ValueDatetime,
                ValueBytes = redbObj?.value_bytes ?? obj.ValueBytes,
                Note = redbObj?.note ?? obj.Note
            };
            
            // Handle creation/modification dates based on configuration
            if (_configuration.AutoSetModifyDate)
            {
                if (record.Id == 0)
                {
                    record.DateCreate = DateTimeOffset.Now;
                }
                else
                {
                    // For existing objects - take the old value
                    var existingDate = redbObj?.date_create ?? obj.DateCreate;
                    // If the date was not set (MinValue), use the current one
                    record.DateCreate = existingDate == DateTimeOffset.MinValue ? DateTimeOffset.Now : existingDate;
                }
                record.DateModify = DateTimeOffset.Now;
            }
            else
            {
                var dateCreate = redbObj?.date_create ?? obj.DateCreate;
                // If the date was not set (MinValue), use the current one
                record.DateCreate = dateCreate == DateTimeOffset.MinValue ? DateTimeOffset.Now : dateCreate;
                
                var dateModify = redbObj?.date_modify ?? obj.DateModify;
                record.DateModify = dateModify == DateTimeOffset.MinValue ? DateTimeOffset.Now : dateModify;
            }
            
            return record;
        }

        // ===== PUBLIC BATCH SAVE METHODS =====

        /// <summary>
        /// 🚀 BATCH SAVE: Save multiple objects (new + updates)
        /// Uses _securityContext to get the user
        /// </summary>
        public async Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await SaveAsync(objects, effectiveUser);
        }

        /// <summary>
        /// 🚀 BATCH SAVE with explicit user
        /// Supports two strategy modes:
        /// - DeleteInsert: delete ALL values → BulkInsert/BulkUpdate of objects → BulkInsert of values
        /// - ChangeTracking: tree-based diff → two-phase EF SaveChanges
        /// </summary>
        public async Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, IRedbUser user)
        {
            var objList = objects.ToList();
            if (objList.Count == 0) return new List<long>();

            // === PHASE 1: VALIDATION AND PREPARATION (BATCH OPTIMIZATION) ===
            
            // 🚀 STEP 1: Hash recomputation (in-memory, no DB queries)
            // Supports: RedbObject<TProps> with Props, RedbObject<TProps> with Props=null, RedbObject (non-generic)
            foreach (var obj in objList)
            {
                var objType = obj.GetType();
                var isGeneric = objType.IsGenericType && 
                                objType.GetGenericTypeDefinition() == typeof(RedbObject<>);
                
                if (isGeneric)
                {
                    var propsProperty = objType.GetProperty("Props");
                    var propsValue = propsProperty?.GetValue(obj);
                    
                    if (propsValue != null)
                    {
                        // Generic with Props - hash from Props, will have N records in _values
                        var currentHash = RedbHash.ComputeFor(obj);
                        if (currentHash.HasValue)
                        {
                            obj.Hash = currentHash.Value;
                        }
                    }
                    else
                    {
                        // Generic with Props=null - hash from base fields, 0 records in _values
                        obj.Hash = RedbHash.ComputeForBaseFields(obj);
                    }
                }
                else
                {
                    // Non-generic RedbObject - hash from base fields, 0 records in _values
                    obj.Hash = RedbHash.ComputeForBaseFields(obj);
                }
            }
            
            // 🚀 STEP 2: BATCH existence check (1 query instead of N!)
            var existingIds = objList.Where(o => o.Id != 0).Select(o => o.Id).ToArray();
            var existingIdsInDb = existingIds.Any()
                ? await _context.QueryScalarListAsync<long>(Sql.ObjectStorage_SelectExistingIds(), existingIds)
                : [];
            var existingIdsSet = existingIdsInDb.ToHashSet();
            
            // Processing of missing objects
            foreach (var obj in objList.Where(o => o.Id != 0 && !existingIdsSet.Contains(o.Id)))
            {
                switch (_configuration.MissingObjectStrategy)
                {
                    case MissingObjectStrategy.AutoSwitchToInsert:
                        obj.Id = 0;
                        break;
                    case MissingObjectStrategy.ReturnNull:
                        throw new InvalidOperationException($"Object with id {obj.Id} not found. Strategy: ReturnNull is not supported.");
                    case MissingObjectStrategy.ThrowException:
                    default:
                        throw new InvalidOperationException($"Object with id {obj.Id} not found.");
                }
            }
            
            // 🚀 STEP 3: BATCH auto-detection of schemes (caching by type)
            var schemeCache = new Dictionary<Type, long>();
            long? objectSchemeId = null; // Cache for non-generic RedbObject scheme
            
            foreach (var obj in objList.Where(o => o.SchemeId == 0 && _configuration.AutoSyncSchemesOnSave))
            {
                var objType = obj.GetType();
                if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(RedbObject<>))
                {
                    var propsType = objType.GetGenericArguments()[0];
                    
                    // Check cache
                    if (schemeCache.TryGetValue(propsType, out var cachedSchemeId))
                    {
                        obj.SchemeId = cachedSchemeId;
                        continue;
                    }
                    
                    // Load/sync the scheme (select generic overload without parameters)
                    var getSchemeMethod = typeof(ISchemeSyncProvider)
                        .GetMethods()
                        .FirstOrDefault(m => m.Name == nameof(ISchemeSyncProvider.GetSchemeByTypeAsync) 
                                          && m.IsGenericMethod 
                                          && m.GetParameters().Length == 0)?
                        .MakeGenericMethod(propsType);
                    
                    if (getSchemeMethod != null)
                    {
                        var existingSchemeTask = (Task?)getSchemeMethod.Invoke(_schemeSync, null);
                        if (existingSchemeTask != null)
                        {
                            await existingSchemeTask.ConfigureAwait(false);
                            var existingScheme = existingSchemeTask.GetType().GetProperty("Result")?.GetValue(existingSchemeTask) as IRedbScheme;
                            
                            if (existingScheme != null)
                            {
                                obj.SchemeId = existingScheme.Id;
                                schemeCache[propsType] = existingScheme.Id;
                            }
                            else if (_configuration.AutoSyncSchemesOnSave)
                            {
                                var syncMethod = typeof(ISchemeSyncProvider)
                                    .GetMethods()
                                    .FirstOrDefault(m => m.Name == nameof(ISchemeSyncProvider.SyncSchemeAsync)
                                                      && m.IsGenericMethod
                                                      && m.GetParameters().Length == 0)?
                                    .MakeGenericMethod(propsType);
                                
                                if (syncMethod != null)
                                {
                                    var syncTask = (Task?)syncMethod.Invoke(_schemeSync, null);
                                    if (syncTask != null)
                                    {
                                        await syncTask.ConfigureAwait(false);
                                        var scheme = syncTask.GetType().GetProperty("Result")?.GetValue(syncTask) as IRedbScheme;
                                        if (scheme != null)
                                        {
                                            obj.SchemeId = scheme.Id;
                                            schemeCache[propsType] = scheme.Id;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                else if (objType == typeof(RedbObject))
                {
                    // Non-generic RedbObject - use Object scheme
                    if (objectSchemeId == null)
                    {
                        var objectScheme = await _schemeSync.EnsureObjectSchemeAsync("RedbObject");
                        objectSchemeId = objectScheme.Id;
                    }
                    obj.SchemeId = objectSchemeId.Value;
                }
            }

            // 🚀 STEP 4: BATCH permissions check
            if (_configuration.DefaultCheckPermissionsOnSave)
            {
                // Load unique schemes once
                var uniqueSchemeIds = objList.Select(o => o.SchemeId).Distinct().ToArray();
                var schemesList = await _context.QueryAsync<RedbScheme>(
                    Sql.ObjectStorage_SelectSchemesByIds(), uniqueSchemeIds);
                var schemesDict = schemesList.ToDictionary(s => s.Id, s => s);
                
                // Check INSERT permissions for new objects
                var newObjectsByScheme = objList.Where(o => o.Id == 0).GroupBy(o => o.SchemeId);
                foreach (var group in newObjectsByScheme)
                {
                    if (schemesDict.TryGetValue(group.Key, out var schemeContract))
                    {
                        var canInsert = await _permissionProvider.CanUserInsertScheme(schemeContract, user);
                        if (!canInsert)
                        {
                            throw new UnauthorizedAccessException($"User {user.Id} has no create permission for objects in scheme {group.Key}");
                        }
                    }
                }
                
                // Check UPDATE permissions for existing objects
                foreach (var obj in objList.Where(o => o.Id != 0))
                {
                    var canUpdate = await _permissionProvider.CanUserEditObject(obj, user);
                    if (!canUpdate)
                    {
                        throw new UnauthorizedAccessException($"User {user.Id} has no update permission for object {obj.Id}");
                    }
                }
            }

            // === PHASE 2: COLLECTION OF ALL OBJECTS (MAIN + NESTED) ===
            var allObjectsToSave = new List<IRedbObject>();
            var allValuesToSave = new List<RedbValue>();
            var processedObjectIds = new HashSet<long>();

            foreach (var obj in objList)
            {
                await CollectAllObjectsRecursively(obj, allObjectsToSave, processedObjectIds);
            }

            // === PHASE 3: BATCH PROCESSING (REUSING PROTECTED METHODS) ===
            await AssignMissingIds(allObjectsToSave, user);
            await EnsureSchemesForAllTypes(allObjectsToSave);
            await ProcessAllObjectsPropertiesRecursively(allObjectsToSave, allValuesToSave);

            // === PHASE 4: STRATEGY SELECTION AND TRANSACTIONAL SAVE ===
            var strategy = _configuration.EavSaveStrategy;
            await using var transaction = await _context.BeginTransactionAsync();

            try
            {
                // 🔒 FOR UPDATE: lock ALL existing objects to prevent race condition
                var existingObjectIds = allObjectsToSave.Where(o => o.Id > 0).Select(o => o.Id).ToArray();
                if (existingObjectIds.Any())
                {
                    // Lock all objects in a single query
                    await _context.ExecuteAsync(
                        Sql.ObjectStorage_LockObjectsForUpdate(), existingObjectIds);
                }
                
                await ExecuteBatchByStrategy(strategy, allObjectsToSave, allValuesToSave);
                
                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }

            // === PHASE 5: CACHE UPDATE ===
            if (_configuration.EnablePropsCache && PropsCache.Instance != null)
            {
                foreach (var savedObj in allObjectsToSave)
                {
                    if (savedObj.Hash.HasValue)
                    {
                        var objType = savedObj.GetType();
                        if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(RedbObject<>))
                        {
                            var propsType = objType.GetGenericArguments()[0];
                            var setMethod = typeof(GlobalPropsCache).GetMethod("Set")?.MakeGenericMethod(propsType);
                            setMethod?.Invoke(PropsCache, new[] { savedObj });
                        }
                    }
                }
            }

            return objList.Select(o => o.Id).ToList();
        }

        /// <summary>
        /// Executes batch save by strategy. OpenSource: only DeleteInsert.
        /// Pro: override to support ChangeTracking.
        /// </summary>
        protected virtual async Task ExecuteBatchByStrategy(
            EavSaveStrategy strategy,
            List<IRedbObject> allObjectsToSave,
            List<RedbValue> allValuesToSave)
        {
            if (strategy == EavSaveStrategy.ChangeTracking)
            {
                throw new RedbProRequiredException("ChangeTracking batch", ProFeatureCategory.ChangeTracking);
            }

            await SaveBatchWithDeleteInsertStrategy(allObjectsToSave, allValuesToSave);
        }

        // ===== DELETEINSERT STRATEGY =====

        /// <summary>
        /// DeleteInsert batch strategy: delete ALL values, BulkInsert/BulkUpdate of objects, BulkInsert of values.
        /// </summary>
        protected async Task SaveBatchWithDeleteInsertStrategy(
            List<IRedbObject> allObjectsToSave, 
            List<RedbValue> allValuesToSave)
        {
            // Step 1: Determine which objects ACTUALLY exist in the DB
            var allIds = allObjectsToSave.Select(o => o.Id).ToArray();
            var existingIdsInDb = allIds.Any()
                ? await _context.QueryScalarListAsync<long>(Sql.ObjectStorage_SelectExistingIds(), allIds)
                : [];
            var existingIdsSet = existingIdsInDb.ToHashSet();

            // Step 2: Delete all old values
            if (existingIdsInDb.Any())
            {
                await _context.Bulk.BulkDeleteValuesByObjectIdsAsync(existingIdsInDb);
            }

            // Step 3: Separation and BulkInsert of new objects
            var newObjects = allObjectsToSave.Where(o => !existingIdsSet.Contains(o.Id)).ToList();
            var existingObjects = allObjectsToSave.Where(o => existingIdsSet.Contains(o.Id)).ToList();

            if (newObjects.Any())
            {
                var newRecords = newObjects.Select(ConvertToObjectRecord).ToList();
                await _context.Bulk.BulkInsertObjectsAsync(newRecords);
            }

            // Step 4: BulkUpdate of existing objects
            if (existingObjects.Any())
            {
                var existingRecords = existingObjects.Select(ConvertToObjectRecord).ToList();
                await _context.Bulk.BulkUpdateObjectsAsync(existingRecords);
            }

            // Step 5: BulkInsert of all values
            if (allValuesToSave.Any())
            {
                var sortedValues = ValuesTopologicalSort.SortByFkDependency(allValuesToSave);
                await _context.Bulk.BulkInsertValuesAsync(sortedValues);
            }
        }

    }
}

