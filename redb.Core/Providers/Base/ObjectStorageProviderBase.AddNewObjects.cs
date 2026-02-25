using redb.Core.Providers;
using redb.Core.Data;
using redb.Core.Utils;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Configuration;
using redb.Core.Models.Security;
using redb.Core.Query;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// 🚀 BULK INSERT - high-performance creation of multiple objects
    /// </summary>
    public abstract partial class ObjectStorageProviderBase
    {
        /// <summary>
        /// 🚀 BULK INSERT: Create multiple new objects in one operation (WITHOUT permission checks)
        /// Reuses logic from SaveAsyncNew + BulkInsert for maximum performance
        /// </summary>
        public async Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects) where TProps : class, new()
        {
            return await AddNewObjectsAsync(objects, _securityContext.CurrentUser);
        }

        /// <summary>
        /// 🚀 BULK INSERT with explicit user: Create multiple new objects (WITHOUT permission checks)
        /// </summary>
        public async Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects, IRedbUser user) where TProps : class, new()
        {
            // Materialize IEnumerable to list for multiple iterations
            var objectsList = objects?.ToList() ?? new List<IRedbObject<TProps>>();
            if (objectsList.Count == 0)
            {
                return new List<long>();
            }




            // Validation
            foreach (var obj in objectsList)
            {
                if (obj.Props == null)
                {
                    throw new ArgumentException($"Object properties '{obj.Name}' cannot be null", nameof(objects));
                }
            }

            // Initial processing of hashes and schemes for main objects

            foreach (var obj in objectsList)
            {
                // Recalculate hash (from SaveAsyncNew)
                var currentHash = RedbHash.ComputeFor(obj);
                if (currentHash.HasValue)
                {
                    obj.Hash = currentHash.Value;
                }

                // Auto-determination of scheme (from SaveAsyncNew, but WITHOUT checks for existing objects)
                if (obj.SchemeId == 0 && _configuration.AutoSyncSchemesOnSave)
                {

                    var existingScheme = await _schemeSync.GetSchemeByTypeAsync<TProps>();
                    if (existingScheme != null)
                    {
                        obj.SchemeId = existingScheme.Id;

                    }
                    else if (_configuration.AutoSyncSchemesOnSave)
                    {

                        try
                        {
                            var scheme = await _schemeSync.SyncSchemeAsync<TProps>();
                            obj.SchemeId = scheme.Id;

                        }
                        catch (Exception ex)
                        {

                            throw;
                        }
                    }
                }
            }

            // === REUSE LOGIC FROM SaveAsyncNew ===

            var objectsToSave = new List<IRedbObject>();
            var valuesToSave = new List<RedbValue>();
            var processedObjectIds = new HashSet<long>();

            // STEP 2: Recursive collection of all objects (main + nested IRedbObject)

            foreach (var obj in objectsList)
            {
                await CollectAllObjectsRecursively(obj, objectsToSave, processedObjectIds);
            }


            // STEP 3: Assign IDs to all objects without ID (via GetNextKey)

            await AssignMissingIds(objectsToSave, user);
            
            // ✅ Set ParentId for nested objects after ID assignment
            var mainObjectIds = objectsList.Select(o => o.Id).ToHashSet();
            foreach (var obj in objectsToSave)
            {
                if (!mainObjectIds.Contains(obj.Id))
                {
                    // This is a nested object - set ParentId = main object ID
                    obj.ParentId = objectsList.First().Id;

                }
            }

            // STEP 4: Create/verify schemes for all object types

            await EnsureSchemesForAllTypes(objectsToSave);


            // STEP 5: Recursive processing of Props of all objects into values lists

            await ProcessAllObjectsPropertiesRecursively(objectsToSave, valuesToSave);

            
            // STEP 6: WITHOUT Delete strategy - these are NEW objects


            // STEP 7: BULK INSERT instead of regular saving

            await CommitAllChangesBulk(objectsToSave, valuesToSave);


            // Return IDs of all created main objects
            var resultIds = objectsList.Select(o => o.Id).ToList();

            
            return resultIds;
        }

        /// <summary>
        /// Step 7 (BULK): Bulk save with BulkInsert instead of Add().
        /// </summary>
        private async Task CommitAllChangesBulk(List<IRedbObject> objects, List<RedbValue> valuesList)
        {
            // 1. BULK INSERT objects
            if (objects.Count > 0)
            {
                var objectRecords = objects.Select(obj =>
                {
                    var record = new RedbObjectRow
                    {
                        Id = obj.Id,
                        IdScheme = obj.SchemeId,
                        IdParent = obj.ParentId,
                        IdOwner = obj.OwnerId,
                        IdWhoChange = obj.WhoChangeId,
                        Name = obj.Name,
                        Hash = obj.Hash,
                        DateBegin = obj.DateBegin,
                        DateComplete = obj.DateComplete,
                        Key = obj.Key,
                        ValueLong = obj.ValueLong,
                        ValueString = obj.ValueString,
                        ValueGuid = obj.ValueGuid,
                        ValueBool = obj.ValueBool,
                        ValueDouble = obj.ValueDouble,
                        ValueNumeric = obj.ValueNumeric,
                        ValueDatetime = obj.ValueDatetime,
                        ValueBytes = obj.ValueBytes,
                        Note = obj.Note
                    };

                    if (_configuration.AutoSetModifyDate)
                    {
                        record.DateCreate = DateTimeOffset.Now;
                        record.DateModify = DateTimeOffset.Now;
                    }
                    else
                    {
                        var dateCreate = obj.DateCreate;
                        record.DateCreate = dateCreate == DateTimeOffset.MinValue ? DateTimeOffset.Now : dateCreate;

                        var dateModify = obj.DateModify;
                        record.DateModify = dateModify == DateTimeOffset.MinValue ? DateTimeOffset.Now : dateModify;
                    }

                    return record;
                }).ToList();

                await _context.Bulk.BulkInsertObjectsAsync(objectRecords);
            }

            // 2. BULK INSERT values (with topological sort for FK constraint)
            if (valuesList.Count > 0)
            {
                var sortedValues = ValuesTopologicalSort.SortByFkDependency(valuesList);
                await _context.Bulk.BulkInsertValuesAsync(sortedValues);
            }
        }
    }
}
