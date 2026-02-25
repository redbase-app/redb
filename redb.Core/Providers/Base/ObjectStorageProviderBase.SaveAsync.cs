using redb.Core.Providers;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Utils;
using redb.Core.Extensions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Configuration;
using redb.Core.Models.Security;
using redb.Core.Caching;
using redb.Core.Query;
using Microsoft.Extensions.Logging;

using System.Text.Json.Serialization;

using System.Reflection;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Threading.Tasks;
using System;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// 🚀 NEW SaveAsync - correct architecture with recursive processing
    /// </summary>
    public abstract partial class ObjectStorageProviderBase
    {
        /// <summary>
        /// 🚀 NEW SAVEASYNC: Correct recursive processing of all data types
        /// Collects objects and values into lists, then batch save
        /// </summary>
        public async Task<long> SaveAsyncNew<TProps>(IRedbObject<TProps> obj, IRedbUser user) where TProps : class, new()
        {
            System.Console.Out.Flush();
            
            if (obj.Props == null)
            {
                throw new ArgumentException("Object properties cannot be null", nameof(obj));
            }

            // === HASH RECOMPUTATION AT START (from old SaveAsync) ===
            var currentHash = RedbHash.ComputeFor(obj);
            if (currentHash.HasValue)
            {
                obj.Hash = currentHash.Value;
            }

            // Strategy for missing objects
            var isNewObject = obj.Id == 0;
            if (!isNewObject)
            {
                var exists = await _context.ExecuteScalarAsync<long?>(
                    Sql.ObjectStorage_CheckObjectExists(), obj.Id);
                if (exists == null)
                {
                    switch (_configuration.MissingObjectStrategy)
                    {
                        case MissingObjectStrategy.AutoSwitchToInsert:
                            isNewObject = true;
                            break;
                        case MissingObjectStrategy.ReturnNull:
                            return 0;
                        case MissingObjectStrategy.ThrowException:
                        default:
                            throw new InvalidOperationException($"Object with id {obj.Id} not found. Current strategy: {_configuration.MissingObjectStrategy}");
                    }
                }
            }

            // === SCHEMA AUTO-DETECTION (from old SaveAsync) ===
            // Scheme check: SchemeId={obj.SchemeId}, AutoSync={_configuration.AutoSyncSchemesOnSave}
            if (obj.SchemeId == 0 && _configuration.AutoSyncSchemesOnSave)
            {

                
                // 🚧 TEMPORARY: check existing scheme WITHOUT creating structures
                var existingScheme = await _schemeSync.GetSchemeByTypeAsync<TProps>();
                if (existingScheme != null)
                {
                    obj.SchemeId = existingScheme.Id;

                }
                else
                {
                    var scheme = await _schemeSync.SyncSchemeAsync<TProps>();
                    obj.SchemeId = scheme.Id;
                }
            }

            // Permission checks
            if (_configuration.DefaultCheckPermissionsOnSave)
            {
                if (isNewObject)
                {
                    var scheme = await _context.QueryFirstOrDefaultAsync<RedbScheme>(
                        Sql.ObjectStorage_SelectSchemeById(), obj.SchemeId);
                    if (scheme != null)
                    {
                        var canInsert = await _permissionProvider.CanUserInsertScheme(scheme, user);
                        if (!canInsert)
                        {
                            throw new UnauthorizedAccessException($"User {user.Id} has no permission to create objects in scheme {obj.SchemeId}");
                        }
                    }
                }
                else
                {
                    var canUpdate = await _permissionProvider.CanUserEditObject(obj, user);
                    if (!canUpdate)
                    {
                        throw new UnauthorizedAccessException($"User {user.Id} has no permission to edit object {obj.Id}");
                    }
                }
            }

            // STEP 1: Creating collectors for objects and values
            var objectsToSave = new List<IRedbObject>();
            var valuesToSave = new List<RedbValue>();
            var processedObjectIds = new HashSet<long>();

            // STEP 2: Recursive collection of all objects (main + nested IRedbObject)

            await CollectAllObjectsRecursively(obj, objectsToSave, processedObjectIds);

            // 🔥 FIX: Remember objects that already have Id (references to existing objects)
            // These are REFERENCES, not nested children - don't change their ParentId!
            var preExistingObjectIds = objectsToSave.Where(o => o.Id != 0).Select(o => o.Id).ToHashSet();

            // STEP 3: Assigning IDs to all objects without ID (via GetNextKey)

            await AssignMissingIds(objectsToSave, user);
            
            // ✅ FIX ParentId after ID assignment - only for TRULY NEW nested objects
            var mainObjectId = obj.Id;
            foreach (var nestedObj in objectsToSave.Skip(1)) // skip main object
            {
                // Skip objects that already existed (references) - don't change their ParentId
                if (preExistingObjectIds.Contains(nestedObj.Id))
                    continue;
                    
                if (nestedObj.ParentId == null || nestedObj.ParentId == 0)
                {
                    nestedObj.ParentId = mainObjectId;
                }
            }
            


            // STEP 4: Creation/verification of schemes for all object types

            await EnsureSchemesForAllTypes(objectsToSave);


            // STEP 5: Recursive processing of Props for all objects into values lists

            await ProcessAllObjectsPropertiesRecursively(objectsToSave, valuesToSave);

            
            // 🔍 DIAGNOSTICS: Check for duplicates in valuesToSave 
            var duplicates = valuesToSave
                .Where(v => v.ArrayIndex == null) // only non-array elements
                .GroupBy(v => new { v.IdStructure, v.IdObject })
                .Where(g => g.Count() > 1)
                .ToList();
                
            // if (duplicates.Any())
            // {

            //     foreach (var duplicate in duplicates)
            //     {

            //     }
            // }
            // else
            // {

            // }

            // STEP 6: Delete/Insert strategy - remove old values, prepare new ones

            await PrepareValuesByStrategy(objectsToSave, valuesToSave, isNewObject);


            // STEP 7: Batch save in correct order
            await CommitAllChangesBatch(objectsToSave, valuesToSave);

            // STEP 8: CACHE UPDATE for all saved objects
            if (_configuration.EnablePropsCache && PropsCache.Instance != null)
            {
                foreach (var savedObj in objectsToSave)
                {
                    if (savedObj.Hash.HasValue)
                    {
                        // ✅ Use new Set method which saves ENTIRE RedbObject
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

            return obj.Id;
        }

        /// <summary>
        /// STEP 2: Recursive collection of all IRedbObject (main + nested).
        /// Uses <paramref name="processed"/> to deduplicate objects that are referenced
        /// multiple times in the object tree (e.g. two licenses referencing the same Plan).
        /// </summary>
        protected async Task CollectAllObjectsRecursively(IRedbObject rootObject, List<IRedbObject> collector, HashSet<long> processed)
        {
            collector.Add(rootObject);

            // Track root object ID to prevent re-adding if referenced elsewhere in the tree.
            // New objects (Id == 0) get IDs assigned later and are not subject to dedup.
            if (rootObject.Id != 0)
                processed.Add(rootObject.Id);

            var rootProperties = GetPropertiesFromRedbObject(rootObject);
            await CollectNestedRedbObjectsFromProperties(rootProperties, collector, processed, rootObject.Id);
        }

        /// <summary>
        /// 🔍 Recursive search for IRedbObject in object Props
        /// </summary>
        private async Task CollectNestedRedbObjectsFromProperties(object? properties, List<IRedbObject> collector, HashSet<long> processed, long parentId)
        {
            if (properties == null) return;

            var propsType = properties.GetType();
            var propsProperties = propsType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            
            foreach (var property in propsProperties)
            {
                // ⚠️ Skip technical properties with [JsonIgnore] or [RedbIgnore]
                if (property.ShouldIgnoreForRedb())
                    continue;
                
                // Skip indexers (e.g. Dictionary<K,V>.Item[key]) - they require index parameters
                if (property.GetIndexParameters().Length > 0) continue;
                    
                var value = property.GetValue(properties);
                if (value == null) continue;

                // Single IRedbObject (reference or nested child)
                if (IsRedbObjectType(value.GetType()))
                {
                    var redbObj = (IRedbObject)value;

                    // Dedup: skip objects already collected (same RedbObject referenced
                    // from multiple properties, e.g. two LicenseInfo entries pointing
                    // to the same Plan). New objects (Id == 0) always collected.
                    if (redbObj.Id != 0 && !processed.Add(redbObj.Id))
                        continue;

                    collector.Add(redbObj);

                    var nestedProperties = GetPropertiesFromRedbObject(redbObj);
                    await CollectNestedRedbObjectsFromProperties(nestedProperties, collector, processed, redbObj.Id);
                }
                // IRedbObject array
                else if (value is IEnumerable enumerable && IsRedbObjectArrayType(value.GetType()))
                {
                    foreach (var item in enumerable)
                    {
                        if (item != null && IsRedbObjectType(item.GetType()))
                        {
                            var redbObj = (IRedbObject)item;

                            // Dedup: same object referenced from multiple array elements
                            if (redbObj.Id != 0 && !processed.Add(redbObj.Id))
                                continue;

                            collector.Add(redbObj);

                            var arrayElementProperties = GetPropertiesFromRedbObject(redbObj);
                            await CollectNestedRedbObjectsFromProperties(arrayElementProperties, collector, processed, redbObj.Id);
                        }
                    }
                }
                // Dictionary with IRedbObject values
                else if (IsDictionaryWithRedbObjectValue(value.GetType()))
                {
                    var valuesProperty = value.GetType().GetProperty("Values");
                    if (valuesProperty != null)
                    {
                        var dictValues = (System.Collections.IEnumerable)valuesProperty.GetValue(value)!;
                        foreach (var item in dictValues)
                        {
                            if (item != null && IsRedbObjectType(item.GetType()))
                            {
                                var redbObj = (IRedbObject)item;

                                // Dedup: same object referenced from multiple dict entries
                                if (redbObj.Id != 0 && !processed.Add(redbObj.Id))
                                    continue;

                                collector.Add(redbObj);

                                var nestedProperties = GetPropertiesFromRedbObject(redbObj);
                                await CollectNestedRedbObjectsFromProperties(nestedProperties, collector, processed, redbObj.Id);
                            }
                        }
                    }
                }
                // 🏗️ Recursion into business classes
                else if (IsBusinessClassType(value.GetType()))
                {
                    await CollectNestedRedbObjectsFromProperties(value, collector, processed, parentId);
                }
                // 📊 Recursion into business class arrays
                else if (value is IEnumerable businessEnumerable && !IsStringType(value.GetType()))
                {
                    foreach (var item in businessEnumerable)
                    {
                        if (item != null && IsBusinessClassType(item.GetType()))
                        {
                            await CollectNestedRedbObjectsFromProperties(item, collector, processed, parentId);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 🔍 IRedbObject type check
        /// </summary>
        private static bool IsRedbObjectType(Type type)
        {
            // Direct check for RedbObject<T>
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(RedbObject<>))
                return true;
            
            // Check interfaces for IRedbObject<T>
            return type.GetInterfaces().Any(i => 
                i.IsGenericType && 
                i.GetGenericTypeDefinition().Name.Contains("IRedbObject"));
        }

        /// <summary>
        /// 🔍 Checking IRedbObject array
        /// </summary>
        private static bool IsRedbObjectArrayType(Type type)
        {
            if (!type.IsArray) return false;
            return IsRedbObjectType(type.GetElementType()!);
        }

        /// <summary>
        /// 🔍 Checking Dictionary with IRedbObject values
        /// </summary>
        private static bool IsDictionaryWithRedbObjectValue(Type type)
        {
            if (!type.IsGenericType) return false;
            var genericDef = type.GetGenericTypeDefinition();
            if (genericDef != typeof(Dictionary<,>) && genericDef != typeof(IDictionary<,>)) return false;
            
            var valueType = type.GetGenericArguments()[1];
            return IsRedbObjectType(valueType);
        }

        /// <summary>
        /// 🔍 Checking string type
        /// </summary>  
        private static bool IsStringType(Type type)
        {
            return type == typeof(string);
        }

        /// <summary>
        /// 🔧 Getting Props from IRedbObject via reflection
        /// </summary>
        private static object? GetPropertiesFromRedbObject(IRedbObject redbObj)
        {
            // Use reflection to get the Props property
            var propertiesProperty = redbObj.GetType().GetProperty("Props");
            return propertiesProperty?.GetValue(redbObj);
        }
        
        /// <summary>
        /// 🔧 Getting Props type from IRedbObject
        /// </summary>
        private static Type? GetPropertiesTypeFromRedbObject(IRedbObject redbObj)
        {
            // Get TProps from IRedbObject<TProps>
            var objType = redbObj.GetType();
            if (objType.IsGenericType)
            {
                return objType.GetGenericArguments()[0]; // TProps
            }
            return null;
        }

        /// <summary>
        /// Load all types into GlobalMetadataCache (1 query instead of N!).
        /// </summary>
        private async Task EnsureTypesCacheLoaded()
        {
            if (Cache.HasTypesByIdCache) return;
            
            var allTypes = await _context.QueryAsync<RedbTypeInfo>(
                Sql.ObjectStorage_SelectAllTypes());
            
            Cache.CacheTypesById(allTypes);
        }

        /// <summary>
        /// 🔍 Checking if structure is Class type (business class)
        /// </summary>
        private async Task<bool> IsClassTypeStructure(IRedbStructure structure)
        {
            await EnsureTypesCacheLoaded();
            var type = Cache.GetTypeById(structure.IdType);
            return type?.Type1 == "Object" || type?.Name == "Class";
        }

        /// <summary>
        /// 🔍 Checking if structure is IRedbObject reference
        /// </summary>
        private async Task<bool> IsRedbObjectStructure(IRedbStructure structure)
        {
            await EnsureTypesCacheLoaded();
            var type = Cache.GetTypeById(structure.IdType);
            return type?.Type1 == "RedbObjectRow" || type?.Name == "Object";
        }
        
        /// <summary>
        /// 🔍 Checking if structure is ListItem reference
        /// </summary>
        private async Task<bool> IsListItemStructure(IRedbStructure structure)
        {
            await EnsureTypesCacheLoaded();
            var type = Cache.GetTypeById(structure.IdType);
            return type?.Type1 == "RedbListItem" || type?.Name == "ListItem";
        }

        /// <summary>
        /// Gets DbType for structure from cache.
        /// </summary>
        protected async Task<string> GetStructureDbType(IRedbStructure structure)
        {
            await EnsureTypesCacheLoaded();
            var type = Cache.GetTypeById(structure.IdType);
            
            return type?.DbType ?? "String";
        }

        /// <summary>
        /// 🎯 STEP 3: Assigning ID via GetNextKey() to all objects without ID
        /// </summary>
        protected async Task AssignMissingIds(List<IRedbObject> objects, IRedbUser user)
        {
            // 🚀 BATCH: Get all needed IDs in one DB call instead of N calls
            var objectsNeedingIds = objects.Where(o => o.Id == 0).ToList();
            if (objectsNeedingIds.Count > 0)
            {
                var newIds = await _context.Keys.NextObjectIdBatchAsync(objectsNeedingIds.Count);
                for (int i = 0; i < objectsNeedingIds.Count; i++)
                {
                    var obj = objectsNeedingIds[i];
                    obj.Id = newIds[i];
                    
                    // Apply audit settings
                    obj.OwnerId = user.Id;
                    obj.WhoChangeId = user.Id;
                    
                    if (_configuration.AutoSetModifyDate)
                    {
                        obj.DateCreate = DateTimeOffset.Now;
                        obj.DateModify = DateTimeOffset.Now;
                    }
                    
                    if (_configuration.AutoRecomputeHash)
                    {
                        obj.Hash = RedbHash.ComputeFor(obj);
                    }
                }
            }
            // Objects with existing IDs - no changes needed
        }

        /// <summary>
        /// 🏗️ STEP 4: Creating/verifying schemas for all object types (using PostgresSchemeSyncProvider)
        /// </summary>
        protected async Task EnsureSchemesForAllTypes(List<IRedbObject> objects)
        {

            
            foreach (var obj in objects)
            {

                
                if (obj.SchemeId == 0 && _configuration.AutoSyncSchemesOnSave)
                {

                    
                    // Get object Props type via reflection
                    var objType = obj.GetType();
                    if (objType.IsGenericType)
                    {
                        var propsType = objType.GetGenericArguments()[0]; // TProps from IRedbObject<TProps>

                        
                        // Looking for existing schema

                        var existingScheme = await _schemeSync.GetSchemeByTypeAsync(propsType);
                        if (existingScheme != null)
                        {
                            obj.SchemeId = existingScheme.Id;
                        }
                        else
                        {
                            throw new InvalidOperationException(
                                $"Scheme not found for type '{propsType.FullName}'. Register scheme first or enable AutoCreateSchemes.");
                        }
                    }
                }
            }

        }

        /// <summary>
        /// 🔄 STEP 5: Recursive processing of Props of all objects → RedbValue lists
        /// ✅ NEW ARCHITECTURE: Uses a structure tree instead of a flat list!
        /// ✅ Skips non-generic RedbObject and RedbObject{TProps} with Props=null (0 records in _values)
        /// </summary>
        protected async Task ProcessAllObjectsPropertiesRecursively(List<IRedbObject> objects, List<RedbValue> valuesList)
        {
            foreach (var obj in objects)
            {
                // Skip non-generic RedbObject (Object scheme) - no _values records
                var objType = obj.GetType();
                if (objType == typeof(RedbObject))
                {
                    continue;
                }
                
                // Skip RedbObject<TProps> with Props=null - no _values records
                if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(RedbObject<>))
                {
                    var propsProperty = objType.GetProperty("Props");
                    var propsValue = propsProperty?.GetValue(obj);
                    if (propsValue == null)
                    {
                        continue;
                    }
                }
                
                // Check that the schema exists (🚀 from cache without hash validation)
                var scheme = await GetSchemeFromCacheOrDbAsync(obj.SchemeId);
                if (scheme == null)
                {
                    throw new InvalidOperationException(
                        $"Scheme with Id={obj.SchemeId} not found for object Id={obj.Id}.");
                }
                
                // ✅ NEW LOGIC: Get the structure tree instead of a flat list
                var schemeProvider = (SchemeSyncProviderBase)_schemeSync;
                var rootStructureTree = await schemeProvider.GetSubtreeAsync(obj.SchemeId, null); // root nodes


                if (rootStructureTree.Count == 0)
                {

                    try
                    {
                        // Create structures via a universal method
                        var propsType = obj.GetType().GetGenericArguments()[0];
                        await SyncStructuresForType(scheme, propsType);
                        
                        // Get the structure tree again
                        schemeProvider.InvalidateStructureTreeCache(obj.SchemeId); // clear cache
                        rootStructureTree = await schemeProvider.GetSubtreeAsync(obj.SchemeId, null);

                    }
                    catch
                    {
                        throw;
                    }
                }
                
                // ✅ NEW TRAVERSAL: Via structure tree with subtrees!
                var valuesCountBefore = valuesList.Count;
                await ProcessPropertiesWithTreeStructures(obj, rootStructureTree, valuesList, objects);
                var valuesGenerated = valuesList.Count - valuesCountBefore;
            }
        }

        /// <summary>
        /// 🌳 NEW METHOD: Props processing via structure tree
        /// Solves redundant structures issues and correct subtree passing
        /// </summary>
        internal async Task ProcessPropertiesWithTreeStructures(IRedbObject obj, List<StructureTreeNode> structureNodes, List<RedbValue> valuesList, List<IRedbObject> objectsToSave)
        {
            var objPropertiesType = GetPropertiesTypeFromRedbObject(obj);
            
            foreach (var structureNode in structureNodes)
            {
                var dbTypeForLog = await GetStructureDbType(structureNode.Structure);
                var property = objPropertiesType?.GetProperty(structureNode.Structure.Name);
                if (property == null || property.GetIndexParameters().Length > 0)
                {
                    continue; // ✅ SOLVES THE REDUNDANT STRUCTURES PROBLEM!
                }
                
                // Get the property value
                var objProperties = GetPropertiesFromRedbObject(obj);
                var rawValue = property.GetValue(objProperties);
                
                // ✅ NULL SEMANTICS
                if (!ObjectStorageProviderExtensions.ShouldCreateValueRecord(rawValue, structureNode.Structure.StoreNull ?? false))
                {
                    continue;
                }
                
                // ✅ CRITICAL DISPATCH BY TYPES 
                if (structureNode.Structure.CollectionType == Core.Utils.RedbTypeIds.Dictionary)
                {
                    await ProcessDictionaryWithSubtree(obj, structureNode, rawValue, valuesList, objectsToSave);
                }
                else if (structureNode.Structure.CollectionType == Core.Utils.RedbTypeIds.Array)
                {
                    var arrayDbType = await GetStructureDbType(structureNode.Structure);
                    await ProcessArrayWithSubtree(obj, structureNode, rawValue, valuesList, objectsToSave, null);
                }
                else if (await IsClassTypeStructure(structureNode.Structure))
                {
                    await ProcessBusinessClassWithSubtree(obj, structureNode, rawValue, valuesList, objectsToSave, null); // root class - no parent
                }
                else if (await IsRedbObjectStructure(structureNode.Structure))
                {
                    await ProcessIRedbObjectField(obj, structureNode.Structure, rawValue, objectsToSave, valuesList);
                }
                else
                {
                    var dbTypeForLog2 = await GetStructureDbType(structureNode.Structure);
                    await ProcessSimpleFieldWithTree(obj, structureNode, rawValue, valuesList);
                }
            }
        }

        /// <summary>
        /// 🚀 Recursive processing of Props of a single object (following SavePropertiesFromObjectAsync pattern)
        /// </summary>
        private async Task ProcessPropertiesRecursively<TProps>(IRedbObject<TProps> obj, List<StructureMetadata> structures, List<RedbValue> valuesList) where TProps : class
        {
            var propertiesType = typeof(TProps);
            var rootStructures = structures.Where(s => s.IdParent == null).ToList();


            foreach (var structure in rootStructures)
            {
                var property = propertiesType.GetProperty(structure.Name);
                if (property == null || property.GetIndexParameters().Length > 0) 
                    continue;

                if (property.ShouldIgnoreForRedb())
                    continue;

                var rawValue = property.GetValue(obj.Props);

                if (!ObjectStorageProviderExtensions.ShouldCreateValueRecord(rawValue, structure.StoreNull))
                    continue;

                // Dispatch by structure type
                var task = structure switch
                {
                    { IsDictionary: true } => ProcessDictionaryFieldForCollection(obj.Id, structure, rawValue, valuesList, obj.SchemeId),
                    { IsArray: true } => ProcessArrayFieldForCollection(obj.Id, structure, rawValue, valuesList, obj.SchemeId),
                    _ when ObjectStorageProviderExtensions.IsClassType(structure.TypeSemantic) 
                        => ProcessClassFieldForCollection(obj.Id, structure, rawValue, valuesList),
                    _ => ProcessSimpleFieldForCollection(obj.Id, structure, rawValue, valuesList)
                };
                await task;
            }
        }

        /// <summary>
        /// 🔧 Processing of a simple field for a collection (similar to SaveSimpleFieldAsync)
        /// </summary>
        private async Task ProcessSimpleFieldForCollection(long objectId, StructureMetadata structure, object? rawValue, List<RedbValue> valuesList)
        {
            // ✅ Process nested objects (IRedbObject, IRedbListItem) before saving
            var processedValue = await ProcessNestedObjectsAsync(rawValue, structure.DbType, false, objectId);
            
            var valueRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = objectId,
                IdStructure = structure.Id
            };
            
            // Set value by type
            SetSimpleValueByType(valueRecord, structure.DbType, processedValue);
            valuesList.Add(valueRecord);
        }

        /// <summary>
        /// 📖 Process Dictionary field for collection mode
        /// </summary>
        private async Task ProcessDictionaryFieldForCollection(long objectId, StructureMetadata structure, object? rawValue, List<RedbValue> valuesList, long schemeId)
        {
            if (rawValue == null) return;
            
            var dictType = rawValue.GetType();
            if (!dictType.IsGenericType) return;
            
            var genericDef = dictType.GetGenericTypeDefinition();
            if (genericDef != typeof(Dictionary<,>) && genericDef != typeof(IDictionary<,>)) return;
            
            var keyType = dictType.GetGenericArguments()[0];
            
            // Create BASE record for Dictionary with hash
            var dictHash = RedbHash.ComputeForProps(rawValue);
            var baseDictRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = objectId,
                IdStructure = structure.Id,
                Guid = dictHash
            };
            valuesList.Add(baseDictRecord);
            
            // Iterate dictionary entries
            var enumerator = ((System.Collections.IEnumerable)rawValue).GetEnumerator();
            while (enumerator.MoveNext())
            {
                var kvp = enumerator.Current;
                if (kvp == null) continue;
                
                var kvpType = kvp.GetType();
                var key = kvpType.GetProperty("Key")!.GetValue(kvp);
                var value = kvpType.GetProperty("Value")!.GetValue(kvp);
                
                if (key == null) continue;
                
                var serializedKey = RedbKeySerializer.SerializeObject(key, keyType);
                
                var elementRecord = new RedbValue
                {
                    Id = await _context.Keys.NextObjectIdAsync(),
                    IdObject = objectId,
                    IdStructure = structure.Id,
                    ArrayParentId = baseDictRecord.Id,
                    ArrayIndex = serializedKey
                };
                
                if (value == null)
                {
                    valuesList.Add(elementRecord);
                    continue;
                }
                
                if (ObjectStorageProviderExtensions.IsClassType(structure.TypeSemantic))
                {
                    elementRecord.Guid = RedbHash.ComputeForProps(value);
                    valuesList.Add(elementRecord);
                    
                    // Save child fields via SaveClassChildrenAsync from ObjectStorageProviderBaseMethods
                    await SaveClassChildrenAsync(objectId, elementRecord.Id, value, structure.Id, schemeId);
                }
                else
                {
                    var processedValue = await ProcessNestedObjectsAsync(value, structure.DbType, false, objectId);
                    SetSimpleValueByType(elementRecord, structure.DbType, processedValue);
                    valuesList.Add(elementRecord);
                }
            }
        }
        
        /// <summary>
        /// 📊 Processing of an array for a collection (similar to SaveArrayFieldAsync) 
        /// </summary>
        private async Task ProcessArrayFieldForCollection(long objectId, StructureMetadata structure, object? rawValue, List<RedbValue> valuesList, long schemeId = 9001)
        {
            
            if (rawValue == null)
                return;

            if (rawValue is not IEnumerable enumerable || rawValue is string)
            {
                throw new InvalidOperationException(
                    $"Array field '{structure.Name}' expected IEnumerable but got '{rawValue.GetType().FullName}'.");
            }


            // ✅ Create BASE array record with hash of the entire array (as in SaveArrayFieldAsync)
            var arrayHash = RedbHash.ComputeForProps(rawValue);
            var baseArrayRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = objectId,
                IdStructure = structure.Id,
                Guid = arrayHash  // ✅ Hash of the entire array in _Guid
            };
            valuesList.Add(baseArrayRecord);
            

            // ✅ Processing of array elements with _array_parent_id and _array_index
            await ProcessArrayElementsForCollection(objectId, structure.Id, baseArrayRecord.Id, enumerable, valuesList, structure, schemeId);
        }

        /// <summary>
        /// 🔢 Processing of array elements with correct _array_parent_id and _array_index
        /// </summary>
        private async Task ProcessArrayElementsForCollection(long objectId, long structureId, long parentValueId, IEnumerable enumerable, List<RedbValue> valuesList, StructureMetadata structure, long schemeId)
        {
            
            int index = 0;
            foreach (var item in enumerable)
            {

                var elementRecord = new RedbValue
                {
                    Id = await _context.Keys.NextObjectIdAsync(),
                    IdObject = objectId,
                    IdStructure = structureId,
                    ArrayParentId = parentValueId,  // ✅ Link to the base array record
                    ArrayIndex = index.ToString()   // ✅ Position in the array (string)
                };

                if (item != null)
                {
                    var itemType = item.GetType();
                    
                    // ♻️ RECURSION IN ARRAYS: different types of elements
                    if (ObjectStorageProviderExtensions.IsRedbObjectReference(structure.TypeSemantic))
                    {
                        
                        // 🔗 IRedbObject array element
                        var redbObj = (IRedbObject)item;
                        elementRecord.Object = redbObj.Id;
                        elementRecord.Guid = RedbHash.ComputeFor(redbObj);  // 🔥 HASH for ChangeTracking!
                        valuesList.Add(elementRecord);
                    }
                    else if (structure.TypeSemantic == "RedbListItem")
                    {
                        
                        // 📋 ListItem array element - take ID directly (similarly to IRedbObject)
                        var listItem = (IRedbListItem)item;
                        var listItemId = listItem.Id;
                        
                        elementRecord.ListItem = listItemId;  // ⭐ Write to the ListItem field
                        elementRecord.Guid = RedbHash.ComputeForProps(item);  // 🔥 HASH for ChangeTracking!
                        valuesList.Add(elementRecord);
                        
                    }
                    else if (IsBusinessClassType(itemType))
                    {
                        
                        // Compute business class hash and save it in the array element
                        var itemHash = RedbHash.ComputeForProps(item);
                        elementRecord.Guid = itemHash;
                        valuesList.Add(elementRecord);

                        // ♻️ RECURSION: processing child fields of the business class from the array
                        // ✅ PASS ArrayIndex for child fields of array elements  
                        await ProcessClassChildrenForCollection(objectId, elementRecord.Id, item, structureId, valuesList, schemeId, index);
                    }
                    else
                    {
                        
                        // Simple array element
                        // ⚠️ CRITICAL: Calling ProcessNestedObjectsAsync to extract ID from IRedbListItem!
                        var processedValue = await ProcessNestedObjectsAsync(item, structure.DbType, false, objectId);
                        
                        
                        SetSimpleValueByType(elementRecord, structure.DbType, processedValue);
                        
                        
                        valuesList.Add(elementRecord);
                    }
                }
                else
                {
                    valuesList.Add(elementRecord); // null element
                }

                index++;
            }
            
        }

        /// <summary>
        /// 🏗️ Processing of a business class for a collection (similar to SaveClassFieldAsync)
        /// </summary>
        private async Task ProcessClassFieldForCollection(long objectId, StructureMetadata structure, object? rawValue, List<RedbValue> valuesList)
        {
            if (rawValue == null) return;



            // ✅ Compute UUID hash of the business class (as in SaveClassFieldAsync)
            var classHash = RedbHash.ComputeForProps(rawValue);

            // Create base Class field record with hash in _Guid
            var classRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = objectId,
                IdStructure = structure.Id,
                Guid = classHash  // ✅ UUID hash in _Guid field
            };
            valuesList.Add(classRecord);


            // ✅ Process child fields of the Class object via recursion  
            // TODO: Need correct schemeId - using 9001 for testing for now
            await ProcessClassChildrenForCollection(objectId, classRecord.Id, rawValue, structure.Id, valuesList, 9001);
        }

        /// <summary>
        /// 👶 Recursive processing of child fields of a business class 
        /// </summary>
        private async Task ProcessClassChildrenForCollection(long objectId, long parentValueId, object businessObject, long parentStructureId, List<RedbValue> valuesList, long schemeId, int? parentArrayIndex = null)
        {


            // Get the schema and look for child structures (🚀 from cache without hash validation)
            var scheme = await GetSchemeFromCacheOrDbAsync(schemeId);
            if (scheme == null) return;

            // Look for child structures with _id_parent = parentStructureId
            var childStructuresRaw = scheme.Structures
                .Where(s => s.IdParent == parentStructureId)
                .ToList();



            // Convert to StructureMetadata to get DbType
            var childStructures = await ConvertStructuresToMetadataAsync(childStructuresRaw);

            var businessType = businessObject.GetType();
            foreach (var childStructure in childStructures)
            {
                var property = businessType.GetProperty(childStructure.Name);
                if (property == null || property.GetIndexParameters().Length > 0) 
                {

                    continue;
                }

                var childValue = property.GetValue(businessObject);


                // ✅ NEW NULL SEMANTICS: check _store_null
                if (!ObjectStorageProviderExtensions.ShouldCreateValueRecord(childValue, childStructure.StoreNull))
                {

                    continue;
                }

                // ♻️ ♻️ FULL RECURSION: processing different types of child fields ♻️ ♻️
                if (childStructure.IsArray)
                {

                    await ProcessArrayFieldForCollection(objectId, childStructure, childValue, valuesList);
                }
                else if (ObjectStorageProviderExtensions.IsClassType(childStructure.TypeSemantic))
                {

                    await ProcessClassFieldForCollection(objectId, childStructure, childValue, valuesList);
                }
                else if (ObjectStorageProviderExtensions.IsRedbObjectReference(childStructure.TypeSemantic))
                {
                    // IRedbObject reference field in business class
                    var childRecord = new RedbValue
                    {
                        Id = await _context.Keys.NextObjectIdAsync(),
                        IdObject = objectId,
                        IdStructure = childStructure.Id,
                        ArrayParentId = parentValueId,
                        ArrayIndex = parentArrayIndex?.ToString()
                    };

                    if (childValue is IRedbObject redbObj)
                    {
                        // Auto-save if Id=0
                        if (redbObj.Id == 0)
                        {
                            var savedId = await SaveAsync((dynamic)childValue);
                            childRecord.Object = savedId;
                        }
                        else
                        {
                            childRecord.Object = redbObj.Id;
                        }
                        childRecord.Guid = RedbHash.ComputeFor(redbObj);
                    }
                    else if (childValue is long refId)
                    {
                        childRecord.Object = refId;
                    }

                    valuesList.Add(childRecord);
                }
                else
                {

                    
                    // Create a child field record linked to the parent Class field
                    var childRecord = new RedbValue
                    {
                        Id = await _context.Keys.NextObjectIdAsync(),
                        IdObject = objectId,
                        IdStructure = childStructure.Id,
                        ArrayParentId = parentValueId,  // ✅ Link to the parent Class field  
                        ArrayIndex = parentArrayIndex?.ToString()   // ✅ Inherit ArrayIndex if it's an array element
                    };

                    SetSimpleValueByType(childRecord, childStructure.DbType, childValue);
                    valuesList.Add(childRecord);

                }
            }
        }

        /// <summary>
        /// 🔍 Check if the type is a business class (not a primitive or an array)
        /// </summary>
        private static bool IsBusinessClassType(Type type)
        {
            // Primitives and strings are not business classes
            if (type.IsPrimitive || type == typeof(string) || type == typeof(decimal) || type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(TimeOnly) || type == typeof(DateOnly) || type == typeof(TimeSpan) || type == typeof(Guid)) 
                return false;
            
            // Arrays are not business classes (processed separately)
            if (type.IsArray || (typeof(IEnumerable).IsAssignableFrom(type) && type != typeof(string)))
                return false;
                
            // IRedbObject is not a business class (processed separately)
            if (type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition().Name.Contains("IRedbObject")))
                return false;
            
            // ⚠️ Task and Task<> are not business classes (technical types for async/await)
            if (type == typeof(Task) || (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Task<>)))
                return false;
            
            // ⚠️ IRedbListItem and RedbListItem are not business classes (dictionary elements, processed separately)
            if (type == typeof(IRedbListItem) || type == typeof(RedbListItem) || type.GetInterfaces().Contains(typeof(IRedbListItem)))
                return false;
                
            // Other classes are business classes
            return type.IsClass;
        }

        /// <summary>
        /// 🔧 Universal method for creating structures for any type via reflection
        /// </summary>
        private async Task SyncStructuresForType(IRedbScheme scheme, Type propsType)
        {
            // Use reflection to call the generic method SyncStructuresFromTypeAsync<TProps>
            var method = typeof(SchemeSyncProviderBase)
                .GetMethod("SyncStructuresFromTypeAsync", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            
            if (method != null)
            {
                var genericMethod = method.MakeGenericMethod(propsType);
                var result = await (Task<List<IRedbStructure>>)genericMethod.Invoke(_schemeSync, new object[] { scheme, true })!;

            }
            else
            {
                // Search for all methods for diagnostics
                var allMethods = typeof(SchemeSyncProviderBase).GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                var syncMethods = allMethods.Where(m => m.Name.Contains("Sync")).ToList();

                foreach (var m in syncMethods)
                {

                }
                throw new InvalidOperationException($"Method SyncStructuresFromTypeAsync not found in SchemeSyncProviderBase");
            }
        }

        // ===== 🌳 NEW METHODS FOR WORKING WITH THE STRUCTURE TREE =====
        
        /// <summary>
        /// 🔧 Simple field with structure tree support
        /// </summary>
        private async Task ProcessSimpleFieldWithTree(IRedbObject obj, StructureTreeNode structureNode, object? rawValue, List<RedbValue> valuesList)
        {
            var dbTypeForLog = await GetStructureDbType(structureNode.Structure);
            
            // ✅ Process nested objects (IRedbObject, IRedbListItem) before saving
            var dbType = await GetStructureDbType(structureNode.Structure);
            
            if (rawValue is IRedbListItem && dbType == "Long")
            {
                throw new InvalidOperationException(
                    $"Schema mismatch: field '{structureNode.Structure.Name}' has DbType='Long' but value is IRedbListItem. Update schema to use DbType='ListItem'.");
            }
            
            var processedValue = await ProcessNestedObjectsAsync(rawValue, dbType, false, obj.Id);
            
            var valueRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = structureNode.Structure.Id
            };
            
            SetSimpleValueByType(valueRecord, dbType, processedValue);
            valuesList.Add(valueRecord);
        }
        
        /// <summary>
        /// 📊 Array with structure subtree
        /// </summary>
        private async Task ProcessArrayWithSubtree(IRedbObject obj, StructureTreeNode arrayStructureNode, object? rawValue, List<RedbValue> valuesList, List<IRedbObject> objectsToSave, long? parentValueId = null)
        {
            var arraySubtreeDbType = await GetStructureDbType(arrayStructureNode.Structure);
            
            // Check schema mismatch for ListItem arrays
            if (arraySubtreeDbType == "Long" && rawValue is System.Collections.IEnumerable enumForCheck && rawValue is not string)
            {
                var firstItem = enumForCheck.Cast<object>().FirstOrDefault();
                if (firstItem is IRedbListItem)
                {
                    throw new InvalidOperationException(
                        $"Schema mismatch: array '{arrayStructureNode.Structure.Name}' has DbType='Long' but contains IRedbListItem. Update schema to use DbType='ListItem'.");
                }
            }
            
            
            if (rawValue == null) 
                return;

            if (rawValue is not IEnumerable enumerable || rawValue is string) 
            {
                throw new InvalidOperationException(
                    $"Array field '{arrayStructureNode.Structure.Name}' expected IEnumerable but got '{rawValue.GetType().FullName}'.");
            }


            // ✅ Create a base array record for both strategies
            var baseArrayRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = arrayStructureNode.Structure.Id,
                ArrayParentId = parentValueId
                // 🔥 Guid will be set AFTER processing elements!
            };
            valuesList.Add(baseArrayRecord);
            

            // ✅ Processing of array elements with correct subtree
            await ProcessArrayElementsWithSubtree(obj, arrayStructureNode, baseArrayRecord.Id, enumerable, valuesList, objectsToSave);
            
            // 🔥 FINAL HASH: compute base array record hash from element hashes
            await ComputeArrayHashFromElements(baseArrayRecord, arrayStructureNode, valuesList, rawValue);
        }
        
        /// <summary>
        /// 📖 Save Dictionary field with base record + elements via _array_parent_id + _array_index (serialized key)
        /// </summary>
        private async Task ProcessDictionaryWithSubtree(IRedbObject obj, StructureTreeNode dictStructureNode, object? rawValue, List<RedbValue> valuesList, List<IRedbObject> objectsToSave, long? parentValueId = null)
        {
            if (rawValue == null) return;
            
            // Dictionary implements IDictionary, but we need to get key-value pairs via reflection
            var dictType = rawValue.GetType();
            if (!dictType.IsGenericType) return;
            
            var genericDef = dictType.GetGenericTypeDefinition();
            if (genericDef != typeof(Dictionary<,>) && genericDef != typeof(IDictionary<,>)) return;
            
            var keyType = dictType.GetGenericArguments()[0];
            
            // Create BASE record for Dictionary with hash
            var dictHash = RedbHash.ComputeForProps(rawValue);
            var baseDictRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = dictStructureNode.Structure.Id,
                ArrayParentId = parentValueId,  // 🔥 FIX: For nested Dictionary, link to parent Class record
                Guid = dictHash  // Hash of entire Dictionary
            };
            valuesList.Add(baseDictRecord);
            
            // Get DbType for value type - check if it's a class type
            var isClassValue = await IsClassTypeStructure(dictStructureNode.Structure);
            var dbType = await GetStructureDbType(dictStructureNode.Structure);
            
            // Iterate dictionary entries
            var enumerator = ((System.Collections.IEnumerable)rawValue).GetEnumerator();
            while (enumerator.MoveNext())
            {
                var kvp = enumerator.Current;
                if (kvp == null) continue;
                
                // Get Key and Value via reflection
                var kvpType = kvp.GetType();
                var key = kvpType.GetProperty("Key")!.GetValue(kvp);
                var value = kvpType.GetProperty("Value")!.GetValue(kvp);
                
                if (key == null) continue; // Skip null keys
                
                // Serialize key using RedbKeySerializer
                var serializedKey = RedbKeySerializer.SerializeObject(key, keyType);
                
                // Create element record
                var elementRecord = new RedbValue
                {
                    Id = await _context.Keys.NextObjectIdAsync(),
                    IdObject = obj.Id,
                    IdStructure = dictStructureNode.Structure.Id,
                    ArrayParentId = baseDictRecord.Id,  // Link to base Dictionary record
                    ArrayIndex = serializedKey          // Serialized key as string
                };
                
                // If value is null, just add the record with null values
                if (value == null)
                {
                    valuesList.Add(elementRecord);
                    continue;
                }
                
                // Check if value is IRedbObject reference - by schema OR by C# type
                var isRedbObjectValue = await IsRedbObjectStructure(dictStructureNode.Structure) ||
                    IsRedbObjectType(value.GetType());
                
                if (isRedbObjectValue)
                {
                    // RedbObject value in Dictionary - save as reference with _Object field
                    var redbObj = (IRedbObject)value;
                    elementRecord.Object = redbObj.Id;
                    elementRecord.Guid = RedbHash.ComputeFor(redbObj);
                    valuesList.Add(elementRecord);
                }
                else if (isClassValue || dictStructureNode.Children.Count > 0)
                {
                    // If value is Class type, create hash and save children recursively
                    elementRecord.Guid = RedbHash.ComputeForProps(value);
                    valuesList.Add(elementRecord);
                    
                    // Recursively process child fields using subtree
                    await ProcessBusinessClassChildrenWithSubtree(obj, elementRecord.Id, value, dictStructureNode.Children, valuesList, objectsToSave);
                }
                else
                {
                    // For simple types
                    var processedValue = await ProcessNestedObjectsAsync(value, dbType, false, obj.Id);
                    SetSimpleValueByType(elementRecord, dbType, processedValue);
                    valuesList.Add(elementRecord);
                }
            }
        }
        
        /// <summary>
        /// 🔢 Array elements with structure subtree
        /// </summary>
        private async Task ProcessArrayElementsWithSubtree(IRedbObject obj, StructureTreeNode arrayStructureNode, long parentValueId, IEnumerable enumerable, List<RedbValue> valuesList, List<IRedbObject> objectsToSave)
        {
            
            long actualParentId = parentValueId;
            
            int index = 0;
            foreach (var item in enumerable)
            {
                
                var elementRecord = new RedbValue
                {
                    Id = await _context.Keys.NextObjectIdAsync(),
                    IdObject = obj.Id,
                    IdStructure = arrayStructureNode.Structure.Id,
                    ArrayParentId = actualParentId, // 🎯 Use found or created ID
                    ArrayIndex = index.ToString()
                };

                if (item != null)
                {
                    var itemType = item.GetType();
                    
                    // ♻️ RECURSION WITH SUBTREES: different types of elements
                    if (await IsRedbObjectStructure(arrayStructureNode.Structure))
                    {
                        // 🔗 IRedbObject array element - BULK STRATEGY: take ID directly 
                        var redbObj = (IRedbObject)item;
                        var objectId = redbObj.Id;
                        var objHash = RedbHash.ComputeFor(redbObj);
                        
                        elementRecord.Object = objectId;  // ✅ _Object - special field for FK on _objects
                        elementRecord.Guid = objHash;  // 🔥 RedbObject HASH for ChangeTracking!
                        valuesList.Add(elementRecord);
                        
                    }
                    else if (await IsListItemStructure(arrayStructureNode.Structure))
                    {
                        
                        // 📋 ListItem array element - take ID directly (similarly to IRedbObject)
                        var listItem = (IRedbListItem)item;
                        var listItemId = listItem.Id;
                        
                        elementRecord.ListItem = listItemId;  // ⭐ Write to the ListItem field
                        elementRecord.Guid = RedbHash.ComputeForProps(item);  // 🔥 ListItem HASH for ChangeTracking!
                        valuesList.Add(elementRecord);
                        
                    }
                    else if (IsBusinessClassType(itemType))
                    {
                        // 🏗️ Business class array element
                        var itemHash = RedbHash.ComputeForProps(item);
                        elementRecord.Guid = itemHash;
                        valuesList.Add(elementRecord);

                        // ♻️ RECURSION: processing child fields with subtree  
                        await ProcessBusinessClassChildrenWithSubtree(obj, elementRecord.Id, item, arrayStructureNode.Children, valuesList, objectsToSave, index);
                    }
                    else
                    {
                        
                        // Simple array element
                        var elementDbType = await GetStructureDbType(arrayStructureNode.Structure);
                        
                        
                        // ⚠️ CRITICAL: Calling ProcessNestedObjectsAsync to extract ID from IRedbListItem!
                        var processedValue = await ProcessNestedObjectsAsync(item, elementDbType, false, obj.Id);
                        
                        
                        SetSimpleValueByType(elementRecord, elementDbType, processedValue);
                        
                        
                        valuesList.Add(elementRecord);
                    }
                }
                else
                {
                    valuesList.Add(elementRecord); // null element
                }

                index++;
            }
            
        }
        
        /// <summary>
        /// 🔥 Computing base array record hash from element hashes
        /// For RedbObject[] and ListItem[] - combines element hashes
        /// For others - uses standard array hash
        /// </summary>
        private async Task ComputeArrayHashFromElements(RedbValue baseArrayRecord, StructureTreeNode arrayStructureNode, List<RedbValue> valuesList, object rawValue)
        {
            // Check the array element type
            var isRedbObjectArray = await IsRedbObjectStructure(arrayStructureNode.Structure);
            var isListItemArray = await IsListItemStructure(arrayStructureNode.Structure);
            
            if (isRedbObjectArray || isListItemArray)
            {
                // 🔗 For RedbObject[] and ListItem[] - collect element hashes
                var elementHashes = valuesList
                    .Where(v => v.ArrayParentId == baseArrayRecord.Id && !string.IsNullOrEmpty(v.ArrayIndex) && v.Guid.HasValue)
                    .OrderBy(v => int.TryParse(v.ArrayIndex, out var idx) ? idx : 0)
                    .Select(v => v.Guid!.Value)
                    .ToList();
                
                if (elementHashes.Any())
                {
                    // Combine element hashes into one array hash
                    baseArrayRecord.Guid = RedbHash.CombineHashes(elementHashes);
                }
                else
                {
                    // Empty array
                    baseArrayRecord.Guid = Guid.Empty;
                    
                }
            }
            else
            {
                // 📦 For business class and primitive arrays - standard hash
                baseArrayRecord.Guid = RedbHash.ComputeForProps(rawValue);
            }
        }
        
        /// <summary>
        /// 🏗️ Business class with structure subtree
        /// 🔥 FIX: Added parentValueId for nested classes!
        /// </summary>
        private async Task ProcessBusinessClassWithSubtree(IRedbObject obj, StructureTreeNode classStructureNode, object? rawValue, List<RedbValue> valuesList, List<IRedbObject> objectsToSave, long? parentValueId = null)
        {

            if (rawValue == null) 
            {
                return;
            }

            // ✅ Compute UUID hash of the business class
            var classHash = RedbHash.ComputeForProps(rawValue);

            // Create base Class field record with hash in _Guid
            // 🔥 FIX: Set ArrayParentId for nested classes!
            var classRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = classStructureNode.Structure.Id,
                ArrayParentId = parentValueId,  // 🔥 FIX: Link to the parent class!
                Guid = classHash
            };
            
            valuesList.Add(classRecord);

            // ✅ Process child fields with the correct subtree!
            await ProcessBusinessClassChildrenWithSubtree(obj, classRecord.Id, rawValue, classStructureNode.Children, valuesList, objectsToSave);
        }
        
        /// <summary>
        /// 👶 Recursive processing of child fields of a business class with subtree
        /// </summary>
        private async Task ProcessBusinessClassChildrenWithSubtree(IRedbObject obj, long parentValueId, object businessObject, List<StructureTreeNode> childrenSubtree, List<RedbValue> valuesList, List<IRedbObject> objectsToSave, int? parentArrayIndex = null)
        {
            var businessType = businessObject.GetType();
            
            foreach (var childStructureNode in childrenSubtree)
            {
                // ✅ PROPERTY EXISTENCE CHECK IN C# CLASS  
                var property = businessType.GetProperty(childStructureNode.Structure.Name);
                if (property == null || property.GetIndexParameters().Length > 0)
                {
                    continue;
                }

                var childValue = property.GetValue(businessObject);

                // ✅ NULL SEMANTICS  
                if (!ObjectStorageProviderExtensions.ShouldCreateValueRecord(childValue, childStructureNode.Structure.StoreNull ?? false))
                {
                    continue;
                }

                // ♻️ RECURSIVE PROCESSING with correct subtrees
                if (childStructureNode.Structure.CollectionType == Core.Utils.RedbTypeIds.Dictionary)
                {
                    // 🔥 FIX: Pass parentValueId for nested Dictionary inside Class
                    await ProcessDictionaryWithSubtree(obj, childStructureNode, childValue, valuesList, objectsToSave, parentValueId);
                }
                else if (childStructureNode.Structure.CollectionType == Core.Utils.RedbTypeIds.Array)
                {
                    // ✅ FIX: Pass parentValueId for nested arrays
                    await ProcessArrayWithSubtree(obj, childStructureNode, childValue, valuesList, objectsToSave, parentValueId);
                }
                else if (await IsClassTypeStructure(childStructureNode.Structure))
                {
                    // 🔥 FIX: Pass parentValueId for nested classes!
                    await ProcessBusinessClassWithSubtree(obj, childStructureNode, childValue, valuesList, objectsToSave, parentValueId);
                }
                else if (await IsRedbObjectStructure(childStructureNode.Structure))
                {
                    await ProcessIRedbObjectField(obj, childStructureNode.Structure, childValue, objectsToSave, valuesList, parentValueId, parentArrayIndex);
                }
                else
                {
                    // Create a child field record linked to the parent Class field
                    var childRecord = new RedbValue
                    {
                        Id = await _context.Keys.NextObjectIdAsync(),
                        IdObject = obj.Id,
                        IdStructure = childStructureNode.Structure.Id,
                        ArrayParentId = parentValueId,
                        ArrayIndex = parentArrayIndex?.ToString()   // ✅ Inherit ArrayIndex if it's an array element
                    };

                    var childDbType = await GetStructureDbType(childStructureNode.Structure);
                    SetSimpleValueByType(childRecord, childDbType, childValue);
                    valuesList.Add(childRecord);

                }
            }
        }
        
        /// <summary>
        /// 🔗 IRedbObject field processing with ID search in the object collector
        /// </summary>
        private async Task ProcessIRedbObjectField(IRedbObject obj, IRedbStructure structure, object? redbObjectValue, List<IRedbObject> objectsToSave, List<RedbValue> valuesList, long? parentValueId = null, int? parentArrayIndex = null)
        {

            
            if (structure.CollectionType == Core.Utils.RedbTypeIds.Array)
            {
                // IRedbObject ARRAY
                await ProcessIRedbObjectArray(obj, structure, (IEnumerable)redbObjectValue!, objectsToSave, valuesList, parentValueId);
            }
            else
            {
                // SINGLE IRedbObject
                await ProcessSingleIRedbObject(obj, structure, (IRedbObject)redbObjectValue!, objectsToSave, valuesList, parentValueId, parentArrayIndex);
            }
        }
        
        /// <summary>
        /// 🔗 Single IRedbObject with ID search in the collector
        /// </summary>
        private async Task ProcessSingleIRedbObject(IRedbObject obj, IRedbStructure structure, IRedbObject redbObjectValue, List<IRedbObject> objectsToSave, List<RedbValue> valuesList, long? parentValueId = null, int? parentArrayIndex = null)
        {
            // 🎯 BULK STRATEGY: Take ID directly from the object (already saved recursively)
            var objectId = redbObjectValue.Id;
            
            var record = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = structure.Id,
                Object = objectId,  // 🔥 FIX: _Object - FK to _objects
                ArrayParentId = parentValueId  // 🔥 FIX: link to parent element record (unique per array element)
                // ArrayIndex NOT set: parent element record ID is sufficient for disambiguation
            };
            
            valuesList.Add(record);

        }
        
        /// <summary>
        /// 📊 IRedbObject array with ID search in the collector
        /// </summary>
        private async Task ProcessIRedbObjectArray(IRedbObject obj, IRedbStructure structure, IEnumerable redbObjectArray, List<IRedbObject> objectsToSave, List<RedbValue> valuesList, long? parentValueId = null)
        {
            // Create base array record
            var arrayHash = RedbHash.ComputeForProps((object)redbObjectArray);
            var baseArrayRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = structure.Id,
                ArrayParentId = parentValueId,  // 🔥 FIX: link to parent element
                Guid = arrayHash
            };
            valuesList.Add(baseArrayRecord);


            // Process array elements
            int index = 0;
            foreach (var item in redbObjectArray)
            {
                if (item != null && IsRedbObjectType(item.GetType()))
                {
                    // 🎯 BULK STRATEGY: Take ID directly from the object (already saved recursively)
                    var redbObj = (IRedbObject)item;
                    var objectId = redbObj.Id;
                    
                    var elementRecord = new RedbValue
                    {
                        Id = await _context.Keys.NextObjectIdAsync(),
                        IdObject = obj.Id,
                        IdStructure = structure.Id,
                        ArrayParentId = baseArrayRecord.Id,
                        ArrayIndex = index.ToString(),
                        Object = objectId  // 🔥 FIX: _Object - FK to _objects
                    };
                    
                    valuesList.Add(elementRecord);

                }
                index++;
            }
        }
        
        /// <summary>
        /// 🔍 Searching for an object in the collector by various strategies
        /// </summary>
        private IRedbObject? FindObjectInCollector(IRedbObject target, List<IRedbObject> objectsToSave)
        {
            // Strategy 1: Exact reference match
            var byReference = objectsToSave.FirstOrDefault(o => ReferenceEquals(o, target));
            if (byReference != null)
            {

                return byReference;
            }
            
            // Strategy 2: By Name + Type
            var byNameAndType = objectsToSave.FirstOrDefault(o => 
                o.Name == target.Name && 
                o.GetType() == target.GetType());
            if (byNameAndType != null)
            {

                return byNameAndType;
            }
            
            // Strategy 3: By Hash (if set)
            if (target.Hash.HasValue)
            {
                var byHash = objectsToSave.FirstOrDefault(o => o.Hash == target.Hash);
                if (byHash != null)
                {

                    return byHash;
                }
            }
            

            return null;
        }

        // SetSimpleValueByType already exists in main file ObjectStorageProviderBase.cs

        /// <summary>
        /// Step 6: Strategy selection for values processing.
        /// OpenSource: only DeleteInsert strategy is supported.
        /// ChangeTracking requires Pro edition - override in ProObjectStorageProviderBase.
        /// </summary>
        protected virtual async Task PrepareValuesByStrategy(List<IRedbObject> objects, List<RedbValue> valuesList, bool isRootObjectNew)
        {
            var strategy = _configuration.EavSaveStrategy;

            if (strategy == EavSaveStrategy.ChangeTracking)
            {
                throw new RedbProRequiredException("ChangeTracking", ProFeatureCategory.ChangeTracking);
            }

            await PrepareValuesWithTreeDeleteInsert(objects, valuesList);
        }

        /// <summary>
        /// DeleteInsert strategy: delete all existing values, then insert new ones.
        /// </summary>
        protected async Task PrepareValuesWithTreeDeleteInsert(List<IRedbObject> objects, List<RedbValue> valuesList)
        {
            
            // Remove all existing values for objects (simple strategy)
            var objectIds = objects.Where(o => o.Id != 0).Select(o => o.Id).ToList();
            if (objectIds.Any())
            {
                // Delete all existing values for objects via bulk operation
                await _context.Bulk.BulkDeleteValuesByObjectIdsAsync(objectIds);
            }
        }

        /// <summary>
        /// 🔢 SEPARATE PROCESSING OF ARRAY ELEMENTS BY INDICES
        /// Groups by structure, compares by indices: element[0] with element[0], element[1] with element[1]
        /// </summary>
        private async Task<(int updated, int inserted, int skipped)> ProcessArrayElementsChangeTracking(
            List<RedbValue> newArrayElements, 
            List<RedbValue> existingValues, 
            List<RedbValue> valuesToInsert, 
            List<RedbValue> valuesToDelete, 
            Dictionary<long, StructureFullInfo> structuresFullInfo)
        {
            int localUpdated = 0, localInserted = 0, localSkipped = 0;
            
            // 1. 🔧 ArrayParentId FIX: use existing base records
            // Group new elements by structure
            var newElementsByStructure = newArrayElements
                .GroupBy(e => new { e.IdObject, e.IdStructure })
                .ToList();
                
            foreach (var structureGroup in newElementsByStructure)
            {
                var key = structureGroup.Key;
                var elementsInGroup = structureGroup.ToList();
                
                // Look for existing base array record in the DB
                var existingBaseField = existingValues
                    .FirstOrDefault(v => v.IdObject == key.IdObject && 
                                        v.IdStructure == key.IdStructure && 
                                        string.IsNullOrEmpty(v.ArrayIndex));
                                        
                if (existingBaseField != null) 
                {
                    // ✅ EXISTING array: reuse base record from DB
                        foreach (var element in elementsInGroup)
                        {
                            element.ArrayParentId = existingBaseField.Id;
                        }
                    }
                // NOTE: else block is NOT needed!
                // For NEW objects, base records are already created in ProcessArrayWithSubtree,
                // so array elements already have the correct ArrayParentId
            }
            
            // 2. 🎯 GROUP BY ArrayParentId (after fix)
            var existingArraysDict = existingValues
                .Where(v => !string.IsNullOrEmpty(v.ArrayIndex))
                .GroupBy(v => v.ArrayParentId)
                .ToDictionary(g => g.Key, g => g.OrderBy(x => int.TryParse(x.ArrayIndex, out var idx) ? idx : 0).ToList());
                
            var newArraysDict = newArrayElements
                .GroupBy(v => v.ArrayParentId)
                .ToDictionary(g => g.Key, g => g.OrderBy(x => x.ArrayIndex).ToList());
            
            // 3. 🎯 COMPARE BY ArrayParentId
            var allArrayParentIds = existingArraysDict.Keys.Union(newArraysDict.Keys).ToList();

            foreach (var arrayParentId in allArrayParentIds)
            {
                var existingElements = existingArraysDict.GetValueOrDefault(arrayParentId) ?? new List<RedbValue>();
                var newElements = newArraysDict.GetValueOrDefault(arrayParentId) ?? new List<RedbValue>();
                
                // 🎯 MAIN LOGIC: compare by indices with improved logic for business classes
                var (updated, inserted, skipped) = await CompareArrayElementsByIndex(existingElements, newElements, valuesToInsert, valuesToDelete, structuresFullInfo);
                localUpdated += updated;
                localInserted += inserted;
                localSkipped += skipped;
            }
            
            return (localUpdated, localInserted, localSkipped);
        }

        /// <summary>
        /// 🎯 COMPARING ARRAY ELEMENTS BY INDICES
        /// </summary>
        private async Task<(int updated, int inserted, int skipped)> CompareArrayElementsByIndex(
            List<RedbValue> existingElements, 
            List<RedbValue> newElements, 
            List<RedbValue> valuesToInsert, 
            List<RedbValue> valuesToDelete, 
            Dictionary<long, StructureFullInfo> structuresFullInfo)
        {
            int localUpdated = 0, localInserted = 0, localSkipped = 0;
            var maxIndex = Math.Max(existingElements.Count, newElements.Count);
            
            for (int i = 0; i < maxIndex; i++)
            {
                var existingElement = i < existingElements.Count ? existingElements[i] : null;
                var newElement = i < newElements.Count ? newElements[i] : null;
                
                if (existingElement != null && newElement != null)
                {
                    // Both elements exist - compare by DbType
                    var structInfo = structuresFullInfo[newElement.IdStructure];
                    var legacyStructuresInfo = new Dictionary<long, string> { { newElement.IdStructure, structInfo.DbType } };
                    
                    var changed = await IsValueChanged(existingElement, newElement, legacyStructuresInfo);
                    if (changed)
                    {
                        UpdateExistingValueFields(existingElement, newElement, legacyStructuresInfo);
                        localUpdated++;
                    }
                    else
                    {
                        localSkipped++;
                    }
                }
                else if (existingElement != null && newElement == null)
                {
                    valuesToDelete.Add(existingElement);
                }
                else if (existingElement == null && newElement != null)
                {
                    valuesToInsert.Add(newElement);
                    localInserted++;
                }
            }
            
            return (localUpdated, localInserted, localSkipped);
        }

        /// <summary>
        /// 📋 Structure with full information for ChangeTracking
        /// </summary>
        public class StructureFullInfo
        {
            public string DbType { get; set; } = "String";
            public bool IsArray { get; set; }
            public bool StoreNull { get; set; }
        }

        /// <summary>
        /// 🔍 Compares two values only by the significant field (by DbType)
        /// ✅ FIXED: Improved comparison for array business classes
        /// </summary>
        private async Task<bool> IsValueChanged(RedbValue oldValue, RedbValue newValue, Dictionary<long, string> structuresInfo)
        {
            if (!structuresInfo.TryGetValue(newValue.IdStructure, out var dbType))
            {
                // Unknown type - consider it changed
                return true;
            }

            // 🎯 SPECIAL LOGIC FOR BUSINESS CLASS ARRAY ELEMENTS
            // If it's an array element (has ArrayIndex) and there's a Guid - compare by Guid hash
            if (!string.IsNullOrEmpty(oldValue.ArrayIndex) && !string.IsNullOrEmpty(newValue.ArrayIndex) && 
                (oldValue.Guid.HasValue || newValue.Guid.HasValue))
            {
                // For business classes in arrays - comparison by hash is more reliable
                var oldGuid = oldValue.Guid;
                var newGuid = newValue.Guid;
                
                // If one of the hashes is null - consider it changed
                if (!oldGuid.HasValue || !newGuid.HasValue)
                    return true;
                    
                return oldGuid != newGuid;
            }

            // 📝 STANDARD LOGIC for regular fields
            return dbType switch
            {
                "String" => oldValue.String != newValue.String,
                "Long" => oldValue.Long != newValue.Long,
                "Double" => oldValue.Double != newValue.Double,
                "Numeric" => oldValue.Numeric != newValue.Numeric,  // ⭐ ADDED
                "DateTime" => oldValue.DateTimeOffset != newValue.DateTimeOffset,  // Backward compatibility
                "DateTimeOffset" => oldValue.DateTimeOffset != newValue.DateTimeOffset,
                "Boolean" => oldValue.Boolean != newValue.Boolean,
                "Guid" => oldValue.Guid != newValue.Guid,
                "ByteArray" => oldValue.ByteArray != newValue.ByteArray,
                "Object" => oldValue.Object != newValue.Object,       // ⭐ ADDED
                "ListItem" => oldValue.ListItem != newValue.ListItem, // ⭐ ADDED
                _ => true // Unknown type - consider changed
            };
        }

        /// <summary>
        /// Updates existing value fields from new value (only significant fields).
        /// </summary>
        protected void UpdateExistingValueFields(RedbValue existingValue, RedbValue newValue, Dictionary<long, string> structuresInfo)
        {
            if (!structuresInfo.TryGetValue(newValue.IdStructure, out var dbType))
            {
                throw new InvalidOperationException(
                    $"Structure Id={newValue.IdStructure} not found in structuresInfo dictionary.");
            }
            
            // If newValue contains Object or ListItem - copy them regardless of DbType
            if (newValue.Object.HasValue)
            {
                existingValue.Object = newValue.Object;
                existingValue.Guid = newValue.Guid;
                return;
            }
            if (newValue.ListItem.HasValue)
            {
                existingValue.ListItem = newValue.ListItem;
                existingValue.Guid = newValue.Guid;
                return;
            }
            
            // Update only significant field by DbType
            switch (dbType)
            {
                case "String":
                    existingValue.String = newValue.String;
                    break;
                case "Long":
                    existingValue.Long = newValue.Long;
                    break;
                case "Double":
                    existingValue.Double = newValue.Double;
                    break;
                case "Numeric":  // ⭐ ADDED
                    existingValue.Numeric = newValue.Numeric;
                    break;
                case "DateTime":  // Backward compatibility
                case "DateTimeOffset":
                    existingValue.DateTimeOffset = newValue.DateTimeOffset;
                    break;
                case "Boolean":
                    existingValue.Boolean = newValue.Boolean;
                    break;
                case "Guid":
                    existingValue.Guid = newValue.Guid;
                    break;
                case "ByteArray":
                    existingValue.ByteArray = newValue.ByteArray;
                    break;
                case "Object":  // ⭐ ADDED
                    existingValue.Object = newValue.Object;
                    break;
                case "ListItem":  // ⭐ ADDED
                    existingValue.ListItem = newValue.ListItem;
                    break;
            }
        }



        /// <summary>
        /// 💾 STEP 7: Batch saving in correct order
        /// 🚀 OPTIMIZATION: BulkInsert/BulkUpdate for objects and values via COPY protocol
        /// </summary>
        private async Task CommitAllChangesBatch(List<IRedbObject> objects, List<RedbValue> valuesList)
        {
            
            // 🔥 OPTIMIZATION: Batch loading of existing objects (ONE request - only ID)
            var allIds = objects.Select(o => o.Id).Where(id => id != 0).ToArray();
            var existingIdsInDb = allIds.Any()
                ? await _context.QueryScalarListAsync<long>(Sql.ObjectStorage_SelectExistingIds(), allIds)
                : [];
            
            var existingIdsSet = existingIdsInDb.ToHashSet();
            
            // 🚀 Separate objects into new/existing
            var newObjects = objects.Where(o => !existingIdsSet.Contains(o.Id)).ToList();
            var existingObjects = objects.Where(o => existingIdsSet.Contains(o.Id)).ToList();
            
            // 🚀 STEP 1: BulkInsert of new objects
            if (newObjects.Any())
            {
                var newRecords = newObjects.Select(ConvertToObjectRecord).ToList();
                await _context.Bulk.BulkInsertObjectsAsync(newRecords);
            }
            
            // 🚀 STEP 2: BulkUpdate of existing objects
            if (existingObjects.Any())
            {
                var existingRecords = existingObjects.Select(ConvertToObjectRecord).ToList();
                await _context.Bulk.BulkUpdateObjectsAsync(existingRecords);
            }
            
            // 🔥 STEP 2.5: BulkUpdate for _pendingValuesToUpdate (from ChangeTracking)
            if (_pendingValuesToUpdate.Any())
            {
                var uniqueUpdates = DeduplicateValueUpdates(_pendingValuesToUpdate, "CommitAllChangesBatch");
                await _context.Bulk.BulkUpdateValuesAsync(uniqueUpdates);
                _pendingValuesToUpdate.Clear();
            }

            // 🚀 STEP 3: Values processing - now all via bulk operations
            
            // STEP 3a: Remove those marked for deletion
            if (_pendingValuesToDelete.Any())
            {
                await _context.Bulk.BulkDeleteValuesAsync(_pendingValuesToDelete);
                _pendingValuesToDelete.Clear();
            }
            
            // STEP 3b: Insert those marked for insertion
            if (_pendingValuesToInsert.Any())
            {
                var sortedInserts = ValuesTopologicalSort.SortByFkDependency(_pendingValuesToInsert.ToList());
                await _context.Bulk.BulkInsertValuesAsync(sortedInserts);
                _pendingValuesToInsert.Clear();
            }
            
            
            // STEP 3c: BulkInsert for values from valuesList (new objects)
            if (valuesList.Any())
            {
                var sortedValues = ValuesTopologicalSort.SortByFkDependency(valuesList);
                await _context.Bulk.BulkInsertValuesAsync(sortedValues);
            }
            
        }

        /// <summary>
        /// Deduplicates value update list by Id to prevent MERGE/UPDATE conflicts
        /// when the same value ID appears in updates from multiple tree comparison paths
        /// (e.g. shared RedbObject references). Logs a warning if duplicates are detected.
        /// </summary>
        protected List<RedbValue> DeduplicateValueUpdates(List<RedbValue> values, string caller)
        {
            if (values.Count <= 1)
                return values;

            var grouped = values.GroupBy(v => v.Id).ToList();
            if (grouped.Count == values.Count)
                return values; // No duplicates — fast path

            var duplicateCount = values.Count - grouped.Count;
            var duplicateGroups = grouped.Where(g => g.Count() > 1).ToList();

            foreach (var g in duplicateGroups)
            {
                var entries = g.ToList();
                var first = entries[0];
                Logger?.LogWarning(
                    "REDB {Caller}: duplicate value _id={ValueId} (IdObject={IdObject}, IdStructure={IdStructure}, " +
                    "ArrayParentId={ArrayParentId}, ArrayIndex={ArrayIndex}) appears {Count} times in pending updates. " +
                    "Keeping last entry.",
                    caller, g.Key, first.IdObject, first.IdStructure,
                    first.ArrayParentId, first.ArrayIndex, entries.Count);
            }

            return grouped.Select(g => g.Last()).ToList();
        }

        #region BULK DELETEINSERT OPTIMIZATION

        /// <summary>
        /// 🚀 OPTIMIZED BULK DELETE/INSERT: Maximum performance via bulk operations
        /// Recursive saving of all objects in a single transaction with level control
        /// </summary>
        private async Task<long> SaveAsyncDeleteInsertBulk<TProps>(IRedbObject<TProps> obj, IRedbUser user) where TProps : class, new()
        {
            // 🎯 TRANSACTION LEVEL CONTROL: create transaction only at the top level
            bool isTopLevel = _context.CurrentTransaction == null;
            IRedbTransaction? transaction = null;

            if (isTopLevel)
            {
                transaction = await _context.BeginTransactionAsync();
            }

            try
            {
                // === OBJECT PREPARATION ===
                if (obj.Props == null)
                {
                    throw new ArgumentException("Object properties cannot be null", nameof(obj));
                }

                // Recompute hash
                var currentHash = RedbHash.ComputeFor(obj);
                if (currentHash.HasValue)
                {
                    obj.Hash = currentHash.Value;
                }

                // === 1. FIRST save/update the main object in _objects ===
                await EnsureMainObjectSaved(obj, user);

                // === 2. NOW recursively save ALL nested RedbObjects (with correct ParentId) ===
                await ProcessAllNestedRedbObjectsFirst(obj);

                // === 2.5. 🎯 CRITICAL: Update link IDs in Props of the main object ===
                await SynchronizeNestedObjectIds(obj);

                // === 3. BULK DELETE of existing values (excluding nested RedbObjects) ===
                if (obj.Id != 0)
                {
                    await BulkDeleteExistingValues(obj.Id);
                }

                // === 4. Data preparation for BULK INSERT ===
                var valuesList = new List<RedbValue>();
                await PrepareAllValuesForInsert(obj, valuesList);

                // === 5. BULK INSERT of all values in one operation (with topological sort for FK) ===
                if (valuesList.Any())
                {
                    var sortedValues = ValuesTopologicalSort.SortByFkDependency(valuesList);
                    await _context.Bulk.BulkInsertValuesAsync(sortedValues);
                }

                // === 6. COMMIT only at the top level ===
                if (isTopLevel && transaction != null)
                {
                    await transaction.CommitAsync();
                }

                // === 7. CACHE UPDATE for the main and all nested objects ===
                if (_configuration.EnablePropsCache && PropsCache.Instance != null && obj.Hash.HasValue)
                {
                    // ✅ Save the ENTIRE object to cache (with base fields + Props)
                    if (obj is RedbObject<TProps> redbObj)
                    {
                        PropsCache.Set(redbObj);
                    }
                    
                    // ✅ Recursive caching of all nested RedbObjects (using method from ObjectStorageProviderBase.cs)
                    if (obj.Props != null)
                    {
                        CacheNestedObjects(obj.Props);
                    }
                }

                return obj.Id;
            }
            catch (Exception ex)
            {
                if (isTopLevel && transaction != null)
                {
                    await transaction.RollbackAsync();
                }
                throw;
            }
            finally
            {
                if (transaction != null)
                    await transaction.DisposeAsync();
            }
        }

        /// <summary>
        /// 🔗 Recursive processing of all nested RedbObjects before main bulk operations
        /// Saves them in the same transaction, ensuring correct IDs for links
        /// </summary>
        private async Task ProcessAllNestedRedbObjectsFirst<TProps>(IRedbObject<TProps> obj) where TProps : class, new()
        {
            if (obj.Props == null) return;
            
            var nestedObjects = new List<IRedbObject>();
            
            // 🔍 SEARCH for all nested RedbObjects in the object's properties
            await ExtractNestedRedbObjects(obj.Props, nestedObjects);
            
            if (!nestedObjects.Any())
            {
                return;
            }
            
            // 🚀 RECURSIVE SAVING of each nested object
            foreach (var nestedObj in nestedObjects)
            {
                if (nestedObj.Id == 0)
                {
                    // 🆕 New nested object - create with parent
                    if (nestedObj.ParentId == 0 || nestedObj.ParentId == null)
                    {
                        nestedObj.ParentId = obj.Id;
                    }

                    nestedObj.Id = await SaveAsync((dynamic)nestedObj); // Recursive call - will fall back into bulk strategy if needed
                }
                else
                {
                    // 🔄 Existing nested object - update
                    await SaveAsync((dynamic)nestedObj); // Recursive call
                }
            }
        }
        
        /// <summary>
        /// 🔍 Extract all nested RedbObjects from object Props via reflection
        /// </summary>
        private async Task ExtractNestedRedbObjects(object properties, List<IRedbObject> nestedObjects)
        {
            if (properties == null) return;
            
            var propertiesType = properties.GetType();
            var allProperties = propertiesType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            
            foreach (var prop in allProperties)
            {
                // Skip indexers (e.g. Dictionary<K,V>.Item[key])
                if (prop.GetIndexParameters().Length > 0) continue;
                
                try 
                {
                    var value = prop.GetValue(properties);
                    if (value == null) continue;
                    
                    var valueType = value.GetType();
                    
                    // 🔍 Check single RedbObject
                    if (IsRedbObjectType(valueType))
                    {
                        var redbObj = (IRedbObject)value;
                        nestedObjects.Add(redbObj);
                    }
                    // 🔍 Check array of RedbObjects
                    else if (valueType.IsArray || (valueType.IsGenericType && valueType.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
                    {
                        if (value is IEnumerable enumerable)
                        {
                            foreach (var item in enumerable)
                            {
                                if (item != null && IsRedbObjectType(item.GetType()))
                                {
                                    var redbObj = (IRedbObject)item;
                                    nestedObjects.Add(redbObj);
                                }
                            }
                        }
                    }
                    // 🔍 Check Dictionary<K, RedbObject<T>>
                    else if (IsDictionaryWithRedbObjectValue(valueType))
                    {
                        var valuesProperty = valueType.GetProperty("Values");
                        if (valuesProperty != null)
                        {
                            var dictValues = (IEnumerable)valuesProperty.GetValue(value)!;
                            foreach (var item in dictValues)
                            {
                                if (item != null && IsRedbObjectType(item.GetType()))
                                {
                                    var redbObj = (IRedbObject)item;
                                    nestedObjects.Add(redbObj);
                                    
                                    // Recursion for Props of the nested RedbObject
                                    var nestedProps = GetPropertiesFromRedbObject(redbObj);
                                    if (nestedProps != null)
                                    {
                                        await ExtractNestedRedbObjects(nestedProps, nestedObjects);
                                    }
                                }
                            }
                        }
                    }
                    // 🔍 Recursive check of nested business classes
                    else if (IsBusinessClassType(valueType))
                    {
                        await ExtractNestedRedbObjects(value, nestedObjects);
                    }
                    // 🔍 Check of business class arrays
                    else if (valueType.IsArray && IsBusinessClassType(valueType.GetElementType()!))
                    {
                        if (value is IEnumerable enumerable)
                        {
                            foreach (var item in enumerable)
                            {
                                if (item != null)
                                {
                                    await ExtractNestedRedbObjects(item, nestedObjects);
                                }
                            }
                        }
                    }
                }
                catch
                {
                    throw;
                }
            }
        }
        
        /// <summary>
        /// 🔧 CRITICAL SYNCHRONIZATION: Update link IDs in Props after saving nested objects
        /// This ensures that correct link IDs are saved in _values
        /// </summary>
        private async Task SynchronizeNestedObjectIds<TProps>(IRedbObject<TProps> obj) where TProps : class, new()
        {
            if (obj.Props == null) return;
            
            await SynchronizeNestedIdsInProperties(obj.Props);
        }
        
        /// <summary>
        /// 🔄 Recursive ID synchronization in all object Props
        /// </summary>
        private async Task SynchronizeNestedIdsInProperties(object properties)
        {
            if (properties == null) return;
            
            var propertiesType = properties.GetType();
            var allProperties = propertiesType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            
            foreach (var prop in allProperties)
            {
                // Skip indexers (e.g. Dictionary<K,V>.Item[key])
                if (prop.GetIndexParameters().Length > 0) continue;
                
                try 
                {
                    var value = prop.GetValue(properties);
                    if (value == null) continue;
                    
                    var valueType = value.GetType();
                    
                    // 🔍 Single RedbObject - check if ID is up to date
                    if (IsRedbObjectType(valueType))
                    {
                        var redbObj = (IRedbObject)value;
                    }
                    // 🔍 Array of RedbObjects
                    else if (valueType.IsArray || (valueType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(valueType) && !IsDictionaryWithRedbObjectValue(valueType)))
                    {
                        if (value is IEnumerable enumerable)
                        {
                            var index = 0;
                            foreach (var item in enumerable)
                            {
                                if (item != null && IsRedbObjectType(item.GetType()))
                                {
                                    var redbObj = (IRedbObject)item;

                                    index++;
                                }
                            }
                        }
                    }
                    // 🔍 Dictionary<K, RedbObject<T>>
                    else if (IsDictionaryWithRedbObjectValue(valueType))
                    {
                        var valuesProperty = valueType.GetProperty("Values");
                        if (valuesProperty != null)
                        {
                            var dictValues = (IEnumerable)valuesProperty.GetValue(value)!;
                            foreach (var item in dictValues)
                            {
                                if (item != null && IsRedbObjectType(item.GetType()))
                                {
                                    var redbObj = (IRedbObject)item;
                                    // ID already synchronized after save
                                    
                                    // Recursion for Props of the nested RedbObject
                                    var nestedProps = GetPropertiesFromRedbObject(redbObj);
                                    if (nestedProps != null)
                                    {
                                        await SynchronizeNestedIdsInProperties(nestedProps);
                                    }
                                }
                            }
                        }
                    }
                    // 🔍 Recursive synchronization in business classes
                    else if (IsBusinessClassType(valueType))
                    {
                        await SynchronizeNestedIdsInProperties(value);
                    }
                    // 🔍 Business class arrays
                    else if (valueType.IsArray && IsBusinessClassType(valueType.GetElementType()!))
                    {
                        if (value is IEnumerable enumerable)
                        {
                            foreach (var item in enumerable)
                            {
                                if (item != null)
                                {
                                    await SynchronizeNestedIdsInProperties(item);
                                }
                            }
                        }
                    }
                }
                catch
                {
                    throw;
                }
            }
        }
        
        /// <summary>
        /// 🗑️ SMART BULK DELETE: Deletes values of the main object, excluding nested RedbObjects
        /// One SQL query instead of many EF operations for maximum performance
        /// </summary>
        private async Task BulkDeleteExistingValues(long objectId)
        {
            await _context.ExecuteAsync(Sql.ObjectStorage_DeleteValuesByObjectId(), objectId);
        }
        
        /// <summary>
        /// 🏢 Ensures saving/updating the main object in the _objects table
        /// </summary>
        private async Task EnsureMainObjectSaved<TProps>(IRedbObject<TProps> obj, IRedbUser user) where TProps : class, new()
        {
            var now = DateTimeOffset.Now;
            
            if (obj.Id == 0)
            {
                // 🆕 NEW OBJECT - create record in _objects
                var newId = await _context.Keys.NextObjectIdAsync();
                var schemeId = (await _schemeSync.SyncSchemeAsync<TProps>()).Id;
                var ownerId = obj.OwnerId > 0 ? obj.OwnerId : user.Id;
                var parentId = obj.ParentId == 0 ? (long?)null : obj.ParentId;
                
                // Use object dates if set, otherwise use now
                var dateCreate = obj.DateCreate != default ? obj.DateCreate : now;
                var dateModify = obj.DateModify != default ? obj.DateModify : now;
                
                await _context.ExecuteAsync(Sql.ObjectStorage_InsertObject(),
                    newId, schemeId, obj.Name ?? "", obj.Note, dateCreate, dateModify, ownerId, user.Id, parentId, obj.Hash,
                    obj.ValueString, obj.ValueLong, obj.ValueGuid, obj.ValueBool,
                    obj.ValueDouble, obj.ValueNumeric, obj.ValueDatetime, obj.ValueBytes,
                    obj.Key, obj.DateBegin, obj.DateComplete);
                
                obj.Id = newId;
            }
            else
            {
                // 🔄 EXISTING OBJECT - check and update
                var existsInDb = await _context.ExecuteScalarAsync<long?>(Sql.ObjectStorage_CheckObjectExists(), obj.Id);
                
                if (existsInDb == null)
                {
                    // ✅ MISSING OBJECT PROCESSING STRATEGY
                    if (_configuration.MissingObjectStrategy == MissingObjectStrategy.AutoSwitchToInsert)
                    {
                        var schemeId = (await _schemeSync.SyncSchemeAsync<TProps>()).Id;
                        var ownerId = obj.OwnerId > 0 ? obj.OwnerId : user.Id;
                        var parentId = obj.ParentId == 0 ? (long?)null : obj.ParentId;
                        
                        // Use object dates if set, otherwise use now
                        var dateCreate = obj.DateCreate != default ? obj.DateCreate : now;
                        var dateModify = obj.DateModify != default ? obj.DateModify : now;
                        
                        await _context.ExecuteAsync(Sql.ObjectStorage_InsertObject(),
                            obj.Id, schemeId, obj.Name ?? "", obj.Note, dateCreate, dateModify, ownerId, user.Id, parentId, obj.Hash,
                            obj.ValueString, obj.ValueLong, obj.ValueGuid, obj.ValueBool,
                            obj.ValueDouble, obj.ValueNumeric, obj.ValueDatetime, obj.ValueBytes,
                            obj.Key, obj.DateBegin, obj.DateComplete);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Object with ID {obj.Id} not found in database");
                    }
                }
                else
                {
                    // Update main fields + value_* fields (dateModify always now on update)
                    await _context.ExecuteAsync(Sql.ObjectStorage_UpdateObject(),
                        obj.Name, obj.Note, now, user.Id, obj.Hash,
                        obj.ValueString, obj.ValueLong, obj.ValueGuid, obj.ValueBool,
                        obj.ValueDouble, obj.ValueNumeric, obj.ValueDatetime, obj.ValueBytes,
                        obj.Key, obj.DateBegin, obj.DateComplete, obj.Id);
                }
            }
        }
        
        /// <summary>
        /// 📋 Preparation of all object values for bulk insert operation
        /// Uses existing tree-based logic for maximum compatibility
        /// </summary>
        private async Task PrepareAllValuesForInsert<TProps>(IRedbObject<TProps> obj, List<RedbValue> valuesList) where TProps : class, new()
        {
            // 🎯 USE EXISTING LOGIC: ProcessPropertiesWithTreeStructures
            // This ensures full compatibility with the current tree-based architecture
            
            // Get schema and structure tree (🚀 from cache without hash validation)
            var scheme = await GetSchemeFromCacheOrDbAsync(obj.SchemeId);
            if (scheme == null)
            {
                scheme = await _schemeSync.SyncSchemeAsync<TProps>();
                obj.SchemeId = scheme.Id;
            }
            
            var schemeProvider = (SchemeSyncProviderBase)_schemeSync;
            var structureNodes = await schemeProvider.GetSubtreeAsync(obj.SchemeId, null);
            
            // Clear list for clean insert
            valuesList.Clear();
            
            // Process all properties and collect values
            await ProcessPropertiesWithTreeStructures(obj, structureNodes, valuesList, new List<IRedbObject>());
            
            // 🔧 FIX OBJECT REFERENCES: all values must point to the correct object
            foreach (var value in valuesList)
            {
                if (value.IdObject == 0)
                {
                    value.IdObject = obj.Id;
                }
            }
        }

        #endregion

        #region CACHE UPDATE

        /// <summary>
        /// Recursive cache update for all nested RedbObjects (saving instead of deleting)
        /// </summary>
        // ❌ DELETED: UpdateCacheForNestedObjects - replaced by CacheNestedObjects from ObjectStorageProviderBase.cs
        // Now a single method is used for caching nested objects in both LoadAsync and SaveAsync

        #endregion
    }
}
