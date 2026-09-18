using redb.Core.Providers;
using redb.Core.Data;
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Threading.Tasks;
using System;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// NEW SaveAsync - correct architecture with recursive processing
    /// </summary>
    public abstract partial class ObjectStorageProviderBase
    {
        /// <summary>
        /// STEP 2: Recursive collection of all IRedbObject (main + nested).
        /// Uses <paramref name="processed"/> to deduplicate objects that are referenced
        /// multiple times in the object tree (e.g. two licenses referencing the same Plan).
        /// </summary>
        protected async Task CollectAllObjectsRecursively(IRedbObject rootObject, List<IRedbObject> collector, HashSet<long> processed, HashSet<object> seen, List<IRedbObject>? referenceStubs = null)
        {
            // One instance is one object: a new object (id 0, nothing to dedup by) referenced from two places was
            // collected twice, given an id twice and inserted twice (review after 4.0.0).
            if (!seen.Add(rootObject))
                return;
            collector.Add(rootObject);
            ValidateValueUnique(rootObject);

            // Track root object ID to prevent re-adding if referenced elsewhere in the tree.
            // New objects (Id == 0) get IDs assigned later and are deduplicated by instance only.
            if (rootObject.Id != 0)
                processed.Add(rootObject.Id);

            var rootProperties = GetPropertiesFromRedbObject(rootObject);
            await CollectNestedRedbObjectsFromProperties(rootProperties, collector, processed, seen, rootObject.Id, referenceStubs);
        }

        /// <summary>
        /// Recursive search for IRedbObject in object Props
        /// </summary>
        private async Task CollectNestedRedbObjectsFromProperties(object? properties, List<IRedbObject> collector, HashSet<long> processed, HashSet<object> seen, long parentId, List<IRedbObject>? referenceStubs)
        {
            if (properties == null) return;

            var propsType = properties.GetType();
            var propsProperties = propsType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var property in propsProperties)
            {
                // Skip technical properties with [JsonIgnore] or [RedbIgnore]
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

                    // A reference (id, no properties) is written by the parent as its id and is not
                    // an object to save. Before the check the stub went into the collector and the
                    // parent's save rewrote the referenced object as an empty one: values deleted,
                    // name reset, hash cleared. Checked before the dedup so that a loaded copy of the
                    // same object elsewhere in the graph is still collected.
                    if (IsUnloadedReference(redbObj))
                        {
                            // V4 (L.2): a hand-made reference by id has no hash yet; the caller resolves it in one
                            // query so the parent hashes "id:hash" exactly as a loaded stub carries it.
                            if (referenceStubs != null && redbObj.Hash == null && redbObj.Id > 0) referenceStubs.Add(redbObj);
                            continue;
                        }

                    // Dedup: skip objects already collected (same RedbObject referenced
                    // from multiple properties, e.g. two LicenseInfo entries pointing
                    // to the same Plan). New objects (Id == 0) always collected.
                    if (!seen.Add(redbObj) || (redbObj.Id != 0 && !processed.Add(redbObj.Id)))
                        continue;

                    collector.Add(redbObj);
                    ValidateValueUnique(redbObj);

                    var nestedProperties = GetPropertiesFromRedbObject(redbObj);
                    await CollectNestedRedbObjectsFromProperties(nestedProperties, collector, processed, seen, redbObj.Id, referenceStubs);
                }
                // IRedbObject array
                else if (value is IEnumerable enumerable && IsRedbObjectArrayType(value.GetType()))
                {
                    foreach (var item in enumerable)
                    {
                        if (item != null && IsRedbObjectType(item.GetType()))
                        {
                            var redbObj = (IRedbObject)item;

                            if (IsUnloadedReference(redbObj))
                                {
                                    // V4 (L.2): a hand-made reference by id has no hash yet; the caller resolves it in one
                                    // query so the parent hashes "id:hash" exactly as a loaded stub carries it.
                                    if (referenceStubs != null && redbObj.Hash == null && redbObj.Id > 0) referenceStubs.Add(redbObj);
                                    continue;
                                }

                            // Dedup: same object referenced from multiple array elements
                            if (!seen.Add(redbObj) || (redbObj.Id != 0 && !processed.Add(redbObj.Id)))
                                continue;

                            collector.Add(redbObj);
                            ValidateValueUnique(redbObj);

                            var arrayElementProperties = GetPropertiesFromRedbObject(redbObj);
                            await CollectNestedRedbObjectsFromProperties(arrayElementProperties, collector, processed, seen, redbObj.Id, referenceStubs);
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

                                if (IsUnloadedReference(redbObj))
                                    {
                                        // V4 (L.2): a hand-made reference by id has no hash yet; the caller resolves it in one
                                        // query so the parent hashes "id:hash" exactly as a loaded stub carries it.
                                        if (referenceStubs != null && redbObj.Hash == null && redbObj.Id > 0) referenceStubs.Add(redbObj);
                                        continue;
                                    }

                                // Dedup: same object referenced from multiple dict entries
                                if (!seen.Add(redbObj) || (redbObj.Id != 0 && !processed.Add(redbObj.Id)))
                                    continue;

                                collector.Add(redbObj);
                                ValidateValueUnique(redbObj);

                                var nestedProperties = GetPropertiesFromRedbObject(redbObj);
                                await CollectNestedRedbObjectsFromProperties(nestedProperties, collector, processed, seen, redbObj.Id, referenceStubs);
                            }
                        }
                    }
                }
                // Recursion into business classes
                else if (IsBusinessClassType(value.GetType()))
                {
                    await CollectNestedRedbObjectsFromProperties(value, collector, processed, seen, parentId, referenceStubs);
                }
                // Recursion into business class arrays
                else if (value is IEnumerable businessEnumerable && !IsStringType(value.GetType()))
                {
                    foreach (var item in businessEnumerable)
                    {
                        if (item != null && IsBusinessClassType(item.GetType()))
                        {
                            await CollectNestedRedbObjectsFromProperties(item, collector, processed, seen, parentId, referenceStubs);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Hashes every collected object after the objects it references. A parent hashes "id:hash" of every
        /// reference, so a reference hashed after its parent leaves the parent with the reference's stale or empty
        /// hash - stored for ever, a props-cache miss on every load. The collector is pre-order and collects a
        /// target shared by two parents once, under the first: reversing its order put the second parent before
        /// the target (review after 4.0.0). Post-order over the graph, each instance once.
        /// </summary>
        private static void RecomputeHashesPostOrder(IEnumerable<IRedbObject> objects)
        {
            var done = new HashSet<object>(ReferenceEqualityComparer.Instance);
            foreach (var obj in objects)
                RecomputeHashDeep(obj, done);
        }

        private static void RecomputeHashDeep(IRedbObject obj, HashSet<object> done)
        {
            if (!done.Add(obj))
                return;
            ForEachDirectReference(GetPropertiesFromRedbObject(obj), child =>
            {
                // A stub carries the hash the parent needs; there is nothing loaded to hash.
                if (!IsUnloadedReference(child))
                    RecomputeHashDeep(child, done);
            });
            RecomputeHash(obj);
        }

        /// <summary>
        /// The objects a Props instance references directly - single, in a collection, as dictionary values, and
        /// through nested business classes - the walk of <see cref="CollectNestedRedbObjectsFromProperties"/>.
        /// </summary>
        private static void ForEachDirectReference(object? properties, Action<IRedbObject> visit)
        {
            if (properties == null) return;

            foreach (var property in properties.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (property.ShouldIgnoreForRedb() || property.GetIndexParameters().Length > 0)
                    continue;
                var value = property.GetValue(properties);
                if (value == null) continue;

                if (IsRedbObjectType(value.GetType()))
                    visit((IRedbObject)value);
                else if (value is IEnumerable enumerable && IsRedbObjectArrayType(value.GetType()))
                {
                    foreach (var item in enumerable)
                        if (item != null && IsRedbObjectType(item.GetType()))
                            visit((IRedbObject)item);
                }
                else if (IsDictionaryWithRedbObjectValue(value.GetType()))
                {
                    if (value.GetType().GetProperty("Values")?.GetValue(value) is IEnumerable values)
                        foreach (var item in values)
                            if (item != null && IsRedbObjectType(item.GetType()))
                                visit((IRedbObject)item);
                }
                else if (IsBusinessClassType(value.GetType()))
                    ForEachDirectReference(value, visit);
                else if (value is IEnumerable businessEnumerable && !IsStringType(value.GetType()))
                {
                    foreach (var item in businessEnumerable)
                        if (item != null && IsBusinessClassType(item.GetType()))
                            ForEachDirectReference(item, visit);
                }
            }
        }

        /// <summary>
        /// An object with an id whose properties were never loaded: a stub from the depth boundary
        /// of a load or a hand-written <c>new RedbObject&lt;T&gt; { id = x }</c>. It names another
        /// object; it is not that object. Reads the flag, never <c>Props</c> — the getter would
        /// trigger lazy loading.
        /// </summary>
        private static bool IsUnloadedReference(IRedbObject obj)
            => obj.Id != 0 && obj is RedbObject typed && !typed.IsPropsLoaded;

        /// <summary>
        /// A reference whose properties were never loaded names another object; saving it would save whatever a fresh
        /// load returns, and the caller's edits - made on an instance the getter did not keep (a shared instance inside a
        /// transaction) - would be lost silently (review after 4.0.0). The collector skips such references inside a
        /// graph; saved directly, they are refused.
        /// </summary>
        private static void RefuseUnloadedReference(IRedbObject obj)
        {
            // A stub redb attached a lazy loader to (a depth boundary, a cache), still unloaded. An object without a
            // loader and without loaded Props is the caller's own - a scheme without properties, Props set to null - and
            // is saved as it is.
            if (IsUnloadedReference(obj) && obj.GetType().GetField("_lazyLoader")?.GetValue(obj) != null)
                throw new Exceptions.RedbUnloadedReferenceException(obj.Id, obj.SchemeId);
        }

        /// <summary>
        /// The 440-character limit of _objects._value_unique, enforced in C# so every provider
        /// behaves identically: SQLite does not check VARCHAR lengths at all, and a key accepted
        /// there would be rejected by PostgreSQL and MSSQL (UNIQUE plan §4.5).
        /// </summary>
        private static void ValidateValueUnique(IRedbObject obj)
        {
            if (obj.ValueUnique is { Length: > 440 })
                throw new Exceptions.RedbUniqueKeyValueException(
                    $"object id={obj.Id}: _value_unique is {obj.ValueUnique.Length} characters, the limit is 440");

            // Owner decision 2026-09-02: _value_string is an identifier column - 450 is the MSSQL
            // index-key width, enforced here so the text-typed PostgreSQL and SQLite columns hold
            // the same contract. Long text belongs in _note or in a Props field.
            if (obj.ValueString is { Length: > 450 })
                throw new Exceptions.RedbValueStringTooLongException(obj.Id, obj.ValueString.Length);
        }

        // Answered once per type: every predicate below reflects over the interfaces of the type, and the save asked
        // them for every property of every object (review after 4.0.0).
        private static readonly ConcurrentDictionary<Type, bool> RedbObjectTypeByType = new();
        private static readonly ConcurrentDictionary<Type, bool> RedbObjectArrayTypeByType = new();
        private static readonly ConcurrentDictionary<Type, bool> DictionaryWithRedbObjectValueByType = new();
        private static readonly ConcurrentDictionary<Type, bool> BusinessClassTypeByType = new();

        /// <summary>
        /// IRedbObject type check
        /// </summary>
        private static bool IsRedbObjectType(Type type)
            => RedbObjectTypeByType.GetOrAdd(type, static t => IsRedbObjectTypeCore(t));

        private static bool IsRedbObjectTypeCore(Type type)
        {
            // RedbObject<T> itself or anything derived from it (TreeRedbObject<T>)
            if (Utils.RedbObjectTypes.GenericOf(type) != null)
                return true;

            // Check interfaces for IRedbObject<T>
            return type.GetInterfaces().Any(i =>
                i.IsGenericType &&
                i.GetGenericTypeDefinition().Name.Contains("IRedbObject"));
        }

        /// <summary>
        /// A collection of IRedbObject: an array, or any generic IEnumerable&lt;T&gt; (List, IList, ...) whose element
        /// is one. Only arrays used to count (props cache review, 2026-09-15): the collector skipped the elements of a
        /// List - references by id got no hash, so the parent never matched its loaded hash; a new object was never
        /// saved (foreign key failure); an edit to a loaded one was lost.
        /// </summary>
        private static bool IsRedbObjectArrayType(Type type)
            => RedbObjectArrayTypeByType.GetOrAdd(type, static t => IsRedbObjectArrayTypeCore(t));

        private static bool IsRedbObjectArrayTypeCore(Type type)
        {
            if (type.IsArray)
                return IsRedbObjectType(type.GetElementType()!);
            if (type == typeof(string))
                return false;
            var sequence = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                ? type
                : type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            return sequence != null && IsRedbObjectType(sequence.GetGenericArguments()[0]);
        }

        /// <summary>
        /// Checking Dictionary with IRedbObject values
        /// </summary>
        private static bool IsDictionaryWithRedbObjectValue(Type type)
            => DictionaryWithRedbObjectValueByType.GetOrAdd(type, static t => IsDictionaryWithRedbObjectValueCore(t));

        private static bool IsDictionaryWithRedbObjectValueCore(Type type)
        {
            if (!type.IsGenericType) return false;
            var genericDef = type.GetGenericTypeDefinition();
            if (genericDef != typeof(Dictionary<,>) && genericDef != typeof(IDictionary<,>)) return false;

            var valueType = type.GetGenericArguments()[1];
            return IsRedbObjectType(valueType);
        }

        /// <summary>
        /// Checking string type
        /// </summary>
        private static bool IsStringType(Type type)
        {
            return type == typeof(string);
        }

        /// <summary>
        /// Getting Props from IRedbObject via reflection
        /// </summary>
        private static object? GetPropertiesFromRedbObject(IRedbObject redbObj)
        {
            // Use reflection to get the Props property
            var propertiesProperty = redbObj.GetType().GetProperty("Props");
            return propertiesProperty?.GetValue(redbObj);
        }

        /// <summary>
        /// Getting Props type from IRedbObject
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
        /// Checking if structure is Class type (business class)
        /// </summary>
        private async Task<bool> IsClassTypeStructure(IRedbStructure structure)
        {
            await EnsureTypesCacheLoaded();
            var type = Cache.GetTypeById(structure.IdType);
            return type?.Type1 == "Object" || type?.Name == "Class";
        }

        /// <summary>
        /// Checking if structure is IRedbObject reference
        /// </summary>
        private async Task<bool> IsRedbObjectStructure(IRedbStructure structure)
        {
            await EnsureTypesCacheLoaded();
            var type = Cache.GetTypeById(structure.IdType);
            return type?.Type1 == "RedbObjectRow" || type?.Name == "Object";
        }

        /// <summary>
        /// Checking if structure is ListItem reference
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
        /// STEP 3: Assigning ID via GetNextKey() to all objects without ID
        /// </summary>
        protected async Task AssignMissingIds(List<IRedbObject> objects, IRedbUser user, CancellationToken cancellationToken = default)
        {
            // BATCH: Get all needed IDs in one DB call instead of N calls
            var objectsNeedingIds = objects.Where(o => o.Id == 0).ToList();
            if (objectsNeedingIds.Count > 0)
            {
                var newIds = await _context.Keys.NextObjectIdBatchAsync(objectsNeedingIds.Count, cancellationToken);
                for (int i = 0; i < objectsNeedingIds.Count; i++)
                {
                    var obj = objectsNeedingIds[i];
                    obj.Id = newIds[i];

                    // Apply audit settings
                    obj.OwnerId = user.Id;
                    obj.WhoChangeId = user.Id;

                    if (_configuration.AutoSetModifyDate)
                    {
                        obj.DateCreate = DateTimeOffset.UtcNow;
                        obj.DateModify = DateTimeOffset.UtcNow;
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
        /// STEP 4: Creating/verifying schemas for all object types (using PostgresSchemeSyncProvider)
        /// </summary>
        protected async Task EnsureSchemesForAllTypes(List<IRedbObject> objects, CancellationToken cancellationToken = default)
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
        /// STEP 5: Recursive processing of Props of all objects → RedbValue lists
        /// NEW ARCHITECTURE: Uses a structure tree instead of a flat list!
        /// Skips non-generic RedbObject and RedbObject{TProps} with Props=null (0 records in _values)
        /// </summary>
        protected async Task ProcessAllObjectsPropertiesRecursively(List<IRedbObject> objects, List<RedbValue> valuesList, ISet<long>? skipUnchangedIds = null, CancellationToken cancellationToken = default)
        {
            foreach (var obj in objects)
            {
                // F1 (CT hash shortcut): an existing object whose recomputed content hash equals
                // the persisted one produces no value records at all - the CT diff never sees it,
                // so its rows stay untouched. The set is only supplied under ChangeTracking with
                // AutoRecomputeHash on (hash = content is the props-cache invariant).
                if (skipUnchangedIds != null && obj.Id > 0 && skipUnchangedIds.Contains(obj.Id))
                    continue;

                // Skip non-generic RedbObject (Object scheme) - no _values records
                var objType = obj.GetType();
                if (objType == typeof(RedbObject))
                {
                    continue;
                }

                // Skip RedbObject<TProps> (or a TreeRedbObject<TProps>) with Props=null - no _values records
                if (Utils.RedbObjectTypes.GenericOf(objType) != null)
                {
                    var propsProperty = objType.GetProperty("Props");
                    var propsValue = propsProperty?.GetValue(obj);
                    if (propsValue == null)
                    {
                        continue;
                    }
                }

                // Check that the schema exists (from cache without hash validation)
                var scheme = await GetSchemeFromCacheOrDbAsync(obj.SchemeId);
                if (scheme == null)
                {
                    throw new InvalidOperationException(
                        $"Scheme with Id={obj.SchemeId} not found for object Id={obj.Id}.");
                }

                // NEW LOGIC: Get the structure tree instead of a flat list
                var schemeProvider = (SchemeSyncProviderBase)_schemeSync;
                var rootStructureTree = await schemeProvider.GetSubtreeAsync(obj.SchemeId, null); // root nodes


                if (rootStructureTree.Count == 0)
                {

                    try
                    {
                        // Create structures via a universal method
                        var propsType = obj.GetType().GetGenericArguments()[0];
                        await SyncStructuresForType(scheme, propsType, cancellationToken);

                        // Get the structure tree again
                        schemeProvider.InvalidateStructureTreeCache(obj.SchemeId); // clear cache
                        rootStructureTree = await schemeProvider.GetSubtreeAsync(obj.SchemeId, null);

                    }
                    catch
                    {
                        throw;
                    }
                }

                // NEW TRAVERSAL: Via structure tree with subtrees!
                var valuesCountBefore = valuesList.Count;
                await ProcessPropertiesWithTreeStructures(obj, rootStructureTree, valuesList, objects);
                var valuesGenerated = valuesList.Count - valuesCountBefore;
            }
        }

        /// <summary>
        /// NEW METHOD: Props processing via structure tree
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
                    continue; // SOLVES THE REDUNDANT STRUCTURES PROBLEM!
                }

                // Get the property value
                var objProperties = GetPropertiesFromRedbObject(obj);
                var rawValue = property.GetValue(objProperties);

                // NULL SEMANTICS
                if (!ObjectStorageProviderExtensions.ShouldCreateValueRecord(rawValue, structureNode.Structure.StoreNull ?? false))
                {
                    continue;
                }

                // CRITICAL DISPATCH BY TYPES
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
        /// Check if the type is a business class (not a primitive or an array)
        /// </summary>
        private static bool IsBusinessClassType(Type type)
            => BusinessClassTypeByType.GetOrAdd(type, static t => IsBusinessClassTypeCore(t));

        private static bool IsBusinessClassTypeCore(Type type)
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

            // Task and Task<> are not business classes (technical types for async/await)
            if (type == typeof(Task) || (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Task<>)))
                return false;

            // IRedbListItem and RedbListItem are not business classes (dictionary elements, processed separately)
            if (type == typeof(IRedbListItem) || type == typeof(RedbListItem) || type.GetInterfaces().Contains(typeof(IRedbListItem)))
                return false;

            // Other classes are business classes
            return type.IsClass;
        }

        /// <summary>
        /// Universal method for creating structures for any type via reflection
        /// </summary>
        private async Task SyncStructuresForType(IRedbScheme scheme, Type propsType, CancellationToken cancellationToken = default)
        {
            // Use reflection to call the generic method SyncStructuresFromTypeAsync<TProps>
            var method = typeof(SchemeSyncProviderBase)
                .GetMethod("SyncStructuresFromTypeAsync", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);

            if (method != null)
            {
                var genericMethod = method.MakeGenericMethod(propsType);
                var result = await (Task<List<IRedbStructure>>)genericMethod.Invoke(_schemeSync, new object[] { scheme, true , cancellationToken })!;

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

        // ===== NEW METHODS FOR WORKING WITH THE STRUCTURE TREE =====

        /// <summary>
        /// Simple field with structure tree support
        /// </summary>
        private async Task ProcessSimpleFieldWithTree(IRedbObject obj, StructureTreeNode structureNode, object? rawValue, List<RedbValue> valuesList)
        {
            var dbTypeForLog = await GetStructureDbType(structureNode.Structure);

            // Process nested objects (IRedbObject, IRedbListItem) before saving
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
            // A [RedbUnique] field carries the hash of its canonical value; the database enforces
            // uniqueness over it. NULL values get no key (never take part in uniqueness).
            if (structureNode.Structure.Unique == true)
                valueRecord.Unique = Utils.UniqueKeyEncoder.Compute(valueRecord);
            valuesList.Add(valueRecord);
        }

        /// <summary>
        /// Array with structure subtree
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


            // Create a base array record for both strategies
            var baseArrayRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = arrayStructureNode.Structure.Id,
                ArrayParentId = parentValueId
                // Guid will be set AFTER processing elements!
            };
            valuesList.Add(baseArrayRecord);


            // Processing of array elements with correct subtree
            await ProcessArrayElementsWithSubtree(obj, arrayStructureNode, baseArrayRecord.Id, enumerable, valuesList, objectsToSave);

            // FINAL HASH: compute base array record hash from element hashes
            await ComputeArrayHashFromElements(baseArrayRecord, arrayStructureNode, valuesList, rawValue);
        }

        /// <summary>
        /// Save Dictionary field with base record + elements via _array_parent_id + _array_index (serialized key)
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
                ArrayParentId = parentValueId,  // FIX: For nested Dictionary, link to parent Class record
                Guid = dictHash  // Hash of entire Dictionary
            };
            valuesList.Add(baseDictRecord);

            // Get DbType for value type - check if it's a class type
            var isClassValue = await IsClassTypeStructure(dictStructureNode.Structure);
            var dbType = await GetStructureDbType(dictStructureNode.Structure);

            // Iterate dictionary entries
            var storedEntries = 0;
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
                storedEntries++;

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
                    elementRecord.Guid = redbObj.Hash; // V4 (L.2): the reference rides its own persisted hash
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

                ApplyElementKey(dictStructureNode.Structure, elementRecord, baseDictRecord.Id);
            }

            // S1: a subtree key - mirror the canonical content hash into the key. An EMPTY
            // dictionary claims no key, like an absent one. Element-scoped collections (S3) key
            // their ELEMENTS instead - the base row stays out of the index.
            if (dictStructureNode.Structure.Unique == true && dictStructureNode.Structure.UniqueScope is null && storedEntries > 0)
                baseDictRecord.Unique = Utils.UniqueKeyEncoder.Compute(baseDictRecord);
        }

        /// <summary>
        /// Array elements with structure subtree
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
                    ArrayParentId = actualParentId, // Use found or created ID
                    ArrayIndex = index.ToString()
                };

                if (item != null)
                {
                    var itemType = item.GetType();

                    // RECURSION WITH SUBTREES: different types of elements
                    if (await IsRedbObjectStructure(arrayStructureNode.Structure))
                    {
                        // IRedbObject array element - BULK STRATEGY: take ID directly
                        var redbObj = (IRedbObject)item;
                        var objectId = redbObj.Id;
                        var objHash = redbObj.Hash; // V4 (L.2)

                        elementRecord.Object = objectId;  // _Object - special field for FK on _objects
                        elementRecord.Guid = objHash;  // RedbObject HASH for ChangeTracking!
                        valuesList.Add(elementRecord);

                    }
                    else if (await IsListItemStructure(arrayStructureNode.Structure))
                    {

                        // ListItem array element - take ID directly (similarly to IRedbObject)
                        var listItem = (IRedbListItem)item;
                        var listItemId = listItem.Id;

                        elementRecord.ListItem = listItemId;  // Write to the ListItem field
                        // O-1/B-3: the id already lives in _listitem; the element needs no separate content
                        // hash (the scalar and dictionary forms never wrote one). ListItem content lives
                        // its own life.
                        valuesList.Add(elementRecord);

                    }
                    else if (IsBusinessClassType(itemType))
                    {
                        // Business class array element
                        var itemHash = RedbHash.ComputeForProps(item);
                        elementRecord.Guid = itemHash;
                        valuesList.Add(elementRecord);

                        // RECURSION: processing child fields with subtree
                        await ProcessBusinessClassChildrenWithSubtree(obj, elementRecord.Id, item, arrayStructureNode.Children, valuesList, objectsToSave, index);
                    }
                    else
                    {

                        // Simple array element
                        var elementDbType = await GetStructureDbType(arrayStructureNode.Structure);


                        // CRITICAL: Calling ProcessNestedObjectsAsync to extract ID from IRedbListItem!
                        var processedValue = await ProcessNestedObjectsAsync(item, elementDbType, false, obj.Id);


                        SetSimpleValueByType(elementRecord, elementDbType, processedValue);


                        valuesList.Add(elementRecord);
                    }
                }
                else
                {
                    valuesList.Add(elementRecord); // null element
                }

                ApplyElementKey(arrayStructureNode.Structure, elementRecord, actualParentId);
                index++;
            }

        }

        /// <summary>
        /// S3: keys an ELEMENT row of a collection whose structure declares an element scope.
        /// The Collection scope salts the canon with the owning collection id, so only true
        /// in-collection duplicates collide; the Scheme scope keys the bare canon. References
        /// canonicalise by target id (the encoder checks _Object/_ListItem first), scalars by
        /// their typed column, class elements by their content hash. Rows of unkeyed or
        /// subtree-keyed collections, and rows with no canon (null elements), pass untouched.
        /// </summary>
        private static void ApplyElementKey(IRedbStructure structure, RedbValue elementRecord, long collectionId)
        {
            if (structure.Unique != true || structure.UniqueScope is null)
                return;
            var salt = structure.UniqueScope == (long)Core.Attributes.UniqueScope.Collection
                ? collectionId
                : (long?)null;
            elementRecord.Unique = Utils.UniqueKeyEncoder.Compute(elementRecord, salt);
        }

        /// <summary>
        /// Computing base array record hash from element hashes
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
                // For RedbObject[] and ListItem[] - collect element hashes
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
                // For business class and primitive arrays - standard hash
                baseArrayRecord.Guid = RedbHash.ComputeForProps(rawValue);
            }

            // S1: a subtree key - mirror the canonical content hash into the key. An EMPTY
            // array claims no key, like an absent one: "no content" must not collide between
            // objects. Element-scoped collections (S3) key their ELEMENTS instead.
            if (arrayStructureNode.Structure.Unique == true
                && arrayStructureNode.Structure.UniqueScope is null
                && valuesList.Any(v => v.ArrayParentId == baseArrayRecord.Id))
                baseArrayRecord.Unique = Utils.UniqueKeyEncoder.Compute(baseArrayRecord);
        }

        /// <summary>
        /// Business class with structure subtree
        /// FIX: Added parentValueId for nested classes!
        /// </summary>
        private async Task ProcessBusinessClassWithSubtree(IRedbObject obj, StructureTreeNode classStructureNode, object? rawValue, List<RedbValue> valuesList, List<IRedbObject> objectsToSave, long? parentValueId = null)
        {

            if (rawValue == null)
            {
                return;
            }

            // Compute UUID hash of the business class
            var classHash = RedbHash.ComputeForProps(rawValue);

            // Create base Class field record with hash in _Guid
            // FIX: Set ArrayParentId for nested classes!
            var classRecord = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = classStructureNode.Structure.Id,
                ArrayParentId = parentValueId,  // FIX: Link to the parent class!
                Guid = classHash
            };

            // S1: a subtree key - the canonical content hash IS the value; encode it exactly
            // like a scalar Guid field so save, recompute and the GetByUnique probe share one
            // formula. (A class never carries an element scope - the validator rejects it.)
            if (classStructureNode.Structure.Unique == true && classStructureNode.Structure.UniqueScope is null)
                classRecord.Unique = Utils.UniqueKeyEncoder.Compute(classRecord);

            valuesList.Add(classRecord);

            // Process child fields with the correct subtree!
            await ProcessBusinessClassChildrenWithSubtree(obj, classRecord.Id, rawValue, classStructureNode.Children, valuesList, objectsToSave);
        }

        /// <summary>
        /// Recursive processing of child fields of a business class with subtree
        /// </summary>
        private async Task ProcessBusinessClassChildrenWithSubtree(IRedbObject obj, long parentValueId, object businessObject, List<StructureTreeNode> childrenSubtree, List<RedbValue> valuesList, List<IRedbObject> objectsToSave, int? parentArrayIndex = null)
        {
            var businessType = businessObject.GetType();

            foreach (var childStructureNode in childrenSubtree)
            {
                // PROPERTY EXISTENCE CHECK IN C# CLASS
                var property = businessType.GetProperty(childStructureNode.Structure.Name);
                if (property == null || property.GetIndexParameters().Length > 0)
                {
                    continue;
                }

                var childValue = property.GetValue(businessObject);

                // NULL SEMANTICS
                if (!ObjectStorageProviderExtensions.ShouldCreateValueRecord(childValue, childStructureNode.Structure.StoreNull ?? false))
                {
                    continue;
                }

                // RECURSIVE PROCESSING with correct subtrees
                if (childStructureNode.Structure.CollectionType == Core.Utils.RedbTypeIds.Dictionary)
                {
                    // FIX: Pass parentValueId for nested Dictionary inside Class
                    await ProcessDictionaryWithSubtree(obj, childStructureNode, childValue, valuesList, objectsToSave, parentValueId);
                }
                else if (childStructureNode.Structure.CollectionType == Core.Utils.RedbTypeIds.Array)
                {
                    // FIX: Pass parentValueId for nested arrays
                    await ProcessArrayWithSubtree(obj, childStructureNode, childValue, valuesList, objectsToSave, parentValueId);
                }
                else if (await IsClassTypeStructure(childStructureNode.Structure))
                {
                    // FIX: Pass parentValueId for nested classes!
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
                        ArrayIndex = parentArrayIndex?.ToString()   // Inherit ArrayIndex if it's an array element
                    };

                    var childDbType = await GetStructureDbType(childStructureNode.Structure);
                    SetSimpleValueByType(childRecord, childDbType, childValue);
                    // S2: a [RedbUnique] nested scalar carries the hash of its canonical value
                    // exactly like a root field - same encoder, same index. The validator only
                    // flags structures on collection-free paths, so this is the object's single
                    // row of the structure.
                    if (childStructureNode.Structure.Unique == true)
                        childRecord.Unique = Utils.UniqueKeyEncoder.Compute(childRecord);
                    valuesList.Add(childRecord);

                }
            }
        }

        /// <summary>
        /// IRedbObject field processing with ID search in the object collector
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
        /// Single IRedbObject with ID search in the collector
        /// </summary>
        private async Task ProcessSingleIRedbObject(IRedbObject obj, IRedbStructure structure, IRedbObject redbObjectValue, List<IRedbObject> objectsToSave, List<RedbValue> valuesList, long? parentValueId = null, int? parentArrayIndex = null)
        {
            // BULK STRATEGY: Take ID directly from the object (already saved recursively)
            var objectId = redbObjectValue.Id;

            var record = new RedbValue
            {
                Id = await _context.Keys.NextObjectIdAsync(),
                IdObject = obj.Id,
                IdStructure = structure.Id,
                Object = objectId,  // FIX: _Object - FK to _objects
                ArrayParentId = parentValueId  // FIX: link to parent element record (unique per array element)
                // ArrayIndex NOT set: parent element record ID is sufficient for disambiguation
            };

            valuesList.Add(record);

        }

        /// <summary>
        /// IRedbObject array with ID search in the collector
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
                ArrayParentId = parentValueId,  // FIX: link to parent element
                Guid = arrayHash
            };
            valuesList.Add(baseArrayRecord);


            // Process array elements
            int index = 0;
            var storedElements = 0;
            foreach (var item in redbObjectArray)
            {
                if (item != null && IsRedbObjectType(item.GetType()))
                {
                    // BULK STRATEGY: Take ID directly from the object (already saved recursively)
                    var redbObj = (IRedbObject)item;
                    var objectId = redbObj.Id;

                    var elementRecord = new RedbValue
                    {
                        Id = await _context.Keys.NextObjectIdAsync(),
                        IdObject = obj.Id,
                        IdStructure = structure.Id,
                        ArrayParentId = baseArrayRecord.Id,
                        ArrayIndex = index.ToString(),
                        Object = objectId  // FIX: _Object - FK to _objects
                    };

                    // S3: an element-scoped reference collection keys each link by target id.
                    ApplyElementKey(structure, elementRecord, baseArrayRecord.Id);
                    valuesList.Add(elementRecord);
                    storedElements++;

                }
                index++;
            }

            // S1: a subtree key on a REFERENCE collection - "unique SET of references". The
            // content hash mirrors into the key exactly like the other collection builders;
            // an EMPTY collection claims no key.
            if (structure.Unique == true && structure.UniqueScope is null && storedElements > 0)
                baseArrayRecord.Unique = Utils.UniqueKeyEncoder.Compute(baseArrayRecord);
        }

        // SetSimpleValueByType already exists in main file ObjectStorageProviderBase.cs

        /// <summary>
        /// Structure with full information for ChangeTracking
        /// </summary>
        public class StructureFullInfo
        {
            public string DbType { get; set; } = "String";
            public bool IsArray { get; set; }
            public bool StoreNull { get; set; }
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

            // The key follows the value: recomputed by the builder for the new value, carried here.
            existingValue.Unique = newValue.Unique;

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
                case "Numeric":  // ADDED
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
                case "Object":  // ADDED
                    existingValue.Object = newValue.Object;
                    break;
                case "ListItem":  // ADDED
                    existingValue.ListItem = newValue.ListItem;
                    break;
            }

            // COLLECTION base row: the structure dbType is the ELEMENT type (Long/Numeric/...),
            // while the meaningful field of the base row is the canonical content hash in _Guid.
            // Without this line a CT element edit copied the element column (null -> null) and
            // left the OLD array hash in the database (CT-2 review, proven by the pin
            // CtSaveInvariantsTestsBase.ArrayBaseHash_AfterEdit_MatchesTheCanonicalHash).
            // For dbType == "Guid" (Guid fields, class nodes) the hash is already copied by the switch branch.
            if (newValue.Guid.HasValue && dbType != "Guid")
                existingValue.Guid = newValue.Guid;
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

        /// <summary>
        /// Deduplicates value insert list by unique constraint key (IdStructure, IdObject, ArrayParentId, ArrayIndex)
        /// to prevent duplicate key violations on UIX__values__structure_object* indexes.
        /// Preserves insertion order, keeps the FIRST entry for each key — because subsequent values
        /// may reference the first value's Id via _array_parent_id FK (self-referencing).
        /// </summary>
        protected List<RedbValue> DeduplicateValueInserts(List<RedbValue> values, string caller)
        {
            if (values.Count <= 1)
                return values;

            var seen = new HashSet<(long, long, long?, string?)>(values.Count);
            var result = new List<RedbValue>(values.Count);

            foreach (var v in values)
            {
                var key = (v.IdStructure, v.IdObject, v.ArrayParentId, v.ArrayIndex);
                if (seen.Add(key))
                    result.Add(v);
            }

            if (result.Count == values.Count)
                return values; // no duplicates — return original

            var duplicateCount = values.Count - result.Count;
            Logger?.LogWarning(
                "REDB {Caller}: {DuplicateCount} duplicate value inserts detected and removed " +
                "(total {Total} -> {Unique}). Duplicates by (IdStructure, IdObject, ArrayParentId, ArrayIndex).",
                caller, duplicateCount, values.Count, result.Count);

            return result;
        }

        #region BULK DELETEINSERT OPTIMIZATION

        #endregion

        #region CACHE UPDATE

        /// <summary>
        /// Recursive cache update for all nested RedbObjects (saving instead of deleting)
        /// </summary>
        // DELETED: UpdateCacheForNestedObjects - replaced by CacheNestedObjects from ObjectStorageProviderBase.cs
        // Now a single method is used for caching nested objects in both LoadAsync and SaveAsync

        #endregion
    }
}
