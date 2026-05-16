using redb.Core.Data;
using redb.Core.Utils;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using System.Text.Json.Serialization;

using System.Reflection;
using System.Collections;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Additional methods for ObjectStorageProviderBase under the new saving paradigm
    /// </summary>
    public abstract partial class ObjectStorageProviderBase
    {
        /// <summary>
        /// Save simple field (primitive type)
        /// </summary>
        private async Task SaveSimpleFieldAsync(long objectId, StructureMetadata structure, object? rawValue)
        {
            var processedValue = await ProcessNestedObjectsAsync(rawValue, structure.DbType, false, objectId);
            var valueRecord = new RedbValue
            {
                Id = await _context.Keys.NextValueIdAsync(),
                IdObject = objectId,
                IdStructure = structure.Id
            };
            SetSimpleValueByType(valueRecord, structure.DbType, processedValue);
            _pendingValuesToInsert.Add(valueRecord);
        }

        /// <summary>
        /// Save Class field with UUID hash in _Guid
        /// </summary>
        private async Task SaveClassFieldAsync(long objectId, StructureMetadata structure, object? rawValue, long schemeId)
        {
            if (rawValue == null) return;

            // ✅ Calculate business class UUID hash
            var classHash = RedbHash.ComputeForProps(rawValue);

            // Create base Class field record with hash in _Guid
            var classRecord = new RedbValue
            {
                Id = await _context.Keys.NextValueIdAsync(),
                IdObject = objectId,
                IdStructure = structure.Id,
                Guid = classHash  // UUID hash in _Guid field
            };
            _pendingValuesToInsert.Add(classRecord);

            // ✅ Save child fields of Class object via _array_parent_id
            await SaveClassChildrenAsync(objectId, classRecord.Id, rawValue, structure.Id, schemeId);
        }

        /// <summary>
        /// Save array with base record (array hash) + elements via _array_parent_id + _array_index
        /// </summary>
        private async Task SaveArrayFieldAsync(long objectId, StructureMetadata structure, object? rawValue, long schemeId)
        {
            if (rawValue == null) return;
            if (rawValue is not IEnumerable enumerable || rawValue is string) return;
            
            // ✅ Create BASE array record with hash of the entire array
            var arrayHash = RedbHash.ComputeForProps(rawValue);
            var baseArrayRecord = new RedbValue
            {
                Id = await _context.Keys.NextValueIdAsync(),
                IdObject = objectId,
                IdStructure = structure.Id,
                Guid = arrayHash  // ✅ Hash of entire array in _Guid
            };
            _pendingValuesToInsert.Add(baseArrayRecord);

            // ✅ Create array elements with _array_parent_id + _array_index
            await SaveArrayElementsAsync(objectId, structure, baseArrayRecord.Id, enumerable, schemeId);
        }
        
        /// <summary>
        /// Save Dictionary field with base record (hash) + elements via _array_parent_id + _array_index (serialized key)
        /// </summary>
        private async Task SaveDictionaryFieldAsync(long objectId, StructureMetadata structure, object? rawValue, long schemeId)
        {
            if (rawValue == null) return;
            
            // Dictionary implements IDictionary, but we need to get key-value pairs
            var dictType = rawValue.GetType();
            if (!dictType.IsGenericType) return;
            
            var genericDef = dictType.GetGenericTypeDefinition();
            if (genericDef != typeof(Dictionary<,>) && genericDef != typeof(IDictionary<,>)) return;
            
            // Create BASE record for Dictionary with hash
            var dictHash = RedbHash.ComputeForProps(rawValue);
            var baseDictRecord = new RedbValue
            {
                Id = await _context.Keys.NextValueIdAsync(),
                IdObject = objectId,
                IdStructure = structure.Id,
                Guid = dictHash  // Hash of entire Dictionary
            };
            _pendingValuesToInsert.Add(baseDictRecord);
            
            // Iterate dictionary entries using reflection
            await SaveDictionaryElementsAsync(objectId, structure, baseDictRecord.Id, rawValue, schemeId);
        }
        
        /// <summary>
        /// Save Dictionary elements with _array_parent_id + _array_index (serialized key)
        /// </summary>
        private async Task SaveDictionaryElementsAsync(long objectId, StructureMetadata structure, long dictParentId, object dictValue, long schemeId)
        {
            var dictType = dictValue.GetType();
            var keyType = dictType.GetGenericArguments()[0];
            
            // Get IEnumerable<KeyValuePair<TKey, TValue>> via reflection
            var enumerableType = typeof(IEnumerable<>).MakeGenericType(
                typeof(KeyValuePair<,>).MakeGenericType(dictType.GetGenericArguments()));
            
            var enumerator = ((System.Collections.IEnumerable)dictValue).GetEnumerator();
            
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
                    Id = await _context.Keys.NextValueIdAsync(),
                    IdObject = objectId,
                    IdStructure = structure.Id,
                    ArrayParentId = dictParentId,      // Link to base Dictionary record
                    ArrayIndex = serializedKey         // Serialized key as string
                };
                
                // If value is null, just add the record with null values
                if (value == null)
                {
                    _pendingValuesToInsert.Add(elementRecord);
                    continue;
                }
                
                // If value is Class type, create hash and save children
                if (ObjectStorageProviderExtensions.IsClassType(structure.TypeSemantic))
                {
                    elementRecord.Guid = RedbHash.ComputeForProps(value);
                    _pendingValuesToInsert.Add(elementRecord);
                    
                    // Save child fields of Class element
                    await SaveClassChildrenAsync(objectId, elementRecord.Id, value, structure.Id, schemeId);
                }
                else
                {
                    // For simple types
                    var processedValue = await ProcessNestedObjectsAsync(value, structure.DbType, false, objectId);
                    SetSimpleValueByType(elementRecord, structure.DbType, processedValue);
                    _pendingValuesToInsert.Add(elementRecord);
                }
            }
        }

        /// <summary>
        /// Save array elements with _array_parent_id + _array_index
        /// </summary>
        private async Task SaveArrayElementsAsync(long objectId, StructureMetadata structure, long arrayParentId, IEnumerable arrayValue, long schemeId)
        {
            int index = 0;
            foreach (var item in arrayValue)
            {
                if (item == null) 
                {
                    index++;
                    continue;
                }
                

                // Create array element record
                var elementRecord = new RedbValue
                {
                    Id = await _context.Keys.NextValueIdAsync(),
                    IdObject = objectId,
                    IdStructure = structure.Id,
                    ArrayParentId = arrayParentId,  // Link to base array record
                    ArrayIndex = index.ToString()   // Position in array (now string)
                };

                // If element is Class type, create element hash in _Guid
                if (ObjectStorageProviderExtensions.IsClassType(structure.TypeSemantic))
                {
                    elementRecord.Guid = RedbHash.ComputeForProps(item);  // Element hash
                    _pendingValuesToInsert.Add(elementRecord);
                    
                    // Save Class element child fields via _array_parent_id
                    await SaveClassChildrenAsync(objectId, elementRecord.Id, item, structure.Id, schemeId);
                }
                else
                {
                    var processedValue = await ProcessNestedObjectsAsync(item, structure.DbType, false, objectId);
                    SetSimpleValueByType(elementRecord, structure.DbType, processedValue);
                    _pendingValuesToInsert.Add(elementRecord);
                }

                index++;
            }
        }

        /// <summary>
        /// ✅ NEW PARADIGM: Save child fields of Class objects using hierarchical scheme structures
        /// </summary>
        private async Task SaveClassChildrenAsync(long objectId, long parentRecordId, object classObject, long parentStructureId, long schemeId, int depth = 0)
        {
            if (classObject == null || depth > 5) return; // Protection against deep recursion

            // Get all public properties of Class object via reflection
            var classType = classObject.GetType();
            var properties = classType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => !p.ShouldIgnoreForRedb())
                .Where(p => p.GetIndexParameters().Length == 0) // Skip indexers (e.g. Dictionary.Item[key])
                .ToArray();

            // Get scheme structures for lookup
            var allStructures = await GetStructuresWithMetadataAsync(schemeId);

            foreach (var property in properties)
            {
                var rawValue = property.GetValue(classObject);
                if (rawValue == null) 
                {
                    continue;
                }

                // ✅ NEW LOGIC: Search for child structure among children of parent structure
                var structure = allStructures.FirstOrDefault(s => 
                    s.Name == property.Name && 
                    s.IdParent == parentStructureId);
                    
                if (structure == null) 
                {
                    continue; // Skip fields without structures
                }

                // Create child field record with _array_parent_id link
                var childRecordId = await _context.Keys.NextValueIdAsync();
                var childRecord = new RedbValue
                {
                    Id = childRecordId,
                    IdObject = objectId,
                    IdStructure = structure.Id,  // Now using real structure
                    ArrayParentId = parentRecordId,  // Link to parent Class field
                    ArrayIndex = ((int)(childRecordId % 1000000)).ToString()  // Unique ID as index (now string)
                };

                // Determine field type and save accordingly
                // IMPORTANT: Check for Dictionary BEFORE checking for IEnumerable (Dictionary implements IEnumerable!)
                if (IsDictionaryType(property.PropertyType))
                {
                    // Child Dictionary inside Class - create hash and elements
                    var dictHash = RedbHash.ComputeForProps(rawValue);
                    childRecord.Guid = dictHash;
                    _pendingValuesToInsert.Add(childRecord);
                    
                    // Save Dictionary elements with serialized keys
                    await SaveDictionaryElementsForChildAsync(objectId, structure, childRecord.Id, rawValue, schemeId, depth + 1);
                }
                else if (property.PropertyType.IsArray || property.PropertyType.GetInterfaces().Contains(typeof(System.Collections.IEnumerable)))
                {
                    // Child array - create hash and elements
                    if (rawValue is System.Collections.IEnumerable enumerable && !(rawValue is string))
                    {
                        var arrayHash = RedbHash.ComputeForProps(rawValue);
                        childRecord.Guid = arrayHash;
                        _pendingValuesToInsert.Add(childRecord);
                        
                        // Save child array elements
                        int index = 0;
                        foreach (var item in enumerable)
                        {
                            if (item != null)
                            {
                                var elementRecord = new RedbValue
                                {
                                    Id = await _context.Keys.NextValueIdAsync(),
                                    IdObject = objectId,
                                    IdStructure = structure.Id,
                                    ArrayParentId = childRecord.Id,  // Link to base array record
                                    ArrayIndex = index.ToString()    // Now string
                                };
                                
                                // Determine array element type
                                if (IsBusinessClassProperty(item.GetType()))
                                {
                                    var itemHash = RedbHash.ComputeForProps(item);
                                    elementRecord.Guid = itemHash;
                                    _pendingValuesToInsert.Add(elementRecord);
                                    
                                    // Recursively save array element fields
                                    await SaveClassChildrenAsync(objectId, elementRecord.Id, item, structure.Id, schemeId, depth + 1);
                                }
                                else
                                {
                                    // Simple array element
                                    SetSimpleValueByType(elementRecord, GetDbTypeForValue(item), item);
                                    _pendingValuesToInsert.Add(elementRecord);
                                }
                            }
                            index++;
                        }
                    }
                }
                else if (IsBusinessClassProperty(property.PropertyType))
                {
                    // Nested business class - save recursively
                    var nestedHash = RedbHash.ComputeForProps(rawValue);
                    childRecord.Guid = nestedHash;
                    _pendingValuesToInsert.Add(childRecord);
                    
                    // Recursively save nested class fields
                    await SaveClassChildrenAsync(objectId, childRecord.Id, rawValue, structure.Id, schemeId, depth + 1);
                }
                else
                {
                    // Simple field - save value
                    SetSimpleValueByType(childRecord, GetDbTypeForValue(rawValue), rawValue);
                    _pendingValuesToInsert.Add(childRecord);
                }
            }
        }

        /// <summary>
        /// Get scheme ID for object
        /// </summary>
        protected async Task<long?> GetSchemeIdForObject(long objectId)
        {
            return await _context.ExecuteScalarAsync<long?>(
                Sql.ObjectStorage_SelectSchemeIdByObjectId(), objectId);
        }

        /// <summary>
        /// Determine if type is a business class
        /// </summary>
        private static bool IsBusinessClassProperty(Type type)
        {
            if (type.IsPrimitive || type == typeof(string) || type == typeof(decimal)) return false;
            if (type == typeof(DateTime) || type == typeof(DateTimeOffset) || type == typeof(DateOnly) || type == typeof(TimeOnly) || type == typeof(TimeSpan) || type == typeof(Guid) || type == typeof(TimeSpan) || type == typeof(byte[])) return false;
            if (Nullable.GetUnderlyingType(type) != null) return false;
            if (type.IsArray) return false;
            if (type.IsEnum) return false;
            if (type.Namespace?.StartsWith("System") == true) return false;
            return type.IsClass;
        }

        /// <summary>
        /// Get DB type for value
        /// </summary>
        private static string GetDbTypeForValue(object value)
        {
            return value switch
            {
                int or long => "Long",
                double or float => "Double",
                decimal => "Numeric",
                bool => "Boolean",
                DateTimeOffset => "DateTimeOffset",
                DateTime => "DateTimeOffset",
                Guid => "Guid",
                byte[] => "ByteArray",
                _ => "String"
            };
        }
        
        /// <summary>
        /// Check if type is Dictionary&lt;K,V&gt;
        /// </summary>
        private static bool IsDictionaryType(Type type)
        {
            if (!type.IsGenericType) return false;
            var genericDef = type.GetGenericTypeDefinition();
            return genericDef == typeof(Dictionary<,>) || genericDef == typeof(IDictionary<,>);
        }
        
        /// <summary>
        /// Save Dictionary elements inside a Class child (e.g. ComplexDictValue.Scores)
        /// </summary>
        private async Task SaveDictionaryElementsForChildAsync(long objectId, StructureMetadata structure, long dictParentId, object dictValue, long schemeId, int depth)
        {
            if (depth > 5) return; // Protection against deep recursion
            
            var dictType = dictValue.GetType();
            var keyType = dictType.GetGenericArguments()[0];
            var valueType = dictType.GetGenericArguments()[1];
            
            var enumerator = ((System.Collections.IEnumerable)dictValue).GetEnumerator();
            
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
                    Id = await _context.Keys.NextValueIdAsync(),
                    IdObject = objectId,
                    IdStructure = structure.Id,
                    ArrayParentId = dictParentId,
                    ArrayIndex = serializedKey
                };
                
                if (value == null)
                {
                    _pendingValuesToInsert.Add(elementRecord);
                    continue;
                }
                
                // If value is business class, save recursively
                if (IsBusinessClassProperty(valueType))
                {
                    elementRecord.Guid = RedbHash.ComputeForProps(value);
                    _pendingValuesToInsert.Add(elementRecord);
                    await SaveClassChildrenAsync(objectId, elementRecord.Id, value, structure.Id, schemeId, depth + 1);
                }
                else
                {
                    // Simple type value
                    SetSimpleValueByType(elementRecord, GetDbTypeForValue(value), value);
                    _pendingValuesToInsert.Add(elementRecord);
                }
            }
        }
    }
}
