using System;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace redb.Core.Utils;

/// <summary>
/// Serializes and deserializes Dictionary keys to/from _array_index (text) storage.
/// Simple keys (string, int, Guid, etc.) are serialized directly.
/// Complex keys (Tuple, custom classes) are serialized to Base64-encoded JSON.
/// </summary>
public static class RedbKeySerializer
{
    /// <summary>
    /// Serialize key to _array_index text value
    /// </summary>
    /// <typeparam name="TKey">Type of key</typeparam>
    /// <param name="key">Key to serialize</param>
    /// <returns>String representation for _array_index column</returns>
    public static string Serialize<TKey>(TKey key)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key), "Dictionary key cannot be null");
            
        return SerializeObject(key, typeof(TKey));
    }
    
    /// <summary>
    /// Serialize key to _array_index text value (non-generic version)
    /// </summary>
    /// <param name="key">Key to serialize</param>
    /// <param name="keyType">Type of key</param>
    /// <returns>String representation for _array_index column</returns>
    public static string SerializeObject(object key, Type keyType)
    {
        if (key == null)
            throw new ArgumentNullException(nameof(key), "Dictionary key cannot be null");
        
        // String types - direct
        if (keyType == typeof(string))
            return (string)key;
        if (keyType == typeof(char))
            return ((char)key).ToString();
            
        // Integer types - to string
        if (keyType == typeof(long))
            return ((long)key).ToString(CultureInfo.InvariantCulture);
        if (keyType == typeof(int))
            return ((int)key).ToString(CultureInfo.InvariantCulture);
        if (keyType == typeof(short))
            return ((short)key).ToString(CultureInfo.InvariantCulture);
        if (keyType == typeof(byte))
            return ((byte)key).ToString(CultureInfo.InvariantCulture);
            
        // Floating point - invariant culture
        if (keyType == typeof(double))
            return ((double)key).ToString("G17", CultureInfo.InvariantCulture);
        if (keyType == typeof(float))
            return ((float)key).ToString("G9", CultureInfo.InvariantCulture);
        if (keyType == typeof(decimal))
            return ((decimal)key).ToString(CultureInfo.InvariantCulture);
            
        // Boolean
        if (keyType == typeof(bool))
            return ((bool)key) ? "true" : "false";
            
        // DateTime types - ISO 8601
        if (keyType == typeof(DateTime))
            return ((DateTime)key).ToString("O", CultureInfo.InvariantCulture);
        if (keyType == typeof(DateTimeOffset))
            return ((DateTimeOffset)key).ToString("O", CultureInfo.InvariantCulture);
        if (keyType == typeof(DateOnly))
            return ((DateOnly)key).ToString("O", CultureInfo.InvariantCulture);
        if (keyType == typeof(TimeOnly))
            return ((TimeOnly)key).ToString("O", CultureInfo.InvariantCulture);
        if (keyType == typeof(TimeSpan))
            return ((TimeSpan)key).ToString("c", CultureInfo.InvariantCulture);
            
        // Guid
        if (keyType == typeof(Guid))
            return ((Guid)key).ToString();
            
        // Byte array - Base64
        if (keyType == typeof(byte[]))
            return Convert.ToBase64String((byte[])key);
            
        // Enum - as string name
        if (keyType.IsEnum)
            return key.ToString()!;
            
        // Complex keys (Tuple, custom classes) - Base64-encoded JSON
        // Base64 avoids escaping issues when key is used in JSON facet filters
        string json;
        
        // ValueTuple requires special handling - System.Text.Json doesn't serialize it by default
        if (IsValueTuple(keyType))
        {
            json = SerializeValueTuple(key, keyType);
        }
        else
        {
            json = JsonSerializer.Serialize(key, keyType);
        }
        
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
    }
    
    /// <summary>
    /// Check if type is ValueTuple
    /// </summary>
    private static bool IsValueTuple(Type type)
    {
        if (!type.IsGenericType) return false;
        var genericDef = type.GetGenericTypeDefinition();
        return genericDef == typeof(ValueTuple<,>) ||
               genericDef == typeof(ValueTuple<,,>) ||
               genericDef == typeof(ValueTuple<,,,>) ||
               genericDef == typeof(ValueTuple<,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,,>) ||
               genericDef == typeof(ValueTuple<,,,,,,,>);
    }
    
    /// <summary>
    /// Serialize ValueTuple to JSON manually (System.Text.Json doesn't support it)
    /// </summary>
    private static string SerializeValueTuple(object tuple, Type tupleType)
    {
        var fields = tupleType.GetFields();
        var dict = new Dictionary<string, object?>();
        
        foreach (var field in fields)
        {
            dict[field.Name] = field.GetValue(tuple);
        }
        
        return JsonSerializer.Serialize(dict);
    }
    
    /// <summary>
    /// Deserialize _array_index text value to key
    /// </summary>
    /// <typeparam name="TKey">Type of key</typeparam>
    /// <param name="arrayIndex">String from _array_index column</param>
    /// <returns>Deserialized key</returns>
    public static TKey Deserialize<TKey>(string arrayIndex)
    {
        if (arrayIndex == null)
            throw new ArgumentNullException(nameof(arrayIndex), "_array_index cannot be null for Dictionary element");
            
        return (TKey)DeserializeObject(arrayIndex, typeof(TKey))!;
    }
    
    /// <summary>
    /// Deserialize _array_index text value to key (non-generic version)
    /// </summary>
    /// <param name="arrayIndex">String from _array_index column</param>
    /// <param name="keyType">Type of key</param>
    /// <returns>Deserialized key</returns>
    public static object? DeserializeObject(string arrayIndex, Type keyType)
    {
        if (arrayIndex == null)
            throw new ArgumentNullException(nameof(arrayIndex), "_array_index cannot be null for Dictionary element");
            
        // String types
        if (keyType == typeof(string))
            return arrayIndex;
        if (keyType == typeof(char))
            return arrayIndex.Length > 0 ? arrayIndex[0] : '\0';
            
        // Integer types
        if (keyType == typeof(long))
            return long.Parse(arrayIndex, CultureInfo.InvariantCulture);
        if (keyType == typeof(int))
            return int.Parse(arrayIndex, CultureInfo.InvariantCulture);
        if (keyType == typeof(short))
            return short.Parse(arrayIndex, CultureInfo.InvariantCulture);
        if (keyType == typeof(byte))
            return byte.Parse(arrayIndex, CultureInfo.InvariantCulture);
            
        // Floating point
        if (keyType == typeof(double))
            return double.Parse(arrayIndex, CultureInfo.InvariantCulture);
        if (keyType == typeof(float))
            return float.Parse(arrayIndex, CultureInfo.InvariantCulture);
        if (keyType == typeof(decimal))
            return decimal.Parse(arrayIndex, CultureInfo.InvariantCulture);
            
        // Boolean
        if (keyType == typeof(bool))
            return arrayIndex.Equals("true", StringComparison.OrdinalIgnoreCase);
            
        // DateTime types
        if (keyType == typeof(DateTime))
            return DateTime.Parse(arrayIndex, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (keyType == typeof(DateTimeOffset))
            return DateTimeOffset.Parse(arrayIndex, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        if (keyType == typeof(DateOnly))
            return DateOnly.Parse(arrayIndex, CultureInfo.InvariantCulture);
        if (keyType == typeof(TimeOnly))
            return TimeOnly.Parse(arrayIndex, CultureInfo.InvariantCulture);
        if (keyType == typeof(TimeSpan))
            return TimeSpan.Parse(arrayIndex, CultureInfo.InvariantCulture);
            
        // Guid
        if (keyType == typeof(Guid))
            return Guid.Parse(arrayIndex);
            
        // Byte array - Base64
        if (keyType == typeof(byte[]))
            return Convert.FromBase64String(arrayIndex);
            
        // Enum
        if (keyType.IsEnum)
            return Enum.Parse(keyType, arrayIndex);
            
        // Complex keys (Tuple, custom classes) - Base64-encoded JSON
        var json = Encoding.UTF8.GetString(Convert.FromBase64String(arrayIndex));
        
        // ValueTuple requires special handling
        if (IsValueTuple(keyType))
        {
            return DeserializeValueTuple(json, keyType);
        }
        
        return JsonSerializer.Deserialize(json, keyType);
    }
    
    /// <summary>
    /// Deserialize JSON to ValueTuple manually
    /// </summary>
    private static object DeserializeValueTuple(string json, Type tupleType)
    {
        var dict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json)!;
        var fields = tupleType.GetFields();
        var args = new object?[fields.Length];
        
        for (int i = 0; i < fields.Length; i++)
        {
            var field = fields[i];
            if (dict.TryGetValue(field.Name, out var element))
            {
                args[i] = JsonSerializer.Deserialize(element.GetRawText(), field.FieldType);
            }
        }
        
        return Activator.CreateInstance(tupleType, args)!;
    }
    
    /// <summary>
    /// Check if key type is a complex type requiring JSON serialization
    /// </summary>
    /// <param name="keyType">Type to check</param>
    /// <returns>True if type requires JSON serialization</returns>
    public static bool IsComplexKey(Type keyType)
    {
        // Simple types that don't need JSON
        if (keyType == typeof(string) || keyType == typeof(char))
            return false;
        if (keyType == typeof(long) || keyType == typeof(int) || 
            keyType == typeof(short) || keyType == typeof(byte))
            return false;
        if (keyType == typeof(double) || keyType == typeof(float) || keyType == typeof(decimal))
            return false;
        if (keyType == typeof(bool))
            return false;
        if (keyType == typeof(DateTime) || keyType == typeof(DateTimeOffset) ||
            keyType == typeof(DateOnly) || keyType == typeof(TimeOnly) || keyType == typeof(TimeSpan))
            return false;
        if (keyType == typeof(Guid))
            return false;
        if (keyType == typeof(byte[]))
            return false;
        if (keyType.IsEnum)
            return false;
            
        // Everything else is complex (Tuple, ValueTuple, custom classes, etc.)
        return true;
    }
    
    /// <summary>
    /// Get RedbTypeIds constant for key type
    /// </summary>
    /// <param name="keyType">C# key type</param>
    /// <returns>Type ID for _structures._key_type column</returns>
    public static long GetKeyTypeId(Type keyType)
    {
        if (keyType == typeof(string))
            return RedbTypeIds.String;
        if (keyType == typeof(char))
            return RedbTypeIds.Char;
        if (keyType == typeof(long))
            return RedbTypeIds.Long;
        if (keyType == typeof(int))
            return RedbTypeIds.Int;
        if (keyType == typeof(short))
            return RedbTypeIds.Short;
        if (keyType == typeof(byte))
            return RedbTypeIds.Byte;
        if (keyType == typeof(double))
            return RedbTypeIds.Double;
        if (keyType == typeof(float))
            return RedbTypeIds.Float;
        if (keyType == typeof(decimal))
            return RedbTypeIds.Decimal;
        if (keyType == typeof(bool))
            return RedbTypeIds.Boolean;
        if (keyType == typeof(DateTime))
            return RedbTypeIds.DateTime;
        if (keyType == typeof(DateTimeOffset))
            return RedbTypeIds.DateTimeOffset;
        if (keyType == typeof(DateOnly))
            return RedbTypeIds.DateOnly;
        if (keyType == typeof(TimeOnly))
            return RedbTypeIds.TimeOnly;
        if (keyType == typeof(TimeSpan))
            return RedbTypeIds.TimeSpan;
        if (keyType == typeof(Guid))
            return RedbTypeIds.Guid;
        if (keyType == typeof(byte[]))
            return RedbTypeIds.ByteArray;
        if (keyType.IsEnum)
            return RedbTypeIds.Enum;
            
        // Complex keys (Tuple, custom classes) stored as Base64-encoded JSON
        // Using Class type to distinguish from simple String keys
        return RedbTypeIds.Class;
    }
    
    /// <summary>
    /// Check if C# type is valid as Dictionary key
    /// </summary>
    /// <param name="keyType">Type to check</param>
    /// <returns>True if type can be used as Dictionary key</returns>
    public static bool CanBeKey(Type keyType)
    {
        // All serializable types can be keys
        // Only excluded: types that can't be serialized deterministically
        if (keyType == typeof(object))
            return false;
            
        // Dynamic types are not supported
        if (keyType.FullName == "System.Object")
            return false;
            
        return true;
    }
}

