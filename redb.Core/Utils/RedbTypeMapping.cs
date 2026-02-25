using System;

namespace redb.Core.Utils;

/// <summary>
/// Helper methods for REDB type mapping between C# types and database columns
/// </summary>
public static class RedbTypeMapping
{
    /// <summary>
    /// Get _values column name for type ID
    /// </summary>
    /// <param name="typeId">Type ID from RedbTypeIds or _types table</param>
    /// <returns>Column name in _values table (e.g., "_String", "_Long")</returns>
    public static string GetValueColumn(long typeId)
    {
        return typeId switch
        {
            RedbTypeIds.String => "_String",
            RedbTypeIds.Long => "_Long",
            RedbTypeIds.Int => "_Long",
            RedbTypeIds.Short => "_Long",
            RedbTypeIds.Byte => "_Long",
            RedbTypeIds.Guid => "_Guid",
            RedbTypeIds.Double => "_Double",
            RedbTypeIds.Float => "_Double",
            RedbTypeIds.DateTimeOffset => "_DateTimeOffset",
            RedbTypeIds.DateTime => "_DateTimeOffset",
            RedbTypeIds.Boolean => "_Boolean",
            RedbTypeIds.ByteArray => "_ByteArray",
            RedbTypeIds.Numeric => "_Numeric",
            RedbTypeIds.Decimal => "_Numeric",
            RedbTypeIds.ListItem => "_ListItem",
            RedbTypeIds.Object => "_Object",
            _ => throw new ArgumentException($"Unknown type ID: {typeId}", nameof(typeId))
        };
    }
    
    /// <summary>
    /// Check if type can be used as Dictionary key
    /// </summary>
    /// <param name="typeId">Type ID to check</param>
    /// <returns>True if type can be Dictionary key</returns>
    public static bool CanBeKey(long typeId)
    {
        return typeId switch
        {
            RedbTypeIds.String => true,
            RedbTypeIds.Long => true,
            RedbTypeIds.Int => true,
            RedbTypeIds.Short => true,
            RedbTypeIds.Byte => true,
            RedbTypeIds.Guid => true,
            _ => false
        };
    }
    
    /// <summary>
    /// Check if type is a collection type (Array/Dictionary/JsonDocument/XDocument)
    /// These types are used for _schemes._type to indicate Props structure
    /// </summary>
    /// <param name="typeId">Type ID to check</param>
    /// <returns>True if type is collection</returns>
    public static bool IsCollectionType(long typeId)
    {
        return typeId is RedbTypeIds.Array or RedbTypeIds.Dictionary 
            or RedbTypeIds.JsonDocument or RedbTypeIds.XDocument;
    }
    
    /// <summary>
    /// Check if type requires _values records (vs direct storage in _objects._value_*)
    /// </summary>
    /// <param name="typeId">Type ID to check</param>
    /// <returns>True if type needs _values records</returns>
    public static bool RequiresValuesRecords(long typeId)
    {
        // Only Class, Array, Dictionary, JsonDocument, XDocument require _values
        return typeId is RedbTypeIds.Class or RedbTypeIds.Array or RedbTypeIds.Dictionary
            or RedbTypeIds.JsonDocument or RedbTypeIds.XDocument;
    }
    
    /// <summary>
    /// Get _objects column name for RedbPrimitive value storage
    /// </summary>
    /// <param name="typeId">Type ID</param>
    /// <returns>Column name in _objects table (e.g., "_value_long")</returns>
    public static string GetObjectValueColumn(long typeId)
    {
        return typeId switch
        {
            RedbTypeIds.String => "_value_string",
            RedbTypeIds.Long => "_value_long",
            RedbTypeIds.Int => "_value_long",
            RedbTypeIds.Short => "_value_long",
            RedbTypeIds.Byte => "_value_long",
            RedbTypeIds.Guid => "_value_guid",
            RedbTypeIds.Double => "_value_double",
            RedbTypeIds.Float => "_value_double",
            RedbTypeIds.DateTimeOffset => "_value_datetime",
            RedbTypeIds.DateTime => "_value_datetime",
            RedbTypeIds.Boolean => "_value_bool",
            RedbTypeIds.ByteArray => "_value_bytes",
            RedbTypeIds.Numeric => "_value_numeric",
            RedbTypeIds.Decimal => "_value_numeric",
            _ => throw new ArgumentException($"Type {typeId} cannot be stored in _objects._value_* columns", nameof(typeId))
        };
    }
}

