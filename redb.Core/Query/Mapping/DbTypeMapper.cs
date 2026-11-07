namespace redb.Core.Query.Mapping;

/// <summary>
/// Maps redb DbType names to _values column names.
/// DB-agnostic: same column structure across all databases.
/// </summary>
public static class DbTypeMapper
{
    /// <summary>
    /// Maps database type name to corresponding column in _values table.
    /// </summary>
    /// <param name="dbType">Type name from _types table (Long, String, Boolean, etc.)</param>
    /// <returns>Column name in _values table (_Long, _String, etc.)</returns>
    public static string MapDbTypeToColumn(string? dbType) => dbType switch
    {
        "Long" => "_Long",
        "String" => "_String",
        "Boolean" => "_Boolean",
        "DateTime" => "_DateTimeOffset",
        "DateTimeOffset" => "_DateTimeOffset",
        "Double" => "_Double",
        "Numeric" => "_Numeric",
        "Guid" => "_Guid",
        "ByteArray" => "_ByteArray",
        "ListItem" => "_ListItem",
        "Object" => "_Object",
        "Text" => "_Text",
        _ => "_String"
    };
    
    /// <summary>
    /// Gets the C# type for a database type.
    /// </summary>
    public static Type GetClrType(string? dbType) => dbType switch
    {
        "Long" => typeof(long),
        "String" => typeof(string),
        "Text" => typeof(string),
        "Boolean" => typeof(bool),
        "DateTime" => typeof(DateTime),
        "DateTimeOffset" => typeof(DateTimeOffset),
        "Double" => typeof(double),
        "Numeric" => typeof(decimal),
        "Guid" => typeof(Guid),
        "ByteArray" => typeof(byte[]),
        "ListItem" => typeof(long), // Foreign key to _list_items
        "Object" => typeof(long),   // Foreign key to _objects
        _ => typeof(string)
    };
}

