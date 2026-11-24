namespace redb.Core.Query.Models;

/// <summary>
/// Metadata about a field in the PVT query.
/// Contains structure_id for SQL generation and type information for value extraction.
/// </summary>
/// <param name="StructureId">ID of the structure in _structures table</param>
/// <param name="DbType">Database type name (Long, String, Boolean, etc.)</param>
/// <param name="DbColumn">Column name in _values table (_Long, _String, etc.)</param>
/// <param name="Name">Field name as it appears in Props class</param>
/// <param name="IsArray">Whether this field is an array type</param>
/// <param name="ListItemProp">For ListItem fields: which property (Id, Value, Alias)</param>
/// <param name="DictKey">For Dictionary fields: the key (e.g., "home" for PhoneBook["home"])</param>
/// <param name="ParentStructureId">For nested Dictionary fields: parent structure ID for JOIN via _array_parent_id</param>
public record FieldInfo(
    long StructureId,
    string DbType,
    string DbColumn,
    string Name,
    bool IsArray = false,
    ListItemProperty? ListItemProp = null,
    string? DictKey = null,
    long? ParentStructureId = null
)
{
    /// <summary>
    /// For a field of an element of an array of classes (<c>Rows[].Kind</c>): the structure of that array. Such a field
    /// is not a pivot column; a condition reads it under the element row. An init property, so the constructor stays as it was
    /// in 4.7.4.
    /// </summary>
    public long? ElementOfArrayStructureId { get; init; }
}

