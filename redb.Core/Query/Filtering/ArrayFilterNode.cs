using redb.Core.Query.Models;

namespace redb.Core.Query.Filtering;

/// <summary>
/// Tree structure for ArrayContains filter operations.
/// Preserves logical structure (AND/OR/NOT) for correct SQL generation.
/// </summary>
public abstract record ArrayFilterNode;

/// <summary>
/// Leaf node - a specific operation on an array field.
/// </summary>
/// <param name="FieldName">Name of the array field (e.g., "Scores", "Tags")</param>
/// <param name="StructureId">ID of the structure in _structures table</param>
/// <param name="DbColumn">Column name in _values table (_Long, _String, etc.)</param>
/// <param name="Operator">Type of array operation</param>
/// <param name="Value">Value to compare against (for Contains, Count operations)</param>
/// <param name="ListItemProp">For ListItem arrays: which property (Value, Alias, Id)</param>
public record ArrayLeaf(
    string FieldName,
    long StructureId,
    string DbColumn,
    ArrayLeafOperator Operator,
    object? Value,
    ListItemProperty? ListItemProp = null
) : ArrayFilterNode;

/// <summary>
/// AND node - all child conditions must be true.
/// </summary>
public record ArrayAnd(IReadOnlyList<ArrayFilterNode> Children) : ArrayFilterNode;

/// <summary>
/// OR node - at least one child condition must be true.
/// </summary>
public record ArrayOr(IReadOnlyList<ArrayFilterNode> Children) : ArrayFilterNode;

/// <summary>
/// NOT node - negation of inner condition.
/// </summary>
public record ArrayNot(ArrayFilterNode Inner) : ArrayFilterNode;

/// <summary>
/// Operators for array leaf nodes.
/// </summary>
public enum ArrayLeafOperator
{
    /// <summary>array.Contains(value) → EXISTS ... AND _column = value</summary>
    Contains,
    
    /// <summary>array.Any() → EXISTS ... (array is not empty)</summary>
    Any,
    
    /// <summary>!array.Any() or array.Length == 0 → NOT EXISTS</summary>
    Empty,
    
    /// <summary>array.Count == N</summary>
    CountEqual,
    
    /// <summary>array.Count > N</summary>
    CountGreater,
    
    /// <summary>array.Count >= N</summary>
    CountGreaterOrEqual,
    
    /// <summary>array.Count &lt; N</summary>
    CountLess,
    
    /// <summary>array.Count &lt;= N</summary>
    CountLessOrEqual
}

