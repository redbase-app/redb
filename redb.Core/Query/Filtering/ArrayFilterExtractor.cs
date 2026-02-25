using redb.Core.Query.Models;
using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query.Filtering;

/// <summary>
/// [OBSOLETE] This class is no longer used in Pro version.
/// Arrays are now included in PVT and filtered with ANY()/array_length().
/// Kept for backward compatibility with non-Pro implementations.
/// 
/// Extracts ArrayFilterNode tree from FilterExpression.
/// Preserves logical structure (AND/OR/NOT).
/// Returns "remainder" FilterExpression without ArrayContains for PVT.
/// </summary>
[Obsolete("Arrays are now included in PVT in Pro version. Use ProSqlBuilder for array filtering.")]
public class ArrayFilterExtractor
{
    /// <summary>
    /// Extraction result containing both PVT filter and Array filter.
    /// </summary>
    public record ExtractionResult(
        /// <summary>Filter without ArrayContains for PVT WHERE clause</summary>
        FilterExpression? PvtFilter,
        /// <summary>ArrayContains tree for EXISTS subqueries</summary>
        ArrayFilterNode? ArrayFilter,
        /// <summary>Names of array fields found</summary>
        HashSet<string> ArrayFieldNames
    );
    
    private readonly IReadOnlyDictionary<string, FieldInfo> _fields;
    
    /// <summary>
    /// Creates extractor with field metadata for array detection.
    /// </summary>
    public ArrayFilterExtractor(IReadOnlyDictionary<string, FieldInfo> fields)
    {
        _fields = fields;
    }
    
    /// <summary>
    /// Extracts ArrayContains into separate tree, returns remainder for PVT.
    /// </summary>
    public ExtractionResult Extract(FilterExpression? filter)
    {
        if (filter == null)
            return new ExtractionResult(null, null, new HashSet<string>());
        
        var arrayFieldNames = new HashSet<string>();
        var (pvtFilter, arrayFilter) = ExtractRecursive(filter, arrayFieldNames);
        
        return new ExtractionResult(pvtFilter, arrayFilter, arrayFieldNames);
    }
    
    /// <summary>
    /// Recursive extraction.
    /// </summary>
    private (FilterExpression? pvt, ArrayFilterNode? array) ExtractRecursive(
        FilterExpression filter, 
        HashSet<string> arrayFieldNames)
    {
        switch (filter)
        {
            case ComparisonExpression comparison:
                return ExtractComparison(comparison, arrayFieldNames);
                
            case LogicalExpression logical:
                return ExtractLogical(logical, arrayFieldNames);
                
            case NullCheckExpression nullCheck:
                // NullCheck doesn't contain ArrayContains
                return (nullCheck, null);
                
            case InExpression inExpr:
                // InExpression doesn't contain ArrayContains
                return (inExpr, null);
                
            default:
                return (filter, null);
        }
    }
    
    /// <summary>
    /// Extracts from ComparisonExpression.
    /// </summary>
    private (FilterExpression? pvt, ArrayFilterNode? array) ExtractComparison(
        ComparisonExpression comparison,
        HashSet<string> arrayFieldNames)
    {
        if (!IsArrayOperator(comparison.Operator))
        {
            // Not ArrayContains - goes to PVT
            return (comparison, null);
        }
        
        // This is ArrayContains - convert to ArrayLeaf
        var fieldName = comparison.Property.Name;
        arrayFieldNames.Add(fieldName);
        
        if (!_fields.TryGetValue(fieldName, out var fieldInfo))
        {
            // Field not found - return as-is (will error later)
            return (comparison, null);
        }
        
        var leafOp = MapToArrayLeafOperator(comparison.Operator);
        var leaf = new ArrayLeaf(
            fieldName,
            fieldInfo.StructureId,
            fieldInfo.DbColumn,
            leafOp,
            comparison.Value,
            fieldInfo.ListItemProp // Pass ListItemProp for ListItem arrays
        );
        
        return (null, leaf);
    }
    
    /// <summary>
    /// Extracts from LogicalExpression.
    /// </summary>
    private (FilterExpression? pvt, ArrayFilterNode? array) ExtractLogical(
        LogicalExpression logical,
        HashSet<string> arrayFieldNames)
    {
        var pvtOperands = new List<FilterExpression>();
        var arrayOperands = new List<ArrayFilterNode>();
        
        foreach (var operand in logical.Operands)
        {
            var (pvt, array) = ExtractRecursive(operand, arrayFieldNames);
            
            if (pvt != null)
                pvtOperands.Add(pvt);
            if (array != null)
                arrayOperands.Add(array);
        }
        
        // Build results
        FilterExpression? pvtResult = null;
        ArrayFilterNode? arrayResult = null;
        
        // PVT part
        if (pvtOperands.Count > 0)
        {
            if (pvtOperands.Count == 1)
            {
                // For NOT need to wrap even single operand
                pvtResult = logical.Operator == LogicalOperator.Not 
                    ? new LogicalExpression(LogicalOperator.Not, pvtOperands)
                    : pvtOperands[0];
            }
            else
            {
                pvtResult = new LogicalExpression(logical.Operator, pvtOperands);
            }
        }
        
        // Array part
        if (arrayOperands.Count > 0)
        {
            arrayResult = arrayOperands.Count == 1
                ? arrayOperands[0]
                : logical.Operator switch
                {
                    LogicalOperator.And => new ArrayAnd(arrayOperands),
                    LogicalOperator.Or => new ArrayOr(arrayOperands),
                    LogicalOperator.Not when arrayOperands.Count == 1 => new ArrayNot(arrayOperands[0]),
                    _ => new ArrayAnd(arrayOperands) // fallback
                };
        }
        
        // Special handling for NOT
        if (logical.Operator == LogicalOperator.Not && arrayResult != null && pvtResult == null)
        {
            // Entire NOT was on ArrayContains
            arrayResult = new ArrayNot(arrayResult);
        }
        
        return (pvtResult, arrayResult);
    }
    
    /// <summary>
    /// Checks if operator is array-related.
    /// </summary>
    private static bool IsArrayOperator(ComparisonOperator op)
    {
        return op is 
            ComparisonOperator.ArrayContains or 
            ComparisonOperator.ArrayAny or 
            ComparisonOperator.ArrayEmpty or 
            ComparisonOperator.ArrayCount or
            ComparisonOperator.ArrayCountGt or
            ComparisonOperator.ArrayCountGte or
            ComparisonOperator.ArrayCountLt or
            ComparisonOperator.ArrayCountLte;
    }
    
    /// <summary>
    /// Maps ComparisonOperator to ArrayLeafOperator.
    /// </summary>
    private static ArrayLeafOperator MapToArrayLeafOperator(ComparisonOperator op)
    {
        return op switch
        {
            ComparisonOperator.ArrayContains => ArrayLeafOperator.Contains,
            ComparisonOperator.ArrayAny => ArrayLeafOperator.Any,
            ComparisonOperator.ArrayEmpty => ArrayLeafOperator.Empty,
            ComparisonOperator.ArrayCount => ArrayLeafOperator.CountEqual,
            ComparisonOperator.ArrayCountGt => ArrayLeafOperator.CountGreater,
            ComparisonOperator.ArrayCountGte => ArrayLeafOperator.CountGreaterOrEqual,
            ComparisonOperator.ArrayCountLt => ArrayLeafOperator.CountLess,
            ComparisonOperator.ArrayCountLte => ArrayLeafOperator.CountLessOrEqual,
            _ => throw new NotSupportedException($"Unknown array operator: {op}")
        };
    }
}

