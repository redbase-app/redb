using System;
using System.Collections.Generic;

namespace redb.Core.Query.QueryExpressions;

/// <summary>
/// Base class for filter expressions
/// </summary>
public abstract record FilterExpression;

#region Value Expressions (Pro: arithmetic and functions)

/// <summary>
/// Base class for value expressions (Pro version)
/// Used for arithmetic, functions and complex expressions
/// </summary>
public abstract record ValueExpression;

/// <summary>
/// Reference to object property
/// </summary>
public record PropertyValueExpression(PropertyInfo Property) : ValueExpression;

/// <summary>
/// Constant value
/// </summary>
public record ConstantValueExpression(object? Value, Type Type) : ValueExpression;

/// <summary>
/// Arithmetic expression (Pro Only)
/// Examples: p.Price * 2, p.Stock - p.Reserved
/// </summary>
public record ArithmeticExpression(
    ValueExpression Left,
    ArithmeticOperator Operator,
    ValueExpression Right
) : ValueExpression;

/// <summary>
/// Function call on value (Pro Only)
/// Examples: LOWER(p.Name), EXTRACT(YEAR FROM p.Date)
/// </summary>
public record FunctionCallExpression(
    PropertyFunction Function,
    ValueExpression Argument
) : ValueExpression;

/// <summary>
/// Custom SQL function (Pro Only)
/// Examples: Sql.Function("COALESCE", p.Stock, 0), Sql.Function("POWER", p.Age, 2)
/// </summary>
public record CustomFunctionExpression(
    string FunctionName,
    IReadOnlyList<ValueExpression> Arguments
) : ValueExpression;

#endregion

#region Filter Expressions

/// <summary>
/// Comparison expression (property operator value)
/// Backward compatibility: Left can be PropertyInfo directly
/// </summary>
public record ComparisonExpression(
    PropertyInfo Property,
    ComparisonOperator Operator,
    object? Value
) : FilterExpression
{
    /// <summary>
    /// Pro: Left side as ValueExpression (for arithmetic/functions)
    /// If null - Property is used
    /// </summary>
    public ValueExpression? LeftExpression { get; init; }
    
    /// <summary>
    /// Pro: Right side as ValueExpression (for arithmetic/functions)
    /// If null - Value is used
    /// </summary>
    public ValueExpression? RightExpression { get; init; }
}

/// <summary>
/// Logical expression (AND, OR, NOT)
/// </summary>
public record LogicalExpression(
    LogicalOperator Operator,
    IReadOnlyList<FilterExpression> Operands
) : FilterExpression;

/// <summary>
/// Expression for null check
/// </summary>
public record NullCheckExpression(
    PropertyInfo Property,
    bool IsNull
) : FilterExpression;

/// <summary>
/// Expression for checking inclusion in list
/// </summary>
public record InExpression(
    PropertyInfo Property,
    IReadOnlyList<object> Values
) : FilterExpression;

#endregion

/// <summary>
/// Sorting information.
/// Supports simple property sorting and complex expressions (arithmetic, functions).
/// </summary>
/// <param name="Property">Property for simple sorting (Name, Price, etc.)</param>
/// <param name="Direction">Sort direction (Ascending/Descending)</param>
/// <param name="Expression">Optional: Complex expression for Pro features (p.Price * 2, LOWER(p.Name))</param>
public record OrderingExpression(
    PropertyInfo Property,
    SortDirection Direction,
    ValueExpression? Expression = null
)
{
    /// <summary>
    /// Returns true if this ordering uses complex expression (arithmetic/functions).
    /// </summary>
    public bool HasExpression => Expression != null;
    
    /// <summary>
    /// Extracts all field paths used in this ordering (for field resolution).
    /// </summary>
    public IEnumerable<string> GetFieldPaths()
    {
        if (Expression != null)
        {
            return ExtractFieldPaths(Expression);
        }
        return Property.IsBaseField ? [] : [Property.Name];
    }
    
    private static IEnumerable<string> ExtractFieldPaths(ValueExpression expr) => expr switch
    {
        PropertyValueExpression pve => pve.Property.IsBaseField ? [] : [pve.Property.Name],
        ArithmeticExpression ae => ExtractFieldPaths(ae.Left).Concat(ExtractFieldPaths(ae.Right)),
        FunctionCallExpression fce => ExtractFieldPaths(fce.Argument),
        CustomFunctionExpression cfe => cfe.Arguments
            .OfType<PropertyValueExpression>()
            .Where(p => !p.Property.IsBaseField)
            .Select(p => p.Property.Name),
        _ => []
    };
};
