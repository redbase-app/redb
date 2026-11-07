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
/// Multi-argument function call (Pro Only).
/// Used for the string functions <c>Substring</c>, <c>Replace</c>, <c>IndexOf</c>,
/// <c>PadLeft</c>, <c>PadRight</c> where the SQL counterpart accepts 2-3 arguments.
/// Translated by the Free PVT engine (e.g. <c>$substring</c>, <c>$replace</c>,
/// <c>$indexof</c>, <c>$padleft</c>, <c>$padright</c> in <c>17_pvt_expr.sql</c>) and by
/// the Pro SQL mapper to native PostgreSQL functions (SUBSTRING/REPLACE/POSITION/LPAD/RPAD).
/// </summary>
public record MultiArgFunctionCallExpression(
    PropertyFunction Function,
    IReadOnlyList<ValueExpression> Arguments
) : ValueExpression;

/// <summary>
/// Custom SQL function (Pro Only)
/// Examples: Sql.Function("COALESCE", p.Stock, 0), Sql.Function("POWER", p.Age, 2)
/// </summary>
public record CustomFunctionExpression(
    string FunctionName,
    IReadOnlyList<ValueExpression> Arguments
) : ValueExpression;

/// <summary>
/// Coalesce expression for the C# <c>??</c> operator and n-ary <see cref="System.Linq.Enumerable"/>-style fallbacks.
/// Examples: <c>x.Bonus ?? 0</c>, <c>x.Name ?? x.LegalName ?? "(n/a)"</c>.
/// Emitted by the shared parser when it sees <see cref="System.Linq.Expressions.ExpressionType.Coalesce"/>;
/// translated to <c>COALESCE(arg1, arg2, ...)</c> by both the Free PVT engine (<c>$coalesce</c>) and the Pro SQL mapper.
/// Chained <c>a ?? b ?? c</c> is parsed right-associatively, so the parser flattens it into a single n-ary node.
/// </summary>
public record CoalesceExpression(
    IReadOnlyList<ValueExpression> Arguments
) : ValueExpression;

/// <summary>
/// Conditional value (C# ternary <c>?:</c> operator).
/// Examples: <c>e.IsRemote ? e.Salary : e.Salary * 0.5m</c>,
/// <c>(e.Rating &gt; 4 ? "TOP" : "STD")</c>.
/// Test is a full <see cref="FilterExpression"/> (comparison / logical / null-check).
/// Translated to <c>CASE WHEN test THEN ifTrue ELSE ifFalse END</c> by both the Free
/// PVT engine (<c>$if</c> in <c>17_pvt_expr.sql</c>) and the Pro SQL mapper.
/// Named with the <c>Value</c> suffix to avoid collision with
/// <see cref="System.Linq.Expressions.ConditionalExpression"/>.
/// </summary>
public record ConditionalValueExpression(
    FilterExpression Test,
    ValueExpression IfTrue,
    ValueExpression IfFalse
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
        MultiArgFunctionCallExpression mfce => mfce.Arguments.SelectMany(ExtractFieldPaths),
        CustomFunctionExpression cfe => cfe.Arguments
            .OfType<PropertyValueExpression>()
            .Where(p => !p.Property.IsBaseField)
            .Select(p => p.Property.Name),
        _ => []
    };
};
