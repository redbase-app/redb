using System.Collections.Generic;
using redb.Core.Query.Utils;

namespace redb.Core.Query.Aggregation;

/// <summary>
/// Request for batch aggregation of multiple fields.
/// </summary>
public class AggregateRequest
{
    /// <summary>Field path (e.g. "Price" or "Items[].Price")</summary>
    public string FieldPath { get; set; } = "";
    
    /// <summary>Aggregation function</summary>
    public AggregateFunction Function { get; set; }
    
    /// <summary>Result alias</summary>
    public string? Alias { get; set; }

    /// <summary>
    /// True when the path names a field of the object itself (x.Id, x.Name) and not a Props field.
    /// The path alone cannot tell: x.Props.Name and x.Name both read "Name". A path marked "0$:" is a
    /// base field whatever this says.
    /// </summary>
    public bool IsBaseField { get; set; }

    /// <summary>
    /// Whether an aggregate argument reads a field of the object itself: a member chain that starts
    /// at a parameter typed as the object and does not pass through its Props. <c>x.Id</c> does,
    /// <c>x.Props.Id</c> and anything under a parameter typed as Props do not.
    /// </summary>
    public static bool ReadsObjectField(System.Linq.Expressions.Expression? expression)
    {
        var current = StripConvert(expression);
        if (current is not System.Linq.Expressions.MemberExpression)
            return false;

        var throughProps = false;
        while (current is System.Linq.Expressions.MemberExpression member)
        {
            if (member.Member.Name == "Props")
                throughProps = true;
            current = StripConvert(member.Expression);
        }

        return !throughProps
               && current is System.Linq.Expressions.ParameterExpression parameter
               && typeof(redb.Core.Models.Contracts.IRedbObject).IsAssignableFrom(parameter.Type);
    }

    private static System.Linq.Expressions.Expression? StripConvert(System.Linq.Expressions.Expression? expression)
    {
        while (expression is System.Linq.Expressions.UnaryExpression
               {
                   NodeType: System.Linq.Expressions.ExpressionType.Convert
                       or System.Linq.Expressions.ExpressionType.ConvertChecked
               } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }
}

/// <summary>
/// Batch aggregation result.
/// </summary>
public class AggregateResult
{
    /// <summary>Results by aliases</summary>
    public Dictionary<string, object?> Values { get; set; } = new();
    
    /// <summary>Get value by alias</summary>
    public T? Get<T>(string alias) where T : struct
    {
        if (Values.TryGetValue(alias, out var value) && value != null)
        {
            return (T)TemporalDecoder.ChangeType(value, typeof(T));
        }
        return null;
    }
    
    /// <summary>Get value by alias (nullable)</summary>
    public T? GetNullable<T>(string alias) where T : struct
    {
        if (Values.TryGetValue(alias, out var value))
        {
            if (value == null) return null;
            return (T)TemporalDecoder.ChangeType(value, typeof(T));
        }
        return null;
    }
}
