using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Query.Aggregation;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Translates HAVING predicates of the form
/// <c>Expression&lt;Func&lt;IRedbGrouping&lt;TKey, TProps&gt;, bool&gt;&gt;</c> into the
/// JSON shape understood by <c>pvt_build_groupby_sql</c> (and by the Pro
/// SQL builder, which compiles the same shape to native HAVING SQL).
///
/// Supported predicates:
///   Agg.Count(g)                                — <c>{"$count":"*"}</c>
///   Agg.Count(g, x =&gt; condition)                — <c>{"$count":"*","filter":&lt;condition&gt;}</c>
///   Agg.Sum/Average/Min/Max(g, x =&gt; x.Field)   — <c>{"$sum":{"$field":"Field"}}</c>
///   binary comparisons (>, &lt;, &gt;=, &lt;=, ==, !=) and &amp;&amp; / ||
///
/// Captured constants and outer-scope values are evaluated via
/// <see cref="Expression.Lambda"/>.Compile() and emitted as
/// <c>{"$const":value}</c> nodes.
/// </summary>
internal static class HavingPredicateParser
{
    private const string OwnerApi = "Having";

    /// <summary>
    /// Compile predicate body to PVT JSON string.
    /// Returns null when predicate is null.
    /// </summary>
    public static string? ToJson<TKey, TProps>(
        Expression<Func<IRedbGrouping<TKey, TProps>, bool>>? predicate)
        => ToJson(predicate, GroupItemKind.Props);

    /// <summary>
    /// Compile predicate body to PVT JSON string; <paramref name="kind"/> says what the lambdas inside Agg.* read.
    /// </summary>
    public static string? ToJson<TKey, TProps>(
        Expression<Func<IRedbGrouping<TKey, TProps>, bool>>? predicate, GroupItemKind kind)
    {
        if (predicate is null) return null;
        var node = TranslateBoolean(predicate.Body, predicate.Parameters[0], kind);
        return JsonSerializer.Serialize(node);
    }

    private static object TranslateBoolean(Expression expr, ParameterExpression groupParam, GroupItemKind kind)
    {
        // Unwrap Convert(... , bool)
        while (expr is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
            expr = u.Operand;

        switch (expr.NodeType)
        {
            case ExpressionType.AndAlso:
            case ExpressionType.And:
            {
                var be = (BinaryExpression)expr;
                return new Dictionary<string, object>
                {
                    ["$and"] = new[] { TranslateBoolean(be.Left, groupParam, kind), TranslateBoolean(be.Right, groupParam, kind) }
                };
            }
            case ExpressionType.OrElse:
            case ExpressionType.Or:
            {
                var be = (BinaryExpression)expr;
                return new Dictionary<string, object>
                {
                    ["$or"] = new[] { TranslateBoolean(be.Left, groupParam, kind), TranslateBoolean(be.Right, groupParam, kind) }
                };
            }
            case ExpressionType.Not:
            {
                var ue = (UnaryExpression)expr;
                return new Dictionary<string, object> { ["$not"] = TranslateBoolean(ue.Operand, groupParam, kind) };
            }
            case ExpressionType.GreaterThan:
            case ExpressionType.GreaterThanOrEqual:
            case ExpressionType.LessThan:
            case ExpressionType.LessThanOrEqual:
            case ExpressionType.Equal:
            case ExpressionType.NotEqual:
                return TranslateComparison((BinaryExpression)expr, groupParam, kind);
        }

        throw new NotSupportedException(
            "Unsupported HAVING expression node: " + expr.NodeType + " (" + expr + ")");
    }

    private static Dictionary<string, object> TranslateComparison(BinaryExpression be, ParameterExpression groupParam, GroupItemKind kind)
    {
        var opKey = be.NodeType switch
        {
            ExpressionType.GreaterThan => "$gt",
            ExpressionType.GreaterThanOrEqual => "$gte",
            ExpressionType.LessThan => "$lt",
            ExpressionType.LessThanOrEqual => "$lte",
            ExpressionType.Equal => "$eq",
            ExpressionType.NotEqual => "$ne",
            _ => throw new NotSupportedException("Comparison " + be.NodeType + " is not supported in HAVING.")
        };

        var left = TranslateOperand(be.Left, groupParam, kind);
        var right = TranslateOperand(be.Right, groupParam, kind);
        return new Dictionary<string, object> { [opKey] = new[] { left, right } };
    }

    private static object TranslateOperand(Expression expr, ParameterExpression groupParam, GroupItemKind kind)
    {
        // Unwrap Convert (boxing for value types: e.g. (decimal)Agg.Sum(...))
        while (expr is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
            expr = u.Operand;

        if (expr is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Agg))
            return TranslateAggregate(mc, groupParam, kind);

        // Treat anything not referencing the group parameter as a captured constant.
        if (!GroupSelectorMembers.ReferencesParameter(expr, groupParam))
        {
            var value = EvaluateConstant(expr);
            if (value is null)
                // G-5 (ревью 2026-09-03): null превращался в СТРОКУ "null" и сравнение шло с
                // текстом. NULL-семантики у HAVING-констант нет - отказ громкий.
                throw new NotSupportedException(
                    "HAVING: сравнение с null-константой не поддерживается - сравните с Count или не-null значением.");
            return new Dictionary<string, object> { ["$const"] = value };
        }

        throw new NotSupportedException(
            "Unsupported HAVING operand: " + expr + ". Only Agg.* aggregates and captured constants are allowed.");
    }

    private static object TranslateAggregate(MethodCallExpression mc, ParameterExpression groupParam, GroupItemKind kind)
    {
        // One strict reading for SelectAsync and HAVING: the same call means the same SQL in both.
        var request = GroupAggregateParser.Parse(mc, groupParam, alias: mc.Method.Name, kind, OwnerApi);

        if (request.Function == AggregateFunction.Count)
        {
            var node = new Dictionary<string, object> { ["$count"] = "*" };
            if (request.ConditionJson != null)
                node["filter"] = JsonDocument.Parse(request.ConditionJson).RootElement.Clone();
            return node;
        }

        var opKey = request.Function switch
        {
            AggregateFunction.Sum => "$sum",
            AggregateFunction.Average => "$avg",
            AggregateFunction.Min => "$min",
            _ => "$max"
        };
        return new Dictionary<string, object>
        {
            [opKey] = new Dictionary<string, object> { ["$field"] = request.FieldPath }
        };
    }

    private static object? EvaluateConstant(Expression expr)
    {
        if (expr is ConstantExpression ce) return NormalizeConstant(ce.Value);
        var lambda = Expression.Lambda(Expression.Convert(expr, typeof(object)));
        var compiled = (Func<object?>)lambda.Compile();
        return NormalizeConstant(compiled());
    }

    private static object? NormalizeConstant(object? v)
    {
        if (v is null) return null;
        // Keep JSON-friendly primitive types
        switch (v)
        {
            case bool or string or short or int or long or decimal or double or float:
                return v;
            case DateTime dt:
                return dt.ToString("o", CultureInfo.InvariantCulture);
            case DateTimeOffset dto:
                return dto.ToString("o", CultureInfo.InvariantCulture);
            case Guid g:
                return g.ToString();
            default:
                return v.ToString();
        }
    }
}
