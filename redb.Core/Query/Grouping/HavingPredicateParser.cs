using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
///   Agg.Sum/Average/Min/Max(g, x =&gt; x.Field)   — <c>{"$sum":{"$field":"Field"}}</c>
///   binary comparisons (>, &lt;, &gt;=, &lt;=, ==, !=) and &amp;&amp; / ||
///
/// Captured constants and outer-scope values are evaluated via
/// <see cref="Expression.Lambda"/>.Compile() and emitted as
/// <c>{"$const":value}</c> nodes.
/// </summary>
internal static class HavingPredicateParser
{
    /// <summary>
    /// Compile predicate body to PVT JSON string.
    /// Returns null when predicate is null.
    /// </summary>
    public static string? ToJson<TKey, TProps>(
        Expression<Func<IRedbGrouping<TKey, TProps>, bool>>? predicate) where TProps : class, new()
    {
        if (predicate is null) return null;
        var node = TranslateBoolean(predicate.Body, predicate.Parameters[0]);
        return JsonSerializer.Serialize(node);
    }

    private static object TranslateBoolean(Expression expr, ParameterExpression groupParam)
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
                    ["$and"] = new[] { TranslateBoolean(be.Left, groupParam), TranslateBoolean(be.Right, groupParam) }
                };
            }
            case ExpressionType.OrElse:
            case ExpressionType.Or:
            {
                var be = (BinaryExpression)expr;
                return new Dictionary<string, object>
                {
                    ["$or"] = new[] { TranslateBoolean(be.Left, groupParam), TranslateBoolean(be.Right, groupParam) }
                };
            }
            case ExpressionType.Not:
            {
                var ue = (UnaryExpression)expr;
                return new Dictionary<string, object> { ["$not"] = TranslateBoolean(ue.Operand, groupParam) };
            }
            case ExpressionType.GreaterThan:
            case ExpressionType.GreaterThanOrEqual:
            case ExpressionType.LessThan:
            case ExpressionType.LessThanOrEqual:
            case ExpressionType.Equal:
            case ExpressionType.NotEqual:
                return TranslateComparison((BinaryExpression)expr, groupParam);
        }

        throw new NotSupportedException(
            "Unsupported HAVING expression node: " + expr.NodeType + " (" + expr + ")");
    }

    private static Dictionary<string, object> TranslateComparison(BinaryExpression be, ParameterExpression groupParam)
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

        var left = TranslateOperand(be.Left, groupParam);
        var right = TranslateOperand(be.Right, groupParam);
        return new Dictionary<string, object> { [opKey] = new[] { left, right } };
    }

    private static object TranslateOperand(Expression expr, ParameterExpression groupParam)
    {
        // Unwrap Convert (boxing for value types: e.g. (decimal)Agg.Sum(...))
        while (expr is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
            expr = u.Operand;

        if (expr is MethodCallExpression mc)
        {
            var agg = TryTranslateAggregate(mc, groupParam);
            if (agg != null) return agg;
        }

        // Treat anything not referencing the group parameter as a captured constant.
        if (!ReferencesParameter(expr, groupParam))
        {
            var value = EvaluateConstant(expr);
            return new Dictionary<string, object> { ["$const"] = value ?? (object)"null" };
        }

        throw new NotSupportedException(
            "Unsupported HAVING operand: " + expr + ". Only Agg.* aggregates and captured constants are allowed.");
    }

    private static object? TryTranslateAggregate(MethodCallExpression mc, ParameterExpression groupParam)
    {
        // Agg.* static calls
        if (mc.Method.DeclaringType == typeof(Agg) || (mc.Method.DeclaringType?.FullName == "redb.Core.Query.Aggregation.Agg"))
        {
            var funcName = mc.Method.Name;
            var opKey = funcName switch
            {
                "Sum" => "$sum",
                "Average" => "$avg",
                "Min" => "$min",
                "Max" => "$max",
                "Count" => "$count",
                _ => null
            };
            if (opKey is null) return null;

            // Group-overload form: Agg.<Func>(g, x => x.Field) or Agg.Count(g)
            // Standalone form (Agg.Sum(value)) is rejected — must be applied to a group.
            if (mc.Arguments.Count >= 1 && IsGroupReference(mc.Arguments[0], groupParam))
            {
                if (funcName == "Count")
                {
                    // Always COUNT(*) for groups
                    return new Dictionary<string, object> { [opKey] = "*" };
                }

                if (mc.Arguments.Count < 2)
                    throw new NotSupportedException("Agg." + funcName + " requires a selector lambda over the group.");

                var field = ExtractFieldPathFromLambda(mc.Arguments[1]);
                if (string.IsNullOrEmpty(field))
                    throw new NotSupportedException("Agg." + funcName + " selector must reference a single Props field.");

                return new Dictionary<string, object>
                {
                    [opKey] = new Dictionary<string, object> { ["$field"] = field! }
                };
            }
        }
        return null;
    }

    private static bool IsGroupReference(Expression expr, ParameterExpression groupParam)
    {
        while (expr is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
            expr = u.Operand;
        return expr == groupParam;
    }

    private static string? ExtractFieldPathFromLambda(Expression expr)
    {
        if (expr is UnaryExpression q && q.NodeType == ExpressionType.Quote)
            expr = q.Operand;

        if (expr is LambdaExpression lambda)
        {
            var body = lambda.Body;
            while (body is UnaryExpression conv &&
                   (conv.NodeType == ExpressionType.Convert || conv.NodeType == ExpressionType.ConvertChecked))
                body = conv.Operand;
            return ExtractMemberPath(body as MemberExpression);
        }
        return null;
    }

    private static string? ExtractMemberPath(MemberExpression? member)
    {
        if (member is null) return null;
        var parts = new List<string>();
        Expression? cur = member;
        while (cur is MemberExpression m)
        {
            if (m.Member.Name != "Props")
                parts.Insert(0, m.Member.Name);
            cur = m.Expression;
        }
        return parts.Count == 0 ? null : string.Join(".", parts);
    }

    private static bool ReferencesParameter(Expression expr, ParameterExpression p)
    {
        var v = new ParamRefVisitor(p);
        v.Visit(expr);
        return v.Found;
    }

    private sealed class ParamRefVisitor : ExpressionVisitor
    {
        private readonly ParameterExpression _p;
        public bool Found;
        public ParamRefVisitor(ParameterExpression p) { _p = p; }
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _p) Found = true;
            return base.VisitParameter(node);
        }
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
