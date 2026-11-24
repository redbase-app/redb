using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace redb.Core.Query.Aggregation;

/// <summary>
/// Reads the aggregate requests of an AggregateAsync selector (<c>x => new { A = Agg.Sum(x.Props.Price), ... }</c>).
/// <see cref="RedbQueryable{TProps}.AggregateAsync{TResult}"/> runs what it returns, and the SQL preview of the
/// aggregate (ToAggregateSqlStringAsync) builds its statement from the same requests.
/// </summary>
internal static class AggregateSelectorParser
{
    /// <summary>One request per <see cref="Agg"/> call of the anonymous type, aliased by its member name.</summary>
    public static List<AggregateRequest> Parse(LambdaExpression selector)
    {
        var result = new List<AggregateRequest>();
        if (selector.Body is not NewExpression newExpr)
            return result;

        for (var i = 0; i < newExpr.Arguments.Count; i++)
        {
            if (newExpr.Arguments[i] is not MethodCallExpression methodCall || methodCall.Method.DeclaringType != typeof(Agg))
                continue;

            var function = methodCall.Method.Name switch
            {
                "Sum" => AggregateFunction.Sum,
                "Average" => AggregateFunction.Average,
                "Min" => AggregateFunction.Min,
                "Max" => AggregateFunction.Max,
                "Count" => AggregateFunction.Count,
                _ => throw new NotSupportedException($"Unknown aggregation: {methodCall.Method.Name}")
            };

            var fieldPath = "*"; // Count()
            var isBaseField = false;
            if (methodCall.Arguments.Count > 0)
            {
                fieldPath = ExtractFieldPath(methodCall.Arguments[0]);
                isBaseField = AggregateRequest.ReadsObjectField(methodCall.Arguments[0]);
            }

            result.Add(new AggregateRequest
            {
                FieldPath = fieldPath,
                Function = function,
                Alias = newExpr.Members?[i].Name ?? $"Item{i}",
                IsBaseField = isBaseField
            });
        }

        return result;
    }

    /// <summary>
    /// The field path of an aggregated value. Every single aggregate (SumAsync, AverageAsync, MinAsync, MaxAsync,
    /// GetStatisticsAsync) and every Agg.* call of AggregateAsync read their path here:
    ///   x.Props.Price              → "Price"
    ///   x.Props.Customer.Name      → "Customer.Name"
    ///   x.Props.Score.Value        → "Score"            (the Value of a nullable is the nullable)
    ///   x.Props.Items[2].Price     → "Items[2].Price"   (specific index)
    ///   x.Props.Items.Select(i => i.Price) → "Items[].Price" (all elements)
    ///   x.Props.PhoneBook["home"]  → "PhoneBook[home]"  (Dictionary key)
    ///   x.Props.Scores["a"].Value  → "Scores[a]"
    ///   x.Props.PhoneBook.Values   → "PhoneBook[]"      (all Dictionary values)
    /// A path that is not such a chain from the parameter of the selector (a method call, a value of the caller, a
    /// conditional) is refused with <see cref="NotSupportedException"/>: it used to be cut at the first node the walk
    /// did not know, and the aggregate read another field (Items[0].Rank as the object's own Rank).
    /// </summary>
    public static string ExtractFieldPath(Expression expr)
    {
        var parts = new List<string>();
        Walk(expr, expr, parts);
        if (parts.Count == 0)
            throw new NotSupportedException(Parsing.BaseFilterExpressionParser.UnsupportedFieldPathMessage(expr));
        return string.Join(".", parts);
    }

    private static void Walk(Expression? expr, Expression whole, List<string> parts)
    {
        switch (expr)
        {
            case UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert:
                Walk(convert.Operand, whole, parts);
                return;

            // The start of the chain: the parameter of the selector, or of a Select lambda inside it.
            case ParameterExpression:
                return;

            // x.Props of the object: the Props of a RedbObject add no part.
            case MemberExpression { Member.Name: "Props", Expression: ParameterExpression parameter }
                when typeof(redb.Core.Models.Contracts.IRedbObject).IsAssignableFrom(parameter.Type):
                return;

            // Score.Value of a Nullable<T> is the value of Score: the accessor adds no part.
            case MemberExpression { Member.Name: "Value", Expression: { } nullable }
                when Nullable.GetUnderlyingType(nullable.Type) != null:
                Walk(nullable, whole, parts);
                return;

            // Dictionary.Values: every value of the dictionary.
            case MemberExpression { Member.Name: "Values", Expression: { } dictionary } when IsDictionaryType(dictionary.Type):
                Walk(dictionary, whole, parts);
                AppendToLast(parts, "[]", whole);
                return;

            case MemberExpression { Expression: { } owner } member:
                Walk(owner, whole, parts);
                parts.Add(member.Member.Name);
                return;

            // Items[2] of an array.
            case BinaryExpression { NodeType: ExpressionType.ArrayIndex } arrayIndex:
                Walk(arrayIndex.Left, whole, parts);
                AppendToLast(parts, $"[{KeyOf(arrayIndex.Right, whole)}]", whole);
                return;

            // Items[2] of a list, PhoneBook["home"] of a dictionary.
            case MethodCallExpression { Method.Name: "get_Item", Object: { } indexed } indexer when indexer.Arguments.Count == 1:
                Walk(indexed, whole, parts);
                AppendToLast(parts, $"[{KeyOf(indexer.Arguments[0], whole)}]", whole);
                return;

            // Items.ElementAt(2).
            case MethodCallExpression { Method.Name: "ElementAt" } elementAt when elementAt.Method.DeclaringType == typeof(Enumerable):
                Walk(elementAt.Arguments[0], whole, parts);
                AppendToLast(parts, $"[{KeyOf(elementAt.Arguments[1], whole)}]", whole);
                return;

            // Items.Select(i => i.Price) and Items.Sum(i => i.Price): every element.
            case MethodCallExpression { Method.Name: "Select" or "Sum" or "Average" or "Min" or "Max" } overElements
                when overElements.Method.DeclaringType == typeof(Enumerable):
                Walk(overElements.Arguments[0], whole, parts);
                AppendToLast(parts, "[]", whole);
                if (overElements.Arguments.Count > 1 && StripQuote(overElements.Arguments[1]) is LambdaExpression lambda)
                    Walk(lambda.Body, whole, parts);
                return;

            default:
                throw new NotSupportedException(Parsing.BaseFilterExpressionParser.UnsupportedFieldPathMessage(whole));
        }
    }

    private static Expression StripQuote(Expression e) => e is UnaryExpression { NodeType: ExpressionType.Quote } q ? q.Operand : e;

    private static void AppendToLast(List<string> parts, string suffix, Expression whole)
    {
        if (parts.Count == 0)
            throw new NotSupportedException(Parsing.BaseFilterExpressionParser.UnsupportedFieldPathMessage(whole));
        parts[^1] += suffix;
    }

    /// <summary>
    /// An index or a key: a constant, or a value of the caller (a captured variable). A key read from the object
    /// itself is refused: the path would depend on the row.
    /// </summary>
    private static string KeyOf(Expression key, Expression whole)
    {
        if (ParameterFinder.Contains(key))
            throw new NotSupportedException(Parsing.BaseFilterExpressionParser.UnsupportedFieldPathMessage(whole));
        var value = key is ConstantExpression constant
            ? constant.Value
            : Expression.Lambda<Func<object?>>(Expression.Convert(key, typeof(object))).Compile()();
        return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "";
    }

    private sealed class ParameterFinder : ExpressionVisitor
    {
        private bool _found;

        public static bool Contains(Expression e)
        {
            var finder = new ParameterFinder();
            finder.Visit(e);
            return finder._found;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            _found = true;
            return node;
        }
    }

    /// <summary>
    /// Checks if a type is Dictionary&lt;K,V&gt; or IDictionary&lt;K,V&gt;
    /// </summary>
    private static bool IsDictionaryType(Type? type)
    {
        if (type == null) return false;
        if (type.IsGenericType)
        {
            var genericDef = type.GetGenericTypeDefinition();
            if (genericDef == typeof(Dictionary<,>) || genericDef == typeof(IDictionary<,>))
                return true;
        }
        return type.GetInterfaces().Any(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));
    }
}
