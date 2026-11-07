using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using redb.Core.Query.Aggregation;

namespace redb.Core.Query.Grouping;

/// <summary>
/// What the parameter of an aggregate or condition lambda stands for in a grouping.
/// </summary>
internal enum GroupItemKind
{
    /// <summary>An object's Props (GroupBy, tree GroupBy, grouped window).</summary>
    Props,

    /// <summary>An element of an array of classes (GroupByArray over Contacts).</summary>
    ArrayClassElement,

    /// <summary>An element of an array of simple values (GroupByArray over Skills): the element itself is a value.</summary>
    ArrayValueElement
}

/// <summary>
/// One strict reading of <c>Agg.*</c> calls for every grouping (GroupBy, tree GroupBy, grouped window, GroupByArray).
/// A call it cannot translate is refused with <see cref="NotSupportedException"/>; nothing becomes <c>"*"</c> or a
/// path the caller did not write.
/// </summary>
internal static class GroupAggregateParser
{
    /// <summary>Whether the expression is a call of a method of <see cref="Agg"/>.</summary>
    public static bool IsAggCall(Expression expression)
        => GroupSelectorMembers.StripConvert(expression) is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Agg);

    /// <summary>
    /// Reads one <c>Agg.*</c> call over the group parameter into an aggregate request.
    /// </summary>
    /// <param name="call">The call, already known to be a method of <see cref="Agg"/>.</param>
    /// <param name="groupParam">Parameter of the group in the selector or HAVING lambda.</param>
    /// <param name="alias">Alias of the result column.</param>
    /// <param name="kind">What the lambda parameter of the call stands for.</param>
    /// <param name="ownerApi">Name of the API for messages, e.g. "GroupByArray.SelectAsync".</param>
    public static AggregateRequest Parse(
        MethodCallExpression call, ParameterExpression groupParam, string alias, GroupItemKind kind, string ownerApi)
    {
        var name = call.Method.Name;
        var function = name switch
        {
            "Sum" => AggregateFunction.Sum,
            "Average" => AggregateFunction.Average,
            "Min" => AggregateFunction.Min,
            "Max" => AggregateFunction.Max,
            "Count" => AggregateFunction.Count,
            _ => throw new NotSupportedException($"{ownerApi}: Agg.{name} is not an aggregate of a group.")
        };

        if (call.Arguments.Count == 0 || GroupSelectorMembers.StripConvert(call.Arguments[0]) != groupParam)
            throw new NotSupportedException(
                $"{ownerApi}: Agg.{name} takes the group as its first argument (Agg.{name}(g, ...)); got '{call}'.");

        if (function == AggregateFunction.Count)
        {
            if (call.Arguments.Count == 1)
                return new AggregateRequest { FieldPath = "*", Function = function, Alias = alias };
            var predicate = UnwrapLambda(call.Arguments[1])
                ?? throw new NotSupportedException($"{ownerApi}: the condition of Agg.Count must be a lambda; got '{call.Arguments[1]}'.");
            return new AggregateRequest
            {
                FieldPath = "*",
                Function = function,
                Alias = alias,
                ConditionJson = ItemPredicateParser.ToJson(predicate, kind, ownerApi)
            };
        }

        if (call.Arguments.Count < 2)
            throw new NotSupportedException($"{ownerApi}: Agg.{name} needs a selector over the group: Agg.{name}(g, x => x.Field).");

        var selector = UnwrapLambda(call.Arguments[1])
            ?? throw new NotSupportedException($"{ownerApi}: the selector of Agg.{name} must be a lambda; got '{call.Arguments[1]}'.");
        return new AggregateRequest
        {
            FieldPath = FieldPath(selector, kind, ownerApi, $"the selector of Agg.{name}"),
            Function = function,
            Alias = alias
        };
    }

    /// <summary>
    /// The stored field a lambda names: a chain of members from its parameter (<c>x =&gt; x.Address.City</c>), or the
    /// parameter itself for an element of an array of simple values (<see cref="GroupFieldRequest.ElementPath"/>).
    /// Anything else is a computation and is refused.
    /// </summary>
    public static string FieldPath(LambdaExpression lambda, GroupItemKind kind, string ownerApi, string what)
        => FieldPath(lambda.Body, lambda.Parameters[0], kind, ownerApi, what);

    /// <summary>
    /// The stored field an expression over <paramref name="parameter"/> names; see <see cref="FieldPath(LambdaExpression, GroupItemKind, string, string)"/>.
    /// </summary>
    public static string FieldPath(Expression body, ParameterExpression parameter, GroupItemKind kind, string ownerApi, string what)
    {
        var expr = GroupSelectorMembers.StripConvert(body);
        if (expr == parameter)
        {
            if (kind == GroupItemKind.ArrayValueElement)
                return GroupFieldRequest.ElementPath;
            throw new NotSupportedException(kind == GroupItemKind.ArrayClassElement
                ? $"{ownerApi}: {what} names the whole element of an array of classes; name a field of the element (x => x.Field)."
                : $"{ownerApi}: {what} names the whole object; name a field (x => x.Field).");
        }

        if (expr is MemberExpression member && GroupSelectorMembers.IsFieldChain(member)
            && TryMemberPath(member, parameter, out var path))
            return path;

        throw new NotSupportedException(
            $"{ownerApi}: {what} must name a stored field ({Example(kind)}); '{body}' is a computation, which is not translated to SQL.");
    }

    /// <summary>
    /// Path of a member chain that starts at <paramref name="parameter"/>, joined with dots. A <c>Props</c> member and
    /// the <c>Value</c> of a <see cref="Nullable{T}"/> are not part of the stored path.
    /// </summary>
    public static bool TryMemberPath(MemberExpression member, ParameterExpression parameter, out string path)
    {
        var parts = new List<string>();
        Expression? current = member;
        while (current is MemberExpression m)
        {
            var declaring = m.Member.DeclaringType;
            var isNullableValue = m.Member.Name == "Value" && declaring is { IsGenericType: true }
                                  && declaring.GetGenericTypeDefinition() == typeof(Nullable<>);
            if (!isNullableValue && m.Member.Name != "Props")
                parts.Insert(0, m.Member.Name);
            current = m.Expression is null ? null : GroupSelectorMembers.StripConvert(m.Expression);
        }
        path = string.Join(".", parts);
        return current == parameter && parts.Count > 0;
    }

    private static string Example(GroupItemKind kind) => kind switch
    {
        GroupItemKind.ArrayValueElement => "x => x for the element itself",
        GroupItemKind.ArrayClassElement => "x => x.Field or x => x.Inner.Field of the element",
        _ => "x => x.Field or x => x.Inner.Field"
    };

    private static LambdaExpression? UnwrapLambda(Expression expression)
    {
        if (expression is UnaryExpression quote && quote.NodeType == ExpressionType.Quote)
            expression = quote.Operand;
        return expression as LambdaExpression;
    }
}
