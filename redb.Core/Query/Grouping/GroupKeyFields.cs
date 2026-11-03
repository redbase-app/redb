using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace redb.Core.Query.Grouping;

/// <summary>
/// The keys of a GroupBy over objects (flat, tree, grouped window): a stored field (<c>x =&gt; x.Department</c>,
/// <c>x =&gt; x.Address.City</c>, <c>x =&gt; x.Status.Id</c>) or an anonymous type of such fields. A computed key is
/// refused, never read as a path it is not.
/// </summary>
internal static class GroupKeyFields
{
    public static List<GroupFieldRequest> Parse(Expression keySelector, bool isBaseFieldGrouping, string ownerApi)
    {
        var lambda = keySelector as LambdaExpression
            ?? throw new NotSupportedException($"{ownerApi}: the key of the grouping must be a lambda; got '{keySelector}'.");
        var parameter = lambda.Parameters[0];
        var body = GroupSelectorMembers.StripConvert(lambda.Body);
        var result = new List<GroupFieldRequest>();

        if (body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var alias = newExpr.Members?[i].Name ?? $"Key{i}";
                result.Add(new GroupFieldRequest
                {
                    FieldPath = GroupAggregateParser.FieldPath(newExpr.Arguments[i], parameter, GroupItemKind.Props, ownerApi, $"key member '{alias}'"),
                    Alias = alias,
                    IsBaseField = isBaseFieldGrouping
                });
            }
            return result;
        }

        if (body is MemberExpression member)
        {
            result.Add(new GroupFieldRequest
            {
                FieldPath = GroupAggregateParser.FieldPath(member, parameter, GroupItemKind.Props, ownerApi, "the key"),
                Alias = member.Member.Name,
                IsBaseField = isBaseFieldGrouping
            });
            return result;
        }

        // G-3: a computed key used to give an empty GROUP BY without a word.
        throw new NotSupportedException(
            $"{ownerApi} supports a field (x => x.Category) or an anonymous type of fields (x => new {{ x.A, x.B }}); " +
            $"a computed key is not supported: '{lambda.Body}'.");
    }
}
