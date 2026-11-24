using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using redb.Core.Models.Contracts;

namespace redb.Core.Query.Window;

/// <summary>
/// The field path of a window key, a selected field or a Win.* argument (flat and tree windows). A window reads plain
/// chains of fields: p.Category, p.Address.City, x.Props.Price, x.Name; the Value of a nullable is the nullable. Anything
/// else (an element by index, a dictionary entry, a method call) is refused with <see cref="NotSupportedException"/>:
/// the walk used to stop at the first node it did not know and partition or sort by the field after it, so
/// PartitionBy(p => p.Items[0].Name) partitioned by the object's own Name.
/// </summary>
internal static class WindowFieldPath
{
    public static string Of(Expression expression)
    {
        var parts = new List<string>();
        var current = StripConvert(expression);
        while (true)
        {
            switch (current)
            {
                case ParameterExpression when parts.Count > 0:
                    parts.Reverse();
                    return string.Join(".", parts);

                // x.Props of the object adds no part.
                case MemberExpression { Member.Name: "Props", Expression: ParameterExpression parameter }
                    when typeof(IRedbObject).IsAssignableFrom(parameter.Type):
                    current = parameter;
                    continue;

                // Score.Value of a Nullable<T> is Score.
                case MemberExpression { Member.Name: "Value", Expression: { } nullable }
                    when Nullable.GetUnderlyingType(nullable.Type) != null:
                    current = StripConvert(nullable);
                    continue;

                case MemberExpression { Expression: { } owner } member:
                    parts.Add(member.Member.Name);
                    current = StripConvert(owner);
                    continue;

                default:
                    throw new NotSupportedException(Parsing.BaseFilterExpressionParser.UnsupportedFieldPathMessage(expression));
            }
        }
    }

    private static Expression? StripConvert(Expression? e)
    {
        while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            e = unary.Operand;
        return e;
    }
}
