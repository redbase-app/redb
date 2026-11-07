using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Text.Json;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Translates the condition of a conditional count (<c>Agg.Count(g, c =&gt; c.IsVerified &amp;&amp; c.Type == "email")</c>)
/// into the boolean JSON that HAVING already uses, so every generator reads one language:
/// <c>$and/$or/$not</c>, <c>$eq/$ne/$gt/$gte/$lt/$lte</c>, <c>$isnull/$notnull</c>, <c>$field</c>, <c>$const</c>.
/// Any other form is refused with <see cref="Supported"/> in the message.
/// </summary>
internal static class ItemPredicateParser
{
    /// <summary>What a condition may contain; part of every refusal.</summary>
    public const string Supported =
        "a condition of Agg.Count may compare a field with a constant or with another field (==, !=, <, <=, >, >=), " +
        "combine comparisons with &&, || and !, test a field for null (x.F == null, x.F != null, x.F.HasValue) " +
        "and use a boolean field alone";

    /// <summary>JSON of the condition, or a <see cref="NotSupportedException"/> naming the part it cannot translate.</summary>
    public static string ToJson(LambdaExpression predicate, GroupItemKind kind, string ownerApi)
        => JsonSerializer.Serialize(new Translator(predicate.Parameters[0], kind, ownerApi).Boolean(predicate.Body));

    private sealed class Translator
    {
        private readonly ParameterExpression _param;
        private readonly GroupItemKind _kind;
        private readonly string _ownerApi;

        public Translator(ParameterExpression param, GroupItemKind kind, string ownerApi)
        {
            _param = param;
            _kind = kind;
            _ownerApi = ownerApi;
        }

        public object Boolean(Expression expr)
        {
            expr = GroupSelectorMembers.StripConvert(expr);
            switch (expr.NodeType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.And when expr.Type == typeof(bool):
                    return Node("$and", new[] { Boolean(((BinaryExpression)expr).Left), Boolean(((BinaryExpression)expr).Right) });
                case ExpressionType.OrElse:
                case ExpressionType.Or when expr.Type == typeof(bool):
                    return Node("$or", new[] { Boolean(((BinaryExpression)expr).Left), Boolean(((BinaryExpression)expr).Right) });
                case ExpressionType.Not:
                    return Node("$not", Boolean(((UnaryExpression)expr).Operand));
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                    return Comparison((BinaryExpression)expr);
            }

            // x.F.HasValue
            if (expr is MemberExpression { Member.Name: "HasValue" } hasValue && hasValue.Expression is { } inner
                && Nullable.GetUnderlyingType(inner.Type) != null && IsField(inner))
                return Node("$notnull", Field(inner));

            // A boolean field alone: x.IsVerified, or the element of a bool[] (x => x).
            if ((expr.Type == typeof(bool) || expr.Type == typeof(bool?)) && IsField(expr))
                return Node("$eq", new object[] { Field(expr), Const(true) });

            throw Refuse(expr);
        }

        private object Comparison(BinaryExpression be)
        {
            var op = be.NodeType switch
            {
                ExpressionType.Equal => "$eq",
                ExpressionType.NotEqual => "$ne",
                ExpressionType.GreaterThan => "$gt",
                ExpressionType.GreaterThanOrEqual => "$gte",
                ExpressionType.LessThan => "$lt",
                _ => "$lte"
            };

            var leftIsField = IsField(be.Left);
            var rightIsField = IsField(be.Right);
            if (!leftIsField && !rightIsField)
                throw Refuse(be);

            var fieldSide = leftIsField ? be.Left : be.Right;
            var otherSide = leftIsField ? be.Right : be.Left;

            if (!ReferencesParameter(otherSide) && Evaluate(otherSide) is null)
            {
                return op switch
                {
                    "$eq" => Node("$isnull", Field(fieldSide)),
                    "$ne" => Node("$notnull", Field(fieldSide)),
                    _ => throw new NotSupportedException(
                        $"{_ownerApi}: '{be}' orders a field against null; {Supported}.")
                };
            }

            var left = Operand(be.Left, be.Right);
            var right = Operand(be.Right, be.Left);
            return Node(op, new[] { left, right });
        }

        private object Operand(Expression side, Expression otherSide)
        {
            if (IsField(side))
                return Field(side);
            if (ReferencesParameter(side))
                throw Refuse(side);

            var value = Evaluate(side);
            // C# compares an enum through its number: Convert(x.Kind, Int32) == 2. Stored enums are their names.
            var enumType = EnumTypeOf(otherSide);
            if (enumType != null && value != null && !(value is string))
                value = Enum.ToObject(enumType, value).ToString();
            return Const(Normalize(value!));
        }

        private bool IsField(Expression expr)
        {
            expr = GroupSelectorMembers.StripConvert(expr);
            if (expr == _param)
                return _kind == GroupItemKind.ArrayValueElement;
            return expr is MemberExpression m && GroupSelectorMembers.IsFieldChain(m)
                   && GroupAggregateParser.TryMemberPath(m, _param, out _);
        }

        private object Field(Expression expr)
            => Node("$field", GroupAggregateParser.FieldPath(expr, _param, _kind, _ownerApi, "a field in the condition of Agg.Count"));

        private static Type? EnumTypeOf(Expression expr)
        {
            var stripped = GroupSelectorMembers.StripConvert(expr);
            var t = Nullable.GetUnderlyingType(stripped.Type) ?? stripped.Type;
            return t.IsEnum ? t : null;
        }

        private bool ReferencesParameter(Expression expr) => GroupSelectorMembers.ReferencesParameter(expr, _param);

        private static object? Evaluate(Expression expr)
        {
            if (expr is ConstantExpression ce) return ce.Value;
            return Expression.Lambda<Func<object?>>(Expression.Convert(expr, typeof(object))).Compile()();
        }

        private static object Normalize(object value) => value switch
        {
            bool or string or short or int or long or decimal or double or float or byte => value,
            char c => c.ToString(),
            DateTime dt => dt.ToString("o", CultureInfo.InvariantCulture),
            DateTimeOffset dto => dto.ToString("o", CultureInfo.InvariantCulture),
            Guid g => g.ToString(),
            Enum e => e.ToString(),
            _ => throw new NotSupportedException(
                $"A constant of type {value.GetType().Name} cannot be used in the condition of Agg.Count; {Supported}.")
        };

        private static Dictionary<string, object> Const(object value) => Node("$const", value);

        private static Dictionary<string, object> Node(string key, object value) => new() { [key] = value };

        private NotSupportedException Refuse(Expression expr)
            => new($"{_ownerApi}: '{expr}' cannot be translated; {Supported}.");
    }
}
