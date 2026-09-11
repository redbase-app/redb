using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Общий разбор селектора SelectAsync для всех группировщиков (G-1, решение владельца
/// 2026-09-04: DTO/MemberInit поддерживается ВЕЗДЕ, а не только в обычном GroupBy).
/// Селектор - анонимный тип (NewExpression) или DTO с инициализатором членов
/// (MemberInitExpression, parameterless конструктор); всё прочее - громкий отказ
/// (fail-closed, ревью 2026-09-03). Классификацию членов (Key/Agg/Win/клиентское значение)
/// делает каждый группировщик сам - здесь только форма.
/// </summary>
internal static class GroupSelectorMembers
{
    internal readonly record struct Member(string Name, Type Type, Expression Expr);

    public static List<Member> Extract(LambdaExpression selector, string ownerApi)
    {
        var members = new List<Member>();
        switch (selector.Body)
        {
            case NewExpression newExpr:
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var name = newExpr.Members?[i].Name ?? $"Item{i}";
                    var type = (newExpr.Members?[i] as System.Reflection.PropertyInfo)?.PropertyType
                               ?? newExpr.Arguments[i].Type;
                    members.Add(new Member(name, type, newExpr.Arguments[i]));
                }
                break;

            case MemberInitExpression initExpr:
                if (initExpr.NewExpression.Arguments.Count > 0)
                    throw new NotSupportedException(
                        $"{ownerApi}: DTO с параметрами конструктора не поддерживается - " +
                        "нужен parameterless конструктор и инициализатор членов.");
                foreach (var binding in initExpr.Bindings)
                {
                    if (binding is not MemberAssignment assignment)
                        throw new NotSupportedException(
                            $"{ownerApi}: биндинг '{binding.Member.Name}' ({binding.BindingType}) не поддерживается - " +
                            "только простые присваивания членов DTO.");
                    var type = (assignment.Member as System.Reflection.PropertyInfo)?.PropertyType
                               ?? assignment.Expression.Type;
                    members.Add(new Member(assignment.Member.Name, type, assignment.Expression));
                }
                break;

            default:
                throw new NotSupportedException(
                    $"{ownerApi} поддерживает анонимный тип (g => new {{ ... }}) или DTO с инициализатором " +
                    $"членов (g => new Dto {{ ... }}); получено: {selector.Body.NodeType}.");
        }
        return members;
    }

    public static Expression StripConvert(Expression expr)
    {
        while (expr is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
            expr = u.Operand;
        return expr;
    }

    /// <summary>Цепочка членов от параметра группы через .Key (g.Key / g.Key.X / g.Key.X.Id).</summary>
    public static bool IsKeyAccess(Expression expr, ParameterExpression groupParam)
    {
        var sawKey = false;
        var current = expr;
        while (current is MemberExpression m)
        {
            if (m.Member.Name == "Key") sawKey = true;
            current = m.Expression;
        }
        return sawKey && ReferenceEquals(current, groupParam);
    }

    public static bool ReferencesParameter(Expression expr, ParameterExpression parameter)
    {
        var finder = new ParameterFinder(parameter);
        finder.Visit(expr);
        return finder.Found;
    }

    private sealed class ParameterFinder : ExpressionVisitor
    {
        private readonly ParameterExpression _target;
        public bool Found { get; private set; }
        public ParameterFinder(ParameterExpression target) => _target = target;
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _target) Found = true;
            return base.VisitParameter(node);
        }
    }

    /// <summary>Сборка результата: анонимный тип - через его конструктор; DTO - Activator +
    /// присваивания членов (null для non-nullable value-типа оставляет default).</summary>
    public static TResult Construct<TResult>(LambdaExpression selector, object?[] values)
    {
        if (selector.Body is NewExpression newExpr && newExpr.Constructor != null)
            return (TResult)newExpr.Constructor.Invoke(values);

        var initExpr = (MemberInitExpression)selector.Body;
        var instance = Activator.CreateInstance(typeof(TResult))!;
        for (int i = 0; i < initExpr.Bindings.Count; i++)
        {
            var assignment = (MemberAssignment)initExpr.Bindings[i];
            var value = values[i];
            switch (assignment.Member)
            {
                case System.Reflection.PropertyInfo pi:
                    if (value == null && pi.PropertyType.IsValueType && Nullable.GetUnderlyingType(pi.PropertyType) == null)
                        break;
                    pi.SetValue(instance, value);
                    break;
                case System.Reflection.FieldInfo fi:
                    if (value == null && fi.FieldType.IsValueType && Nullable.GetUnderlyingType(fi.FieldType) == null)
                        break;
                    fi.SetValue(instance, value);
                    break;
            }
        }
        return (TResult)instance;
    }
}
