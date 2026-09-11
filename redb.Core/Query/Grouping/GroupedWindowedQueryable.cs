using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Window;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Queryable for applying window functions to grouped data.
/// Wraps grouped query and adds window function support.
/// </summary>
public class GroupedWindowedQueryable<TKey, TProps> : IGroupedWindowedQueryable<TKey, TProps>
    where TProps : class, new()
{
    private readonly IRedbQueryProvider _provider;
    private readonly long _schemeId;
    private readonly Expression _keySelector;
    private readonly GroupedWindowSpec<TKey, TProps> _windowSpec;
    private readonly string? _filterJson;
    private readonly FilterExpression? _filter;

    /// <summary>
    /// Constructor with filterJson (legacy compatibility).
    /// </summary>
    public GroupedWindowedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        Expression keySelector,
        GroupedWindowSpec<TKey, TProps> windowSpec,
        string? filterJson = null)
    {
        _provider = provider;
        _schemeId = schemeId;
        _keySelector = keySelector;
        _windowSpec = windowSpec;
        _filterJson = filterJson;
        _filter = null;
    }
    
    /// <summary>
    /// Constructor with FilterExpression (Pro version).
    /// </summary>
    public GroupedWindowedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        Expression keySelector,
        GroupedWindowSpec<TKey, TProps> windowSpec,
        FilterExpression? filter)
    {
        _provider = provider;
        _schemeId = schemeId;
        _keySelector = keySelector;
        _windowSpec = windowSpec;
        _filter = filter;
        _filterJson = null;
    }

    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var groupFields = ParseGroupFields();
        var aggregations = ParseAggregations(selector);
        var windowFuncs = ParseWindowFunctions(selector);
        var partitionBy = ParsePartitionBy();
        var orderBy = ParseOrderBy();

        // Use FilterExpression if available, otherwise filterJson
        var jsonResult = _filter != null
            ? await _provider.ExecuteGroupedWindowQueryAsync(_schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, _filter, cancellationToken: cancellationToken)
            : await _provider.ExecuteGroupedWindowQueryAsync(_schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, _filterJson, cancellationToken: cancellationToken);

        return MaterializeResults<TResult>(jsonResult, selector, groupFields);
    }

    public async Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var groupFields = ParseGroupFields();
        var aggregations = ParseAggregations(selector);
        var windowFuncs = ParseWindowFunctions(selector);
        var partitionBy = ParsePartitionBy();
        var orderBy = ParseOrderBy();

        // Use FilterExpression if available, otherwise filterJson
        return _filter != null
            ? await _provider.GetGroupedWindowSqlPreviewAsync(
                _schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, _filter, cancellationToken: cancellationToken)
            : await _provider.GetGroupedWindowSqlPreviewAsync(
                _schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, _filterJson, cancellationToken: cancellationToken);
    }

    private List<GroupFieldRequest> ParseGroupFields()
    {
        var result = new List<GroupFieldRequest>();
        
        var body = _keySelector is LambdaExpression lambda ? lambda.Body : _keySelector;
        
        if (body is NewExpression newExpr && newExpr.Members != null)
        {
            for (int i = 0; i < newExpr.Members.Count; i++)
            {
                var member = newExpr.Members[i];
                var arg = newExpr.Arguments[i];
                var fieldPath = ExtractFieldPath(arg);
                result.Add(new GroupFieldRequest { FieldPath = fieldPath, Alias = member.Name });
            }
        }
        else if (body is MemberExpression memberExpr)
        {
            var fieldPath = ExtractFieldPath(memberExpr);
            result.Add(new GroupFieldRequest { FieldPath = fieldPath, Alias = memberExpr.Member.Name });
        }
        
        return result;
    }

    private List<AggregateRequest> ParseAggregations(LambdaExpression selector)
    {
        var result = new List<AggregateRequest>();
        var param = selector.Parameters[0];

        foreach (var (name, _, rawExpr) in GroupSelectorMembers.Extract(selector, "Оконный SelectAsync"))
        {
            var expr = GroupSelectorMembers.StripConvert(rawExpr);

            if (GroupSelectorMembers.IsKeyAccess(expr, param))
                continue;

            if (expr is MethodCallExpression methodCall)
            {
                var funcName = methodCall.Method.Name;
                var declaringType = methodCall.Method.DeclaringType;

                if (declaringType == typeof(Win))
                    continue; // оконная функция - её разбирает ParseWindowFunctions

                if (declaringType?.Name == "Agg" &&
                    funcName is "Sum" or "Average" or "Min" or "Max" or "Count")
                {
                    var fieldPath = methodCall.Arguments.Count > 1
                        ? ExtractFieldPathFromLambda(methodCall.Arguments[1])
                        : null;
                    result.Add(new AggregateRequest
                    {
                        Function = Enum.Parse<AggregateFunction>(funcName),
                        FieldPath = fieldPath ?? "",
                        Alias = name
                    });
                    continue;
                }

                if (declaringType == typeof(IRedbGrouping<TKey, TProps>) &&
                    funcName is "Sum" or "Avg" or "Min" or "Max" or "Count")
                {
                    var fieldPath = methodCall.Arguments.Count > 0
                        ? ExtractFieldPathFromLambda(methodCall.Arguments[0])
                        : null;
                    result.Add(new AggregateRequest
                    {
                        Function = Enum.Parse<AggregateFunction>(funcName),
                        FieldPath = fieldPath ?? "",
                        Alias = name
                    });
                    continue;
                }
            }

            if (expr is MemberExpression && GroupSelectorMembers.ReferencesParameter(expr, param))
                continue; // путь поля (e.Props.X) - материализуется по имени/алиасу

            if (!GroupSelectorMembers.ReferencesParameter(expr, param))
                continue; // клиентское значение - вычислится при материализации

            throw new NotSupportedException(
                $"Оконный SelectAsync: член '{name}' использует строку/группу, но не является полем, " +
                $"g.Key, Agg.*, методом группы или Win.* ('{expr}').");
        }

        return result;
    }


    private List<WindowFuncRequest> ParseWindowFunctions(LambdaExpression selector)
    {
        var result = new List<WindowFuncRequest>();

        foreach (var (name, _, rawExpr) in GroupSelectorMembers.Extract(selector, "Оконный SelectAsync"))
        {
            if (GroupSelectorMembers.StripConvert(rawExpr) is MethodCallExpression methodCall &&
                methodCall.Method.DeclaringType == typeof(Win))
            {
                string? fieldPath = null;
                if (methodCall.Arguments.Count > 0)
                    fieldPath = ExtractFieldPathFromLambda(methodCall.Arguments[0]);

                result.Add(new WindowFuncRequest
                {
                    Func = methodCall.Method.Name,
                    FieldPath = fieldPath ?? "",
                    Alias = name
                });
            }
        }

        return result;
    }


    private List<WindowFieldRequest> ParsePartitionBy()
    {
        var result = new List<WindowFieldRequest>();
        
        foreach (var expr in _windowSpec.PartitionByExpressions)
        {
            if (expr.Body is MemberExpression memberExpr)
            {
                result.Add(new WindowFieldRequest { FieldPath = memberExpr.Member.Name, Alias = memberExpr.Member.Name });
            }
        }
        
        return result;
    }

    private List<WindowOrderRequest> ParseOrderBy()
    {
        var result = new List<WindowOrderRequest>();
        
        foreach (var (expr, desc) in _windowSpec.OrderByExpressions)
        {
            // OrderBy can reference aggregations like Agg.Sum(g, x => x.Supply) or g.Sum(x => x.Supply)
            if (expr.Body is MethodCallExpression methodCall)
            {
                var funcName = methodCall.Method.Name;
                var declaringType = methodCall.Method.DeclaringType;
                string? fieldPath = null;
                
                // Agg.Sum(g, x => x.Field) - field is in Arguments[1]
                if (declaringType?.Name == "Agg" && methodCall.Arguments.Count > 1)
                {
                    fieldPath = ExtractFieldPathFromLambda(methodCall.Arguments[1]);
                }
                // g.Sum(x => x.Field) - field is in Arguments[0]  
                else if (methodCall.Arguments.Count > 0)
                {
                    fieldPath = ExtractFieldPathFromLambda(methodCall.Arguments[0]);
                }
                // Use aggregation alias pattern
                result.Add(new WindowOrderRequest { FieldPath = $"Agg_{funcName}_{fieldPath ?? "Count"}", Descending = desc });
            }
            else if (expr.Body is MemberExpression memberExpr)
            {
                result.Add(new WindowOrderRequest { FieldPath = memberExpr.Member.Name, Descending = desc });
            }
        }
        
        return result;
    }

    private string ExtractFieldPath(Expression expr)
    {
        if (expr is MemberExpression memberExpr)
        {
            var path = new List<string>();
            var current = memberExpr;
            
            while (current != null)
            {
                if (current.Member.Name != "Props")
                    path.Insert(0, current.Member.Name);
                current = current.Expression as MemberExpression;
            }
            
            return string.Join(".", path);
        }
        
        return expr.ToString();
    }

    private string? ExtractFieldPathFromLambda(Expression expr)
    {
        if (expr is UnaryExpression unary)
            expr = unary.Operand;
            
        if (expr is LambdaExpression lambda)
            return ExtractFieldPath(lambda.Body);
            
        return ExtractFieldPath(expr);
    }

    private List<TResult> MaterializeResults<TResult>(JsonDocument? jsonDoc, LambdaExpression selector, List<GroupFieldRequest> groupFields)
    {
        var results = new List<TResult>();
        if (jsonDoc == null) return results;

        var members = GroupSelectorMembers.Extract(selector, "Оконный SelectAsync");
        var param = selector.Parameters[0];

        // Клиентские значения - раз на запрос. Win/Agg-вызовы клиентскими не считаются, даже
        // когда не ссылаются на параметр (Win.RowNumber()): их значение приходит из JSON.
        var clientValues = new object?[members.Count];
        var isClient = new bool[members.Count];
        for (int i = 0; i < members.Count; i++)
        {
            var e = GroupSelectorMembers.StripConvert(members[i].Expr);
            var serverCall = e is MethodCallExpression mcx &&
                (mcx.Method.DeclaringType == typeof(Win) || mcx.Method.DeclaringType?.Name == "Agg");
            if (!serverCall && !GroupSelectorMembers.ReferencesParameter(e, param))
            {
                isClient[i] = true;
                clientValues[i] = Expression.Lambda(members[i].Expr).Compile().DynamicInvoke();
            }
        }

        foreach (var element in jsonDoc.RootElement.EnumerateArray())
        {
            var values = new object?[members.Count];
            for (int i = 0; i < members.Count; i++)
            {
                if (isClient[i]) { values[i] = clientValues[i]; continue; }

                var (name, type, rawExpr) = members[i];
                if (element.TryGetProperty(name, out var prop))
                    values[i] = ConvertJsonValue(prop, type);
                else
                {
                    var jsonAlias = ExtractJsonAliasFromArgument(GroupSelectorMembers.StripConvert(rawExpr), groupFields);
                    if (!string.IsNullOrEmpty(jsonAlias) && element.TryGetProperty(jsonAlias, out prop))
                        values[i] = ConvertJsonValue(prop, type);
                    else
                        values[i] = GetDefaultValue(type);
                }
            }
            results.Add(GroupSelectorMembers.Construct<TResult>(selector, values));
        }

        return results;
    }


    /// <summary>
    /// Extracts JSON field alias from selector argument.
    /// Handles patterns:
    ///   g.Key (single key) → returns the single GroupBy alias
    ///   g.Key.Department (composite key) → returns alias for "Department"
    ///   g.Key.SomeField.Id → returns "SomeField" (parent of .Id)
    ///   g.Key.Id (single ListItem key) → returns the single GroupBy alias
    /// </summary>
    private string? ExtractJsonAliasFromArgument(Expression arg, List<GroupFieldRequest> groupFields)
    {
        if (arg is MemberExpression member)
        {
            // g.Key → single key → return the only GroupBy alias
            if (member.Member.Name == "Key" && groupFields.Count == 1)
            {
                return groupFields[0].Alias;
            }

            // g.Key.SomeField.Id → return parent field name "SomeField"
            // g.Key.Id (single key) → return the only GroupBy alias
            if (member.Member.Name == "Id" && member.Expression is MemberExpression idParent)
            {
                if (idParent.Member.Name == "Key" && groupFields.Count == 1)
                    return groupFields[0].Alias;
                return idParent.Member.Name;
            }

            // g.Key.Department (composite key) → find matching GroupBy alias
            if (member.Expression is MemberExpression keyAccess && keyAccess.Member.Name == "Key")
            {
                var fieldName = member.Member.Name;
                var matchingGroup = groupFields.FirstOrDefault(g =>
                    g.Alias == fieldName || g.FieldPath == fieldName);
                return matchingGroup?.Alias ?? fieldName;
            }
        }
        return null;
    }

    private object? ConvertJsonValue(JsonElement element, Type targetType)
    {
        // Unwrap Nullable<T> so projection members like bool?/long? resolve to their core type.
        var t = Nullable.GetUnderlyingType(targetType) ?? targetType;
        return element.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            // SQLite stores bool as INTEGER 0/1 → JSON Number (PG returns true/false handled above).
            JsonValueKind.Number when t == typeof(bool) => element.GetInt32() != 0,
            JsonValueKind.Number when t == typeof(long) => element.GetInt64(),
            JsonValueKind.Number when t == typeof(int) => element.GetInt32(),
            JsonValueKind.Number when t == typeof(decimal) => element.GetDecimal(),
            JsonValueKind.Number when t == typeof(double) => element.GetDouble(),
            JsonValueKind.Number when t == typeof(float) => (float)element.GetDouble(),
            // SQLite returns bool/Guid/DateTimeOffset as TEXT for some expressions.
            JsonValueKind.String when t == typeof(bool) =>
                element.GetString() is var s && (s == "1" || string.Equals(s, "true", StringComparison.OrdinalIgnoreCase)),
            JsonValueKind.String when t == typeof(Guid) =>
                element.TryGetGuid(out var g) ? g : (object?)element.GetString(),
            JsonValueKind.String when t == typeof(DateTimeOffset) =>
                element.TryGetDateTimeOffset(out var d) ? d : (object?)element.GetString(),
            JsonValueKind.String => element.GetString(),
            _ => element.ToString()
        };
    }

    private object? GetDefaultValue(Type type) =>
        type.IsValueType ? Activator.CreateInstance(type) : null;
}
