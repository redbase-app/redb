using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Utils;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Grouping by array elements (Items[].Property)
/// </summary>
public class RedbArrayGroupedQueryable<TKey, TItem, TProps> : IRedbGroupedQueryable<TKey, TItem>
    where TItem : class, new()
    where TProps : class, new()
{
    private readonly IRedbQueryProvider _provider;
    private readonly long _schemeId;
    private readonly string? _filterJson;
    private readonly FilterExpression? _filter;
    private readonly Expression _arraySelector;
    private readonly Expression _keySelector;
    private string? _havingJson;
    private readonly List<LambdaExpression> _havingPredicates = new();
    
    public RedbArrayGroupedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        string? filterJson,
        Expression<Func<TProps, IEnumerable<TItem>>> arraySelector,
        Expression<Func<TItem, TKey>> keySelector)
        : this(provider, schemeId, filterJson, null, arraySelector, keySelector)
    {
    }

    public RedbArrayGroupedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        string? filterJson,
        FilterExpression? filter,
        Expression<Func<TProps, IEnumerable<TItem>>> arraySelector,
        Expression<Func<TItem, TKey>> keySelector)
    {
        _provider = provider;
        _schemeId = schemeId;
        _filterJson = filterJson;
        _filter = filter;
        _arraySelector = arraySelector;
        _keySelector = keySelector;
    }
    
    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var arrayPath = ExtractArrayPath();
        var groupFields = ParseGroupFields();
        var aggregations = ParseAggregations(selector);
        
        // Extract alias for g.Key from selector and apply to groupFields
        var keyAlias = ExtractKeyAliasFromSelector(selector);
        if (!string.IsNullOrEmpty(keyAlias) && groupFields.Count > 0)
        {
            groupFields[0].Alias = keyAlias;
        }
        
        var havingJson = BuildHavingJson();
        var jsonResult = _filter != null
            ? await _provider.ExecuteArrayGroupedAggregateAsync(
                _schemeId, arrayPath, groupFields, aggregations, _filter, havingJson, cancellationToken: cancellationToken)
            : await _provider.ExecuteArrayGroupedAggregateAsync(
                _schemeId, arrayPath, groupFields, aggregations, _filterJson, havingJson, cancellationToken: cancellationToken);
        
        if (jsonResult == null) return new List<TResult>();
        return MaterializeResults<TResult>(jsonResult, selector);
    }
    
    private string? ExtractKeyAliasFromSelector<TResult>(Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector)
    {
        if (selector.Body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = newExpr.Arguments[i];
                // Look for g.Key
                if (arg is MemberExpression me && me.Member.Name == "Key")
                {
                    return newExpr.Members?[i]?.Name;
                }
            }
        }
        // G-1: DTO/MemberInit - ключ ищется и в биндингах.
        if (selector.Body is MemberInitExpression initExpr)
        {
            foreach (var binding in initExpr.Bindings)
            {
                if (binding is MemberAssignment ma &&
                    GroupSelectorMembers.StripConvert(ma.Expression) is MemberExpression kme &&
                    kme.Member.Name == "Key")
                    return binding.Member.Name;
            }
        }
        return null;
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        var arrayPath = ExtractArrayPath();
        var groupFields = ParseGroupFields();
        var aggregations = new[] { new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } };
        
        var havingJson = BuildHavingJson();
        var jsonResult = _filter != null
            ? await _provider.ExecuteArrayGroupedAggregateAsync(
                _schemeId, arrayPath, groupFields, aggregations, _filter, havingJson, cancellationToken: cancellationToken)
            : await _provider.ExecuteArrayGroupedAggregateAsync(
                _schemeId, arrayPath, groupFields, aggregations, _filterJson, havingJson, cancellationToken: cancellationToken);
        
        if (jsonResult == null) return 0;
        return jsonResult.RootElement.GetArrayLength();
    }
    
    /// <summary>
    /// Returns SQL string for array GroupBy query.
    /// </summary>
    public Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var arrayPath = ExtractArrayPath();
        var groupFields = ParseGroupFields();
        var aggregations = ParseAggregations(selector);
        
        // SQL preview not supported for array grouping yet
        return Task.FromResult(
            $"-- SQL preview not supported for array GroupBy\n" +
            $"-- SchemeId: {_schemeId}\n" +
            $"-- ArrayPath: {arrayPath}\n" +
            $"-- GroupFields: {string.Join(", ", groupFields.Select(g => g.FieldPath))}\n" +
            $"-- Aggregations: {string.Join(", ", aggregations.Select(a => $"{a.Function}({a.FieldPath})"))}\n" +
            $"-- FilterJson: {_filterJson ?? "null"}");
    }
    
    /// <summary>
    /// WithWindow not supported for array grouping.
    /// </summary>
    public IGroupedWindowedQueryable<TKey, TItem> WithWindow(
        Action<IGroupedWindowSpec<TKey, TItem>> windowConfig)
    {
        throw new NotSupportedException("WithWindow is not supported for array GroupBy. Use regular GroupBy instead.");
    }

    /// <inheritdoc />
    public IRedbGroupedQueryable<TKey, TItem> Having(
        Expression<Func<IRedbGrouping<TKey, TItem>, bool>> predicate)
    {
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        // G-4 (ревью 2026-09-03): копирующий строитель - ветвление не заражает соседнюю ветку.
        var copy = new RedbArrayGroupedQueryable<TKey, TItem, TProps>(
            _provider, _schemeId, _filterJson, _filter,
            (Expression<Func<TProps, IEnumerable<TItem>>>)_arraySelector,
            (Expression<Func<TItem, TKey>>)_keySelector);
        copy._havingPredicates.AddRange(_havingPredicates);
        copy._havingPredicates.Add(predicate);
        return copy;
    }


    private string? BuildHavingJson()
    {
        if (_havingJson != null) return _havingJson;
        if (_havingPredicates.Count == 0) return null;

        if (_havingPredicates.Count == 1)
        {
            var single = (Expression<Func<IRedbGrouping<TKey, TItem>, bool>>)_havingPredicates[0];
            _havingJson = HavingPredicateParser.ToJson(single);
            return _havingJson;
        }

        var param = Expression.Parameter(typeof(IRedbGrouping<TKey, TItem>), "g");
        Expression body = ReplaceParam((Expression<Func<IRedbGrouping<TKey, TItem>, bool>>)_havingPredicates[0], param);
        for (int i = 1; i < _havingPredicates.Count; i++)
        {
            var nextBody = ReplaceParam((Expression<Func<IRedbGrouping<TKey, TItem>, bool>>)_havingPredicates[i], param);
            body = Expression.AndAlso(body, nextBody);
        }
        var combined = Expression.Lambda<Func<IRedbGrouping<TKey, TItem>, bool>>(body, param);
        _havingJson = HavingPredicateParser.ToJson(combined);
        return _havingJson;
    }

    private static Expression ReplaceParam(
        Expression<Func<IRedbGrouping<TKey, TItem>, bool>> lambda,
        ParameterExpression newParam)
    {
        return new ParameterReplacer(lambda.Parameters[0], newParam).Visit(lambda.Body)!;
    }

    private sealed class ParameterReplacer : ExpressionVisitor
    {
        private readonly ParameterExpression _from;
        private readonly ParameterExpression _to;
        public ParameterReplacer(ParameterExpression from, ParameterExpression to) { _from = from; _to = to; }
        protected override Expression VisitParameter(ParameterExpression node) => node == _from ? _to : base.VisitParameter(node);
    }
    
    private string ExtractArrayPath()
    {
        if (_arraySelector is LambdaExpression lambda)
        {
            return ExtractFieldPath(lambda.Body as MemberExpression) ?? "";
        }
        return "";
    }
    
    private List<GroupFieldRequest> ParseGroupFields()
    {
        var fields = new List<GroupFieldRequest>();
        
        if (_keySelector is LambdaExpression lambda)
        {
            var body = lambda.Body;
            
            // Unwrap Convert
            while (body is UnaryExpression unary && 
                   (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
            {
                body = unary.Operand;
            }
            
            if (body is NewExpression newExpr)
            {
                // Multiple fields: x => new { x.A, x.B }
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = GroupSelectorMembers.StripConvert(newExpr.Arguments[i]);
                    var alias = newExpr.Members?[i]?.Name ?? $"Key{i}";
                    // GRP-9 (review 2026-09-24): a key member that is not an item field is refused. It used to be
                    // skipped, or read as a field path it is not, and the grouping ran on other keys without a word.
                    var path = arg is MemberExpression memberArg && GroupSelectorMembers.IsFieldChain(memberArg)
                        ? ExtractFieldPath(memberArg)
                        : null;
                    if (string.IsNullOrEmpty(path))
                        throw new NotSupportedException(
                            $"GroupByArray: key member '{alias}' is not a field of the array item ('{arg}'). Computed keys are not supported.");
                    fields.Add(new GroupFieldRequest { FieldPath = path, Alias = alias });
                }
            }
            else if (body is MemberExpression member)
            {
                // Single field: x => x.Category
                var path = GroupSelectorMembers.IsFieldChain(member) ? ExtractFieldPath(member) : null;
                if (string.IsNullOrEmpty(path))
                    throw new NotSupportedException(
                        $"GroupByArray: the key '{member}' is not a field of the array item. Computed keys are not supported.");
                fields.Add(new GroupFieldRequest { FieldPath = path, Alias = member.Member.Name });
            }
            else
            {
                throw new NotSupportedException(
                    "GroupByArray supports an item field (c => c.Type) or an anonymous type of item fields " +
                    $"(c => new {{ c.A, c.B }}). A computed key is not supported: '{body}'.");
            }
        }
        
        return fields;
    }
    
    private List<AggregateRequest> ParseAggregations<TResult>(
        Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector)
    {
        var aggregations = new List<AggregateRequest>();
        var groupParam = selector.Parameters[0];

        foreach (var (name, _, rawExpr) in GroupSelectorMembers.Extract(selector, "GroupByArray.SelectAsync"))
        {
            var expr = GroupSelectorMembers.StripConvert(rawExpr);

            if (GroupSelectorMembers.IsKeyAccess(expr, groupParam))
                continue;

            if (expr is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Agg))
            {
                var funcName = mc.Method.Name;
                var function = funcName switch
                {
                    "Sum" => AggregateFunction.Sum,
                    "Average" => AggregateFunction.Average,
                    "Min" => AggregateFunction.Min,
                    "Max" => AggregateFunction.Max,
                    "Count" => AggregateFunction.Count,
                    _ => throw new NotSupportedException($"Unknown aggregation: {funcName}")
                };

                string fieldPath = "*";
                if (mc.Arguments.Count > 1)
                    fieldPath = ExtractFieldPathFromLambda(mc.Arguments[1]) ?? "*";

                aggregations.Add(new AggregateRequest { FieldPath = fieldPath, Function = function, Alias = name });
                continue;
            }

            if (!GroupSelectorMembers.ReferencesParameter(expr, groupParam))
                continue; // клиентское значение - вычислится при материализации

            throw new NotSupportedException(
                $"GroupByArray.SelectAsync: член '{name}' использует группу, но не является ни g.Key, ни прямым Agg.* вызовом.");
        }

        return aggregations;
    }

    
    private string? ExtractFieldPathFromLambda(Expression expr)
    {
        if (expr is UnaryExpression quote && quote.NodeType == ExpressionType.Quote)
            expr = quote.Operand;
        
        if (expr is LambdaExpression lambda)
        {
            var body = lambda.Body;
            while (body is UnaryExpression unary &&
                   (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
            {
                body = unary.Operand;
            }
            return ExtractFieldPath(body as MemberExpression);
        }
        return null;
    }
    
    private string? ExtractFieldPath(MemberExpression? member)
    {
        if (member == null) return null;
        
        var parts = new List<string>();
        var current = member;
        
        while (current != null)
        {
            if (current.Member.Name == "Props" || current.Member.Name == "props")
                break;
            parts.Insert(0, current.Member.Name);
            current = current.Expression as MemberExpression;
        }
        
        return parts.Count > 0 ? string.Join(".", parts) : null;
    }
    
    private List<TResult> MaterializeResults<TResult>(JsonDocument json, LambdaExpression selector)
    {
        var results = new List<TResult>();
        var root = json.RootElement;

        if (root.ValueKind != JsonValueKind.Array) return results;

        var members = GroupSelectorMembers.Extract(selector, "GroupByArray.SelectAsync");
        var groupParam = selector.Parameters[0];

        var clientValues = new object?[members.Count];
        var isClient = new bool[members.Count];
        for (int i = 0; i < members.Count; i++)
        {
            if (!GroupSelectorMembers.ReferencesParameter(GroupSelectorMembers.StripConvert(members[i].Expr), groupParam))
            {
                isClient[i] = true;
                clientValues[i] = Expression.Lambda(members[i].Expr).Compile().DynamicInvoke();
            }
        }

        foreach (var item in root.EnumerateArray())
        {
            var values = new object?[members.Count];
            for (int i = 0; i < members.Count; i++)
            {
                if (isClient[i]) { values[i] = clientValues[i]; continue; }

                var (name, type, _) = members[i];
                // Case-insensitive поиск по JSON (сервер алиасует в своём регистре).
                var jsonProp = item.EnumerateObject()
                    .FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));
                values[i] = jsonProp.Value.ValueKind != JsonValueKind.Undefined
                    ? JsonValueConverter.Convert(jsonProp.Value, type)
                    : JsonValueConverter.GetDefault(type);
            }
            results.Add(GroupSelectorMembers.Construct<TResult>(selector, values));
        }

        return results;
    }

}
