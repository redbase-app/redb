using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Query.Aggregation;
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
    private readonly Expression _arraySelector;
    private readonly Expression _keySelector;
    
    public RedbArrayGroupedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        string? filterJson,
        Expression<Func<TProps, IEnumerable<TItem>>> arraySelector,
        Expression<Func<TItem, TKey>> keySelector)
    {
        _provider = provider;
        _schemeId = schemeId;
        _filterJson = filterJson;
        _arraySelector = arraySelector;
        _keySelector = keySelector;
    }
    
    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector)
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
        
        var jsonResult = await _provider.ExecuteArrayGroupedAggregateAsync(
            _schemeId, arrayPath, groupFields, aggregations, _filterJson);
        
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
        return null;
    }
    
    public async Task<int> CountAsync()
    {
        var arrayPath = ExtractArrayPath();
        var groupFields = ParseGroupFields();
        var aggregations = new[] { new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } };
        
        var jsonResult = await _provider.ExecuteArrayGroupedAggregateAsync(
            _schemeId, arrayPath, groupFields, aggregations, _filterJson);
        
        if (jsonResult == null) return 0;
        return jsonResult.RootElement.GetArrayLength();
    }
    
    /// <summary>
    /// Returns SQL string for array GroupBy query.
    /// </summary>
    public Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector)
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
                    var arg = newExpr.Arguments[i];
                    var alias = newExpr.Members?[i]?.Name ?? $"Key{i}";
                    var path = ExtractFieldPath(arg as MemberExpression);
                    if (!string.IsNullOrEmpty(path))
                        fields.Add(new GroupFieldRequest { FieldPath = path, Alias = alias });
                }
            }
            else if (body is MemberExpression member)
            {
                // Single field: x => x.Category
                var path = ExtractFieldPath(member);
                if (!string.IsNullOrEmpty(path))
                    fields.Add(new GroupFieldRequest { FieldPath = path, Alias = member.Member.Name });
            }
        }
        
        return fields;
    }
    
    private List<AggregateRequest> ParseAggregations<TResult>(
        Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector)
    {
        var aggregations = new List<AggregateRequest>();
        
        if (selector.Body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = newExpr.Arguments[i];
                var alias = newExpr.Members?[i]?.Name ?? $"agg{i}";
                
                // Skip g.Key
                if (arg is MemberExpression me && me.Member.Name == "Key")
                    continue;
                
                // Agg.Sum(g, x => x.Field), Agg.Count(g)
                if (arg is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Agg))
                {
                    var funcName = mc.Method.Name;
                    string fieldPath = "*";
                    
                    if (mc.Arguments.Count > 1)
                    {
                        fieldPath = ExtractFieldPathFromLambda(mc.Arguments[1]) ?? "*";
                    }
                    
                    var function = funcName switch
                    {
                        "Sum" => AggregateFunction.Sum,
                        "Average" => AggregateFunction.Average,
                        "Min" => AggregateFunction.Min,
                        "Max" => AggregateFunction.Max,
                        "Count" => AggregateFunction.Count,
                        _ => AggregateFunction.Count
                    };
                    
                    aggregations.Add(new AggregateRequest 
                    { 
                        FieldPath = fieldPath, 
                        Function = function, 
                        Alias = alias 
                    });
                }
            }
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
    
    private List<TResult> MaterializeResults<TResult>(JsonDocument json, Expression selector)
    {
        var results = new List<TResult>();
        var root = json.RootElement;
        
        if (root.ValueKind != JsonValueKind.Array) return results;
        
        var resultType = typeof(TResult);
        var ctor = resultType.GetConstructors().FirstOrDefault();
        
        foreach (var item in root.EnumerateArray())
        {
            if (ctor != null && ctor.GetParameters().Length > 0)
            {
                var args = new List<object?>();
                foreach (var param in ctor.GetParameters())
                {
                    // Case-insensitive property search in JSON
                    var jsonProp = item.EnumerateObject()
                        .FirstOrDefault(p => string.Equals(p.Name, param.Name, StringComparison.OrdinalIgnoreCase));
                    
                    if (jsonProp.Value.ValueKind != JsonValueKind.Undefined)
                        args.Add(JsonValueConverter.Convert(jsonProp.Value, param.ParameterType));
                    else
                        args.Add(JsonValueConverter.GetDefault(param.ParameterType));
                }
                results.Add((TResult)ctor.Invoke(args.ToArray()));
            }
        }
        
        return results;
    }
}
