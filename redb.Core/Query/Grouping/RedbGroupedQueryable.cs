using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Utils;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Implementation of REDB grouped queries
/// </summary>
public class RedbGroupedQueryable<TKey, TProps> : IRedbGroupedQueryable<TKey, TProps> 
    where TProps : class, new()
{
    private readonly IRedbQueryProvider _provider;
    private readonly long _schemeId;
    private readonly string? _filterJson;
    private readonly FilterExpression? _filter;
    private readonly Expression _keySelector;
    private readonly bool _isBaseFieldGrouping;
    
    /// <summary>
    /// Constructor with filterJson (Free version compatibility).
    /// </summary>
    public RedbGroupedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        string? filterJson,
        Expression keySelector,
        bool isBaseFieldGrouping = false)
    {
        _provider = provider;
        _schemeId = schemeId;
        _filterJson = filterJson;
        _filter = null;
        _keySelector = keySelector;
        _isBaseFieldGrouping = isBaseFieldGrouping;
    }
    
    /// <summary>
    /// Constructor with FilterExpression (Pro version - direct access to filter).
    /// </summary>
    public RedbGroupedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        FilterExpression? filter,
        Expression keySelector,
        bool isBaseFieldGrouping = false)
    {
        _provider = provider;
        _schemeId = schemeId;
        _filterJson = null;
        _filter = filter;
        _keySelector = keySelector;
        _isBaseFieldGrouping = isBaseFieldGrouping;
    }
    
    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        // 1. Parse grouping fields from _keySelector
        var groupFields = ParseGroupFields(_keySelector);
        
        // 2. Parse aggregations from selector
        var aggregations = ParseAggregations(selector);
        
        // 3. Execute SQL query (Pro uses FilterExpression directly, Free uses filterJson)
        var jsonResult = _filter != null
            ? await _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filter)
            : await _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filterJson);
        
        // 4. Materialize result
        return MaterializeResults<TResult>(jsonResult, selector);
    }
    
    public async Task<int> CountAsync()
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = new[] { new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } };
        
        // Pro uses FilterExpression directly, Free uses filterJson
        var jsonResult = _filter != null
            ? await _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filter)
            : await _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filterJson);
        
        if (jsonResult == null) return 0;
        return jsonResult.RootElement.GetArrayLength();
    }
    
    /// <summary>
    /// Returns SQL string that will be executed for this GroupBy query.
    /// Requires Pro version provider that supports SQL preview.
    /// </summary>
    public async Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = ParseAggregations(selector);
        
        // Check if provider supports SQL preview (Pro version)
        // Use specific overload based on whether we have FilterExpression or filterJson
        var providerType = _provider.GetType();
        System.Reflection.MethodInfo? getSqlMethod;
        object?[] methodArgs;
        
        if (_filter != null)
        {
            // Pro path: use FilterExpression overload
            getSqlMethod = providerType.GetMethod("GetGroupBySqlPreviewAsync", 
                new[] { typeof(long), typeof(IEnumerable<GroupFieldRequest>), typeof(IEnumerable<AggregateRequest>), typeof(FilterExpression) });
            methodArgs = new object?[] { _schemeId, groupFields, aggregations, _filter };
        }
        else
        {
            // Legacy path: use string overload
            getSqlMethod = providerType.GetMethod("GetGroupBySqlPreviewAsync", 
                new[] { typeof(long), typeof(IEnumerable<GroupFieldRequest>), typeof(IEnumerable<AggregateRequest>), typeof(string) });
            methodArgs = new object?[] { _schemeId, groupFields, aggregations, _filterJson };
        }
        
        if (getSqlMethod == null)
        {
            return $"-- SQL preview not supported by {providerType.Name}\n" +
                   $"-- SchemeId: {_schemeId}\n" +
                   $"-- GroupFields: {string.Join(", ", groupFields.Select(g => g.FieldPath))}\n" +
                   $"-- Aggregations: {string.Join(", ", aggregations.Select(a => $"{a.Function}({a.FieldPath})"))}\n" +
                   $"-- Filter: {(_filter != null ? "FilterExpression" : _filterJson ?? "null")}";
        }
        
        var task = getSqlMethod.Invoke(_provider, methodArgs) as Task<string>;
        return task != null ? await task : "-- SQL preview failed";
    }
    
    /// <summary>
    /// Apply window functions to grouped results.
    /// Allows ranking, running totals, and other analytics on aggregated data.
    /// Pro version receives FilterExpression directly for proper SQL compilation.
    /// </summary>
    public IGroupedWindowedQueryable<TKey, TProps> WithWindow(
        Action<IGroupedWindowSpec<TKey, TProps>> windowConfig)
    {
        var windowSpec = new GroupedWindowSpec<TKey, TProps>();
        windowConfig(windowSpec);
        // Pass FilterExpression directly (Pro uses it, Free falls back to facet-JSON)
        return new GroupedWindowedQueryable<TKey, TProps>(
            _provider, _schemeId, _keySelector, windowSpec, _filter);
    }
    
    /// <summary>
    /// Parses grouping key Expression into list of GroupFieldRequest
    /// </summary>
    private List<GroupFieldRequest> ParseGroupFields(Expression keySelector)
    {
        var result = new List<GroupFieldRequest>();
        
        if (keySelector is LambdaExpression lambda)
        {
            ParseGroupFieldsFromBody(lambda.Body, result);
        }
        
        return result;
    }
    
    private void ParseGroupFieldsFromBody(Expression body, List<GroupFieldRequest> result)
    {
        switch (body)
        {
            // Simple field: x => x.Category
            case MemberExpression member:
                var path = ExtractFieldPath(member);
                if (!string.IsNullOrEmpty(path))
                {
                    result.Add(new GroupFieldRequest 
                    { 
                        FieldPath = path, 
                        Alias = member.Member.Name,
                        IsBaseField = _isBaseFieldGrouping
                    });
                }
                break;
                
            // Anonymous type: x => new { x.Category, x.Year }
            case NewExpression newExpr:
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    var alias = newExpr.Members?[i].Name ?? $"Key{i}";
                    
                    if (arg is MemberExpression memberArg)
                    {
                        var fieldPath = ExtractFieldPath(memberArg);
                        if (!string.IsNullOrEmpty(fieldPath))
                        {
                            result.Add(new GroupFieldRequest 
                            { 
                                FieldPath = fieldPath, 
                                Alias = alias,
                                IsBaseField = _isBaseFieldGrouping
                            });
                        }
                    }
                }
                break;
        }
    }
    
    /// <summary>
    /// Parses aggregations from SelectAsync expression
    /// </summary>
    private List<AggregateRequest> ParseAggregations<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var result = new List<AggregateRequest>();
        
        if (selector.Body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = newExpr.Arguments[i];
                var alias = newExpr.Members?[i].Name ?? $"Agg{i}";
                
                // Skip g.Key - it's not an aggregation
                if (arg is MemberExpression member && member.Member.Name == "Key")
                    continue;
                
                // Agg.Sum(g, x => x.Field)
                if (arg is MethodCallExpression methodCall && 
                    methodCall.Method.DeclaringType == typeof(Agg))
                {
                    var funcName = methodCall.Method.Name;
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
                    if (methodCall.Arguments.Count >= 2)
                    {
                        fieldPath = ExtractFieldPathFromLambda(methodCall.Arguments[1]);
                    }
                    
                    result.Add(new AggregateRequest
                    {
                        FieldPath = fieldPath,
                        Function = function,
                        Alias = alias
                    });
                }
            }
        }
        
        return result;
    }
    
    /// <summary>
    /// Extracts field path from MemberExpression
    /// </summary>
    private string ExtractFieldPath(MemberExpression? member)
    {
        if (member == null) return string.Empty;
        
        var parts = new List<string>();
        Expression? current = member;
        
        while (current is MemberExpression m)
        {
            if (m.Member.Name != "Props")
                parts.Insert(0, m.Member.Name);
            current = m.Expression;
        }
        
        return string.Join(".", parts);
    }
    
    private string ExtractFieldPathFromLambda(Expression expr)
    {
        // Unwrap Quote if present (Expression&lt;Func&lt;...&gt;&gt;)
        if (expr is UnaryExpression quote && quote.NodeType == ExpressionType.Quote)
            expr = quote.Operand;
        
        if (expr is LambdaExpression lambda)
        {
            var body = lambda.Body;
            
            // Unwrap all Convert (for value types)
            while (body is UnaryExpression unary && 
                   (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked))
            {
                body = unary.Operand;
            }
            
            var path = ExtractFieldPath(body as MemberExpression);
            return string.IsNullOrEmpty(path) ? "*" : path;
        }
        
        return "*";
    }
    
    /// <summary>
    /// Materializes JSON result to List&lt;TResult&gt;
    /// </summary>
    private List<TResult> MaterializeResults<TResult>(
        JsonDocument? jsonResult,
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var results = new List<TResult>();
        if (jsonResult == null) return results;
        
        var compiledSelector = selector.Compile();
        
        foreach (var element in jsonResult.RootElement.EnumerateArray())
        {
            // Create RedbGroupingImpl with data from JSON
            var grouping = new RedbGroupingImpl<TKey, TProps>(element);
            
            // But for materialization we need a different approach - 
            // create TResult directly from JSON
            var result = MaterializeSingleResult<TResult>(element, selector);
            results.Add(result);
        }
        
        return results;
    }
    
    private TResult MaterializeSingleResult<TResult>(
        JsonElement element,
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        if (selector.Body is NewExpression newExpr)
        {
            var args = new object?[newExpr.Arguments.Count];
            
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var alias = newExpr.Members?[i].Name ?? $"Item{i}";
                var propType = newExpr.Members?[i] is System.Reflection.PropertyInfo pi 
                    ? pi.PropertyType 
                    : typeof(object);
                
                // Try direct property lookup first
                if (element.TryGetProperty(alias, out var prop))
                {
                    args[i] = JsonValueConverter.Convert(prop, propType);
                }
                else
                {
                    // Handle g.Key.SomeField.Id pattern - look for parent field name
                    var jsonAlias = ExtractJsonAliasFromArgument(newExpr.Arguments[i]);
                    if (!string.IsNullOrEmpty(jsonAlias) && element.TryGetProperty(jsonAlias, out prop))
                    {
                        args[i] = JsonValueConverter.Convert(prop, propType);
                    }
                }
            }
            
            // Create instance of anonymous type
            var ctor = newExpr.Constructor;
            if (ctor != null)
            {
                return (TResult)ctor.Invoke(args);
            }
        }
        
        return default!;
    }
    
    /// <summary>
    /// Extracts JSON field alias from selector argument.
    /// Handles patterns like g.Key.SomeField.Id -> returns "SomeField"
    /// </summary>
    private string? ExtractJsonAliasFromArgument(Expression arg)
    {
        // Handle g.Key.SomeField.Id -> we want "SomeField" (the parent of .Id)
        if (arg is MemberExpression member && member.Member.Name == "Id")
        {
            // Go up one level to get the parent (e.g., SizeItem)
            if (member.Expression is MemberExpression parent)
            {
                return parent.Member.Name;
            }
        }
        return null;
    }
}

/// <summary>
/// Internal implementation of IRedbGrouping for materialization
/// </summary>
internal class RedbGroupingImpl<TKey, TProps> : IRedbGrouping<TKey, TProps> 
    where TProps : class, new()
{
    private readonly JsonElement _element;
    
    public RedbGroupingImpl(JsonElement element)
    {
        _element = element;
    }
    
    public TKey Key => default!; // Filled during materialization
}
