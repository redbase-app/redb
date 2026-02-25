using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Base;
using redb.Core.Query.Utils;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Tree-aware grouped queryable that preserves TreeQueryContext for proper CTE generation.
/// Uses ITreeQueryProvider.ExecuteTreeGroupedAggregateAsync for tree traversal.
/// </summary>
public class TreeGroupedQueryable<TKey, TProps> : IRedbGroupedQueryable<TKey, TProps>
    where TProps : class, new()
{
    private readonly ITreeQueryProvider _treeProvider;
    private readonly TreeQueryContext<TProps> _treeContext;
    private readonly Expression _keySelector;
    private readonly string? _baseFilterJson;
    private readonly bool _isBaseFieldGrouping;

    public TreeGroupedQueryable(
        ITreeQueryProvider treeProvider,
        TreeQueryContext<TProps> treeContext,
        Expression keySelector,
        string? baseFilterJson = null,
        bool isBaseFieldGrouping = false)
    {
        _treeProvider = treeProvider;
        _treeContext = treeContext.Clone(); // Clone to preserve state
        _keySelector = keySelector;
        _baseFilterJson = baseFilterJson;
        _isBaseFieldGrouping = isBaseFieldGrouping;
    }

    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = ParseAggregations(selector);

        // Use tree-aware execution with full context
        var jsonResult = await _treeProvider.ExecuteTreeGroupedAggregateAsync(
            _treeContext, groupFields, aggregations);

        return MaterializeResults<TResult>(jsonResult, selector);
    }

    public async Task<int> CountAsync()
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = new[] { new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } };

        var jsonResult = await _treeProvider.ExecuteTreeGroupedAggregateAsync(
            _treeContext, groupFields, aggregations);

        if (jsonResult == null) return 0;
        return jsonResult.RootElement.GetArrayLength();
    }

    public async Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = ParseAggregations(selector);

        // Delegate to tree provider for real SQL preview
        return await _treeProvider.GetTreeGroupBySqlPreviewAsync(
            _treeContext, groupFields, aggregations);
    }
    
    /// <summary>
    /// Apply window functions to grouped results (tree-aware).
    /// </summary>
    public IGroupedWindowedQueryable<TKey, TProps> WithWindow(
        Action<IGroupedWindowSpec<TKey, TProps>> windowConfig)
    {
        var windowSpec = new GroupedWindowSpec<TKey, TProps>();
        windowConfig(windowSpec);
        return new TreeGroupedWindowedQueryable<TKey, TProps>(
            _treeProvider, _treeContext, _keySelector, windowSpec);
    }

    #region Expression Parsing (same as RedbGroupedQueryable)

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

                if (arg is MemberExpression member && member.Member.Name == "Key")
                    continue;

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

            var path = ExtractFieldPath(body as MemberExpression);
            return string.IsNullOrEmpty(path) ? "*" : path;
        }

        return "*";
    }

    #endregion

    #region Materialization

    private List<TResult> MaterializeResults<TResult>(
        JsonDocument? jsonResult,
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var results = new List<TResult>();
        if (jsonResult == null) return results;

        foreach (var element in jsonResult.RootElement.EnumerateArray())
        {
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

    #endregion
}
