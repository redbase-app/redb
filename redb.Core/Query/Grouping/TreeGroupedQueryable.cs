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
    private readonly List<LambdaExpression> _havingPredicates = new();
    private string? _havingJson;

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
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = ParseAggregations(selector);

        // Use tree-aware execution with full context
        var jsonResult = await _treeProvider.ExecuteTreeGroupedAggregateAsync(
            _treeContext, groupFields, aggregations, BuildHavingJson(), cancellationToken: cancellationToken);

        return MaterializeResults<TResult>(jsonResult, selector, groupFields);
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = new[] { new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } };

        var jsonResult = await _treeProvider.ExecuteTreeGroupedAggregateAsync(
            _treeContext, groupFields, aggregations, BuildHavingJson(), cancellationToken: cancellationToken);

        if (jsonResult == null) return 0;
        return jsonResult.RootElement.GetArrayLength();
    }

    public async Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = ParseAggregations(selector);

        // Delegate to tree provider for real SQL preview
        return await _treeProvider.GetTreeGroupBySqlPreviewAsync(
            _treeContext, groupFields, aggregations, BuildHavingJson(), cancellationToken: cancellationToken);
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

    /// <inheritdoc />
    public IRedbGroupedQueryable<TKey, TProps> Having(
        Expression<Func<IRedbGrouping<TKey, TProps>, bool>> predicate)
    {
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        _havingPredicates.Add(predicate);
        _havingJson = null;
        return this;
    }

    private string? BuildHavingJson()
    {
        if (_havingJson != null) return _havingJson;
        if (_havingPredicates.Count == 0) return null;

        if (_havingPredicates.Count == 1)
        {
            var single = (Expression<Func<IRedbGrouping<TKey, TProps>, bool>>)_havingPredicates[0];
            _havingJson = HavingPredicateParser.ToJson(single);
            return _havingJson;
        }

        var param = Expression.Parameter(typeof(IRedbGrouping<TKey, TProps>), "g");
        Expression body = ReplaceParam((Expression<Func<IRedbGrouping<TKey, TProps>, bool>>)_havingPredicates[0], param);
        for (int i = 1; i < _havingPredicates.Count; i++)
        {
            var nextBody = ReplaceParam((Expression<Func<IRedbGrouping<TKey, TProps>, bool>>)_havingPredicates[i], param);
            body = Expression.AndAlso(body, nextBody);
        }
        var combined = Expression.Lambda<Func<IRedbGrouping<TKey, TProps>, bool>>(body, param);
        _havingJson = HavingPredicateParser.ToJson(combined);
        return _havingJson;
    }

    private static Expression ReplaceParam(
        Expression<Func<IRedbGrouping<TKey, TProps>, bool>> lambda,
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
        var groupParam = selector.Parameters[0];

        foreach (var (name, _, rawExpr) in GroupSelectorMembers.Extract(selector, "TreeGroupBy.SelectAsync"))
        {
            var expr = GroupSelectorMembers.StripConvert(rawExpr);

            if (GroupSelectorMembers.IsKeyAccess(expr, groupParam))
                continue;

            if (expr is MethodCallExpression methodCall && methodCall.Method.DeclaringType == typeof(Agg))
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
                    fieldPath = ExtractFieldPathFromLambda(methodCall.Arguments[1]);

                result.Add(new AggregateRequest { FieldPath = fieldPath, Function = function, Alias = name });
                continue;
            }

            if (!GroupSelectorMembers.ReferencesParameter(expr, groupParam))
                continue; // клиентское значение - вычислится при материализации

            throw new NotSupportedException(
                $"TreeGroupBy.SelectAsync: член '{name}' использует группу, но не является ни g.Key, ни прямым Agg.* вызовом.");
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
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector,
        List<GroupFieldRequest> groupFields)
    {
        var results = new List<TResult>();
        if (jsonResult == null) return results;

        var members = GroupSelectorMembers.Extract(selector, "TreeGroupBy.SelectAsync");
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

        foreach (var element in jsonResult.RootElement.EnumerateArray())
        {
            var values = new object?[members.Count];
            for (int i = 0; i < members.Count; i++)
            {
                if (isClient[i]) { values[i] = clientValues[i]; continue; }

                var (name, type, rawExpr) = members[i];
                if (element.TryGetProperty(name, out var prop))
                    values[i] = JsonValueConverter.Convert(prop, type);
                else
                {
                    var jsonAlias = ExtractJsonAliasFromArgument(GroupSelectorMembers.StripConvert(rawExpr), groupFields);
                    if (!string.IsNullOrEmpty(jsonAlias) && element.TryGetProperty(jsonAlias, out prop))
                        values[i] = JsonValueConverter.Convert(prop, type);
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

    #endregion
}
