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
    private string? _havingJson;
    private readonly List<LambdaExpression> _havingPredicates = new();
    
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
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        // 1. Parse grouping fields from _keySelector
        var groupFields = ParseGroupFields(_keySelector);
        
        // 2. Parse aggregations from selector
        var aggregations = ParseAggregations(selector);
        
        // 3. Execute SQL query (Pro uses FilterExpression directly, Free uses filterJson)
        var havingJson = BuildHavingJson();
        var jsonResult = _filter != null
            ? await _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filter, havingJson, cancellationToken: cancellationToken)
            : await _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filterJson, havingJson, cancellationToken: cancellationToken);
        
        // 4. Materialize result
        return MaterializeResults<TResult>(jsonResult, selector, groupFields);
    }
    
    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        var groupFields = ParseGroupFields(_keySelector);
        var aggregations = new[] { new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } };
        
        // Pro uses FilterExpression directly, Free uses filterJson
        var havingJson = BuildHavingJson();
        var jsonResult = _filter != null
            ? await _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filter, havingJson, cancellationToken: cancellationToken)
            : await _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filterJson, havingJson, cancellationToken: cancellationToken);
        
        if (jsonResult == null) return 0;
        return jsonResult.RootElement.GetArrayLength();
    }
    
    /// <summary>
    /// Returns SQL string that will be executed for this GroupBy query.
    /// Requires Pro version provider that supports SQL preview.
    /// </summary>
    public async Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
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
            // Pro path: use FilterExpression overload (havingJson included)
            getSqlMethod = providerType.GetMethod("GetGroupBySqlPreviewAsync", 
                new[] { typeof(long), typeof(IEnumerable<GroupFieldRequest>), typeof(IEnumerable<AggregateRequest>), typeof(FilterExpression), typeof(string), typeof(CancellationToken) });
            methodArgs = new object?[] { _schemeId, groupFields, aggregations, _filter, BuildHavingJson(), cancellationToken };
            if (getSqlMethod == null)
            {
                // Fall back to legacy 4-arg signature
                getSqlMethod = providerType.GetMethod("GetGroupBySqlPreviewAsync", 
                    new[] { typeof(long), typeof(IEnumerable<GroupFieldRequest>), typeof(IEnumerable<AggregateRequest>), typeof(FilterExpression), typeof(CancellationToken) });
                methodArgs = new object?[] { _schemeId, groupFields, aggregations, _filter, cancellationToken };
            }
        }
        else
        {
            // Legacy path: use string overload (havingJson included)
            getSqlMethod = providerType.GetMethod("GetGroupBySqlPreviewAsync", 
                new[] { typeof(long), typeof(IEnumerable<GroupFieldRequest>), typeof(IEnumerable<AggregateRequest>), typeof(string), typeof(string), typeof(CancellationToken) });
            methodArgs = new object?[] { _schemeId, groupFields, aggregations, _filterJson, BuildHavingJson(), cancellationToken };
            if (getSqlMethod == null)
            {
                getSqlMethod = providerType.GetMethod("GetGroupBySqlPreviewAsync", 
                    new[] { typeof(long), typeof(IEnumerable<GroupFieldRequest>), typeof(IEnumerable<AggregateRequest>), typeof(string), typeof(CancellationToken) });
                methodArgs = new object?[] { _schemeId, groupFields, aggregations, _filterJson, cancellationToken };
            }
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

    /// <inheritdoc />
    public IRedbGroupedQueryable<TKey, TProps> Having(
        Expression<Func<IRedbGrouping<TKey, TProps>, bool>> predicate)
    {
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        // G-4 (ревью 2026-09-03): строитель копирующий, как весь остальной LINQ-API. Раньше
        // Having мутировал this - ветвление (var a = g.Having(x); var b = g.Having(y);)
        // заражало сестринскую ветку обоими предикатами.
        var copy = _filter != null
            ? new RedbGroupedQueryable<TKey, TProps>(_provider, _schemeId, _filter, _keySelector, _isBaseFieldGrouping)
            : new RedbGroupedQueryable<TKey, TProps>(_provider, _schemeId, _filterJson, _keySelector, _isBaseFieldGrouping);
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
            var single = (Expression<Func<IRedbGrouping<TKey, TProps>, bool>>)_havingPredicates[0];
            _havingJson = HavingPredicateParser.ToJson(single);
            return _havingJson;
        }

        // Compose multiple Having(...) calls with AndAlso into one predicate
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
        while (body is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
            body = u.Operand;

        switch (body)
        {
            // Simple field: x => x.Category
            case MemberExpression member:
            {
                var path = ExtractFieldPath(member);
                if (string.IsNullOrEmpty(path))
                    throw new NotSupportedException(
                        $"GroupBy: ключ '{member}' не разобран как поле. Поддерживаются поле или анонимный тип из полей.");
                result.Add(new GroupFieldRequest
                {
                    FieldPath = path,
                    Alias = member.Member.Name,
                    IsBaseField = _isBaseFieldGrouping
                });
                break;
            }

            // Anonymous type: x => new { x.Category, x.Year }
            case NewExpression newExpr:
            {
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    while (arg is UnaryExpression au && (au.NodeType == ExpressionType.Convert || au.NodeType == ExpressionType.ConvertChecked))
                        arg = au.Operand;
                    var alias = newExpr.Members?[i].Name ?? $"Key{i}";

                    if (arg is not MemberExpression memberArg)
                        throw new NotSupportedException(
                            $"GroupBy: член ключа '{alias}' не является полем ('{arg}'). Вычисляемые ключи не поддерживаются.");

                    var fieldPath = ExtractFieldPath(memberArg);
                    if (string.IsNullOrEmpty(fieldPath))
                        throw new NotSupportedException(
                            $"GroupBy: член ключа '{alias}' не разобран как поле Props/базовое поле.");
                    result.Add(new GroupFieldRequest
                    {
                        FieldPath = fieldPath,
                        Alias = alias,
                        IsBaseField = _isBaseFieldGrouping
                    });
                }
                break;
            }

            default:
                // G-3 (ревью 2026-09-03): вычисляемый ключ раньше молча давал пустой GROUP BY.
                throw new NotSupportedException(
                    "GroupBy поддерживает поле (x => x.Category) или анонимный тип из полей " +
                    $"(x => new {{ x.A, x.B }}). Вычисляемый ключ не поддерживается: '{body}'.");
        }
    }

    /// <summary>
    /// Члены селектора SelectAsync: имя, тип и выражение - единый разбор для анонимного типа
    /// (NewExpression) и DTO с инициализатором членов (MemberInitExpression; G-1, решение
    /// владельца 2026-09-04). Всё прочее - громкий отказ (fail-closed, ревью 2026-09-03).
    /// </summary>
    private static List<(string Name, Type Type, Expression Expr)> ExtractSelectorMembers<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var members = new List<(string, Type, Expression)>();
        switch (selector.Body)
        {
            case NewExpression newExpr:
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var name = newExpr.Members?[i].Name ?? $"Item{i}";
                    var type = (newExpr.Members?[i] as System.Reflection.PropertyInfo)?.PropertyType
                               ?? newExpr.Arguments[i].Type;
                    members.Add((name, type, newExpr.Arguments[i]));
                }
                break;

            case MemberInitExpression initExpr:
                if (initExpr.NewExpression.Arguments.Count > 0)
                    throw new NotSupportedException(
                        "GroupBy.SelectAsync: DTO с параметрами конструктора не поддерживается - " +
                        "нужен parameterless конструктор и инициализатор членов.");
                foreach (var binding in initExpr.Bindings)
                {
                    if (binding is not MemberAssignment assignment)
                        throw new NotSupportedException(
                            $"GroupBy.SelectAsync: биндинг '{binding.Member.Name}' ({binding.BindingType}) не поддерживается - " +
                            "только простые присваивания членов DTO.");
                    var type = (assignment.Member as System.Reflection.PropertyInfo)?.PropertyType
                               ?? assignment.Expression.Type;
                    members.Add((assignment.Member.Name, type, assignment.Expression));
                }
                break;

            default:
                throw new NotSupportedException(
                    "GroupBy.SelectAsync поддерживает анонимный тип (g => new { ... }) или DTO с инициализатором " +
                    $"членов (g => new Dto {{ ... }}); получено: {selector.Body.NodeType}.");
        }
        return members;
    }

    private static Expression StripConvert(Expression expr)
    {
        while (expr is UnaryExpression u && (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
            expr = u.Operand;
        return expr;
    }

    /// <summary>Цепочка членов от параметра группы через .Key (g.Key / g.Key.X / g.Key.X.Id).</summary>
    private static bool IsKeyAccess(Expression expr, ParameterExpression groupParam)
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

    private static bool ReferencesParameter(Expression expr, ParameterExpression parameter)
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

    /// <summary>
    /// Parses aggregations from SelectAsync expression. Fail-closed (G-2, ревью 2026-09-03):
    /// член, использующий группу, но не являющийся g.Key или прямым Agg.* вызовом, раньше
    /// молча оставался null - теперь громкий отказ. Члены без ссылки на группу - клиентские
    /// значения, вычисляются при материализации.
    /// </summary>
    private List<AggregateRequest> ParseAggregations<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector)
    {
        var result = new List<AggregateRequest>();
        var groupParam = selector.Parameters[0];

        foreach (var (name, _, rawExpr) in ExtractSelectorMembers(selector))
        {
            var expr = StripConvert(rawExpr);

            if (IsKeyAccess(expr, groupParam))
                continue; // материализуется по алиасу группировки

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

            if (!ReferencesParameter(expr, groupParam))
                continue; // клиентское значение

            throw new NotSupportedException(
                $"GroupBy.SelectAsync: член '{name}' использует группу, но не является ни g.Key, ни прямым вызовом Agg.* " +
                "- вычисления вокруг агрегатов не транслируются в SQL; посчитайте их по результату запроса.");
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
    /// Materializes JSON result to List&lt;TResult&gt;. Анонимный тип и DTO/MemberInit (G-1);
    /// члены, не ссылающиеся на группу, вычисляются на клиенте один раз на запрос.
    /// </summary>
    private List<TResult> MaterializeResults<TResult>(
        JsonDocument? jsonResult,
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector,
        List<GroupFieldRequest> groupFields)
    {
        var results = new List<TResult>();
        if (jsonResult == null) return results;

        var members = ExtractSelectorMembers(selector);
        var groupParam = selector.Parameters[0];

        var clientValues = new object?[members.Count];
        var isClient = new bool[members.Count];
        for (int i = 0; i < members.Count; i++)
        {
            if (!ReferencesParameter(StripConvert(members[i].Expr), groupParam))
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
                    var jsonAlias = ExtractJsonAliasFromArgument(StripConvert(rawExpr), groupFields);
                    if (!string.IsNullOrEmpty(jsonAlias) && element.TryGetProperty(jsonAlias, out prop))
                        values[i] = JsonValueConverter.Convert(prop, type);
                }
            }
            results.Add(ConstructResult<TResult>(selector, values));
        }

        return results;
    }

    private static TResult ConstructResult<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, object?[] values)
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
                        break; // остаётся default
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

    /// <summary>
    /// Extracts JSON field alias from selector argument.
    /// Handles patterns:
    ///   g.Key (single key) → returns the single GroupBy alias
    ///   g.Key.Department (composite key) → returns alias for "Department"
    ///   g.Key.SomeField.Id → returns "SomeField" (parent of .Id)
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
