using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Window;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Where the groups of a grouped window come from: a GroupBy over objects or a GroupByArray over elements.
/// </summary>
internal interface IGroupedWindowSource
{
    /// <summary>Name of the provider for messages.</summary>
    string ProviderName { get; }

    /// <summary>What the lambdas inside Agg.* read.</summary>
    GroupItemKind Kind { get; }

    /// <summary>Keys of the grouping, aliased as the selector names g.Key.</summary>
    List<GroupFieldRequest> GroupFields(LambdaExpression selector);

    Task<JsonDocument> ExecuteAsync(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window, CancellationToken cancellationToken);

    Task<string> GetSqlAsync(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window, CancellationToken cancellationToken);

    /// <summary>The list provider the list item keys are read through (ListItemGroupKeys).</summary>
    redb.Core.Providers.IListProvider? ListItems { get; }
}

/// <summary>
/// Queryable for applying window functions to grouped data.
/// Wraps grouped query and adds window function support.
/// </summary>
public class GroupedWindowedQueryable<TKey, TProps> : IGroupedWindowedQueryable<TKey, TProps>
{
    private const string OwnerApi = "WithWindow.SelectAsync";

    private readonly IGroupedWindowSource _source;
    private readonly GroupedWindowSpec<TKey, TProps> _windowSpec;

    /// <summary>True when the key comes from GroupByRedb: its members are fields of the object, not of Props.</summary>
    public bool IsBaseFieldGrouping { get; init; }

    /// <summary>
    /// Constructor with filterJson (legacy compatibility).
    /// </summary>
    public GroupedWindowedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        Expression keySelector,
        GroupedWindowSpec<TKey, TProps> windowSpec,
        string? filterJson = null)
        : this(provider, schemeId, keySelector, windowSpec, filterJson, null, null)
    {
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
        : this(provider, schemeId, keySelector, windowSpec, null, filter, null)
    {
    }

    /// <summary>
    /// Constructor for a GroupBy with HAVING: the window runs over the groups that pass it.
    /// </summary>
    public GroupedWindowedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        Expression keySelector,
        GroupedWindowSpec<TKey, TProps> windowSpec,
        string? filterJson,
        FilterExpression? filter,
        string? havingJson)
    {
        _windowSpec = windowSpec;
        _source = new FlatGroupWindowSource(this, provider, schemeId, keySelector, filterJson, filter, havingJson);
    }

    internal GroupedWindowedQueryable(IGroupedWindowSource source, GroupedWindowSpec<TKey, TProps> windowSpec)
    {
        _source = source;
        _windowSpec = windowSpec;
    }

    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var (aggregations, window) = Parse(selector);
        var json = await _source.ExecuteAsync(selector, aggregations, window, cancellationToken);
        var rows = ArrayGroupMaterializer.Materialize<TResult>(json, selector, _source.GroupFields(selector), _source.ProviderName, OwnerApi);
        await ListItemGroupKeys.FillAsync(rows, _source.ListItems, OwnerApi, cancellationToken);
        return rows;
    }

    public Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var (aggregations, window) = Parse(selector);
        return _source.GetSqlAsync(selector, aggregations, window, cancellationToken);
    }

    private (List<AggregateRequest> Aggregations, ArrayGroupWindow Window) Parse(LambdaExpression selector)
    {
        var aggregations = new List<AggregateRequest>();
        var functions = new List<WindowFuncRequest>();
        var param = selector.Parameters[0];

        foreach (var (name, _, rawExpr) in GroupSelectorMembers.Extract(selector, OwnerApi))
        {
            var expr = GroupSelectorMembers.StripConvert(rawExpr);

            if (GroupSelectorMembers.IsKeyAccess(expr, param))
                continue;

            if (expr is MethodCallExpression win && win.Method.DeclaringType == typeof(Win))
            {
                functions.Add(WindowFunction(win, param, name));
                continue;
            }

            if (GroupAggregateParser.IsAggCall(expr))
            {
                aggregations.Add(GroupAggregateParser.Parse((MethodCallExpression)expr, param, name, _source.Kind, OwnerApi));
                continue;
            }

            if (!GroupSelectorMembers.ReferencesParameter(expr, param))
                continue; // a client value, computed once at materialization

            throw new NotSupportedException(
                $"{OwnerApi}: member '{name}' uses the group but is neither g.Key, nor Agg.*, nor Win.* ('{rawExpr}').");
        }

        if (functions.Count == 0)
            throw new NotSupportedException($"{OwnerApi}: the selector of a window over groups needs at least one Win.* function.");

        return (aggregations, new ArrayGroupWindow
        {
            Functions = functions,
            PartitionBy = ParsePartitionBy(),
            OrderBy = ParseOrderBy()
        });
    }

    /// <summary>
    /// A Win.* call of the selector. Ntile takes its bucket count; Sum, Avg, Min, Max, Count, Lag, Lead,
    /// FirstValue and LastValue take a key or an Agg.* over the group; the ranking functions take nothing.
    /// </summary>
    private WindowFuncRequest WindowFunction(MethodCallExpression win, ParameterExpression groupParam, string alias)
    {
        var request = new WindowFuncRequest { Func = win.Method.Name, Alias = alias };
        if (win.Arguments.Count == 0)
            return request;

        if (win.Method.Name == "Ntile")
        {
            if (GroupSelectorMembers.ReferencesParameter(win.Arguments[0], groupParam))
                throw new NotSupportedException($"{OwnerApi}: Win.Ntile takes a constant number of buckets; got '{win.Arguments[0]}'.");
            request.Buckets = Expression.Lambda<Func<int>>(win.Arguments[0]).Compile()();
            return request;
        }

        request.FieldPath = OverReference(win.Arguments[0], groupParam, $"the argument of Win.{win.Method.Name}");
        return request;
    }

    private List<WindowFieldRequest> ParsePartitionBy()
    {
        var result = new List<WindowFieldRequest>();
        foreach (var expr in _windowSpec.PartitionByExpressions)
        {
            var body = GroupSelectorMembers.StripConvert(expr.Body);
            // PartitionBy(k => k.Department): a member of a composite key, by its name.
            if (body is MemberExpression memberExpr && memberExpr.Expression == expr.Parameters[0])
            {
                result.Add(new WindowFieldRequest { FieldPath = memberExpr.Member.Name, Alias = memberExpr.Member.Name });
                continue;
            }
            // PartitionBy(k => k): the single key.
            if (body == expr.Parameters[0])
            {
                result.Add(new WindowFieldRequest { FieldPath = "Key", Alias = "Key" });
                continue;
            }
            throw new NotSupportedException($"WithWindow: PartitionBy takes the key or a member of it (k => k.Field); got '{expr}'.");
        }
        return result;
    }

    private List<WindowOrderRequest> ParseOrderBy()
    {
        var result = new List<WindowOrderRequest>();
        foreach (var (expr, desc) in _windowSpec.OrderByExpressions)
            result.Add(new WindowOrderRequest
            {
                FieldPath = OverReference(expr.Body, expr.Parameters[0], "OrderBy of the window"),
                Descending = desc
            });
        return result;
    }

    /// <summary>
    /// A reference inside OVER or a window function: g.Key (single key), g.Key.Member (composite key) or
    /// Agg.*(g, ...) in the "Agg_Func_Field" form providers resolve with <see cref="GroupedWindowNames"/>.
    /// </summary>
    private string OverReference(Expression body, ParameterExpression groupParam, string what)
    {
        var expr = GroupSelectorMembers.StripConvert(body);
        if (GroupAggregateParser.IsAggCall(expr))
        {
            var request = GroupAggregateParser.Parse((MethodCallExpression)expr, groupParam, "over", _source.Kind, OwnerApi);
            if (request.ConditionJson != null)
                throw new NotSupportedException(
                    $"WithWindow: {what} refers to a conditional count; select it in SelectAsync and sort the result instead.");
            return request.Function == AggregateFunction.Count
                ? "Agg_Count_Count"
                : $"Agg_{request.Function}_{request.FieldPath}";
        }
        if (expr is MemberExpression m && GroupSelectorMembers.IsKeyAccess(m, groupParam))
            return m.Member.Name == "Key" ? "Key" : m.Member.Name;
        throw new NotSupportedException($"WithWindow: {what} must be g.Key, a member of g.Key or an Agg.* call; got '{body}'.");
    }

    /// <summary>
    /// A GroupBy over objects as the source of the window.
    /// </summary>
    private sealed class FlatGroupWindowSource : IGroupedWindowSource
    {
        private readonly GroupedWindowedQueryable<TKey, TProps> _owner;
        private readonly IRedbQueryProvider _provider;
        private readonly long _schemeId;
        private readonly Expression _keySelector;
        private readonly string? _filterJson;
        private readonly FilterExpression? _filter;
        private readonly string? _havingJson;

        public FlatGroupWindowSource(GroupedWindowedQueryable<TKey, TProps> owner, IRedbQueryProvider provider, long schemeId,
            Expression keySelector, string? filterJson, FilterExpression? filter, string? havingJson)
        {
            _owner = owner;
            _provider = provider;
            _schemeId = schemeId;
            _keySelector = keySelector;
            _filterJson = filterJson;
            _filter = filter;
            _havingJson = havingJson;
        }

        public string ProviderName => _provider.GetType().Name;
        public redb.Core.Providers.IListProvider? ListItems => (_provider as IListItemKeySource)?.ListItems;

        public GroupItemKind Kind => GroupItemKind.Props;

        public List<GroupFieldRequest> GroupFields(LambdaExpression selector)
            => GroupKeyFields.Parse(_keySelector, _owner.IsBaseFieldGrouping, "WithWindow");

        public async Task<JsonDocument> ExecuteAsync(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window, CancellationToken cancellationToken)
        {
            var groupFields = GroupFields(selector);
            var json = _filter != null
                ? await _provider.ExecuteGroupedWindowQueryAsync(_schemeId, groupFields, aggregations, window.Functions, window.PartitionBy, window.OrderBy, _filter, _havingJson, cancellationToken)
                : await _provider.ExecuteGroupedWindowQueryAsync(_schemeId, groupFields, aggregations, window.Functions, window.PartitionBy, window.OrderBy, _filterJson, _havingJson, cancellationToken);
            return json ?? throw new InvalidOperationException($"{OwnerApi}: {ProviderName} returned no answer for the grouped window.");
        }

        public Task<string> GetSqlAsync(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window, CancellationToken cancellationToken)
        {
            var groupFields = GroupFields(selector);
            return _filter != null
                ? _provider.GetGroupedWindowSqlPreviewAsync(_schemeId, groupFields, aggregations, window.Functions, window.PartitionBy, window.OrderBy, _filter, _havingJson, cancellationToken)
                : _provider.GetGroupedWindowSqlPreviewAsync(_schemeId, groupFields, aggregations, window.Functions, window.PartitionBy, window.OrderBy, _filterJson, _havingJson, cancellationToken);
        }
    }
}
