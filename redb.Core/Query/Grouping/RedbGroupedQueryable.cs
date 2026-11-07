using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Implementation of REDB grouped queries
/// </summary>
public class RedbGroupedQueryable<TKey, TProps> : IRedbGroupedQueryable<TKey, TProps>
    where TProps : class, new()
{
    private const string OwnerApi = "GroupBy";

    private readonly IRedbQueryProvider _provider;
    private readonly long _schemeId;
    private readonly string? _filterJson;
    private readonly FilterExpression? _filter;
    private readonly Expression _keySelector;
    private readonly bool _isBaseFieldGrouping;
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
        var groupFields = GroupKeyFields.Parse(_keySelector, _isBaseFieldGrouping, OwnerApi);
        var aggregations = ParseAggregations(selector);
        var json = await ExecuteAsync(groupFields, aggregations, cancellationToken);
        var rows = ArrayGroupMaterializer.Materialize<TResult>(json, selector, groupFields, _provider.GetType().Name, OwnerApi + ".SelectAsync");
        await ListItemGroupKeys.FillAsync(rows, (_provider as IListItemKeySource)?.ListItems, OwnerApi + ".SelectAsync", cancellationToken);
        return rows;
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        var groupFields = GroupKeyFields.Parse(_keySelector, _isBaseFieldGrouping, OwnerApi);
        var aggregations = new List<AggregateRequest> { new() { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } };
        using var json = await ExecuteAsync(groupFields, aggregations, cancellationToken);
        return ArrayGroupMaterializer.RequireArray(json, _provider.GetType().Name, OwnerApi + ".CountAsync").GetArrayLength();
    }

    /// <summary>
    /// Returns SQL string that will be executed for this GroupBy query.
    /// </summary>
    public Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var groupFields = GroupKeyFields.Parse(_keySelector, _isBaseFieldGrouping, OwnerApi);
        var aggregations = ParseAggregations(selector);
        var havingJson = BuildHavingJson();
        return _filter != null
            ? _provider.GetGroupBySqlPreviewAsync(_schemeId, groupFields, aggregations, _filter, havingJson, cancellationToken)
            : _provider.GetGroupBySqlPreviewAsync(_schemeId, groupFields, aggregations, _filterJson, havingJson, cancellationToken);
    }

    /// <summary>
    /// Apply window functions to grouped results: the window runs over the groups that pass Having.
    /// </summary>
    public IGroupedWindowedQueryable<TKey, TProps> WithWindow(
        Action<IGroupedWindowSpec<TKey, TProps>> windowConfig)
    {
        var windowSpec = new GroupedWindowSpec<TKey, TProps>();
        windowConfig(windowSpec);
        return new GroupedWindowedQueryable<TKey, TProps>(
            _provider, _schemeId, _keySelector, windowSpec, _filterJson, _filter, BuildHavingJson())
        {
            IsBaseFieldGrouping = _isBaseFieldGrouping
        };
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

    private Task<JsonDocument?> ExecuteAsync(List<GroupFieldRequest> groupFields, List<AggregateRequest> aggregations, CancellationToken cancellationToken)
    {
        var havingJson = BuildHavingJson();
        return _filter != null
            ? _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filter, havingJson, cancellationToken: cancellationToken)
            : _provider.ExecuteGroupedAggregateAsync(_schemeId, groupFields, aggregations, _filterJson, havingJson, cancellationToken: cancellationToken);
    }

    private string? BuildHavingJson()
    {
        if (_havingPredicates.Count == 0) return null;
        // Compose multiple Having(...) calls with AndAlso into one predicate
        var param = Expression.Parameter(typeof(IRedbGrouping<TKey, TProps>), "g");
        Expression? body = null;
        foreach (var p in _havingPredicates)
        {
            var next = new ParameterReplacer(p.Parameters[0], param).Visit(p.Body)!;
            body = body == null ? next : Expression.AndAlso(body, next);
        }
        return HavingPredicateParser.ToJson(Expression.Lambda<Func<IRedbGrouping<TKey, TProps>, bool>>(body!, param));
    }

    private sealed class ParameterReplacer : ExpressionVisitor
    {
        private readonly ParameterExpression _from;
        private readonly ParameterExpression _to;
        public ParameterReplacer(ParameterExpression from, ParameterExpression to) { _from = from; _to = to; }
        protected override Expression VisitParameter(ParameterExpression node) => node == _from ? _to : base.VisitParameter(node);
    }

    /// <summary>
    /// Aggregations of SelectAsync. Fail-closed (G-2): a member that uses the group but is neither g.Key nor a direct
    /// Agg.* call is refused; a member that does not use the group is a client value.
    /// </summary>
    private static List<AggregateRequest> ParseAggregations(LambdaExpression selector)
    {
        var result = new List<AggregateRequest>();
        var groupParam = selector.Parameters[0];

        foreach (var (name, _, rawExpr) in GroupSelectorMembers.Extract(selector, OwnerApi + ".SelectAsync"))
        {
            var expr = GroupSelectorMembers.StripConvert(rawExpr);

            if (GroupSelectorMembers.IsKeyAccess(expr, groupParam))
                continue;

            if (GroupAggregateParser.IsAggCall(expr))
            {
                result.Add(GroupAggregateParser.Parse((MethodCallExpression)expr, groupParam, name, GroupItemKind.Props, OwnerApi + ".SelectAsync"));
                continue;
            }

            if (!GroupSelectorMembers.ReferencesParameter(expr, groupParam))
                continue;

            throw new NotSupportedException(
                $"{OwnerApi}.SelectAsync: member '{name}' uses the group but is neither g.Key nor a direct Agg.* call; " +
                "computations around aggregates are not translated to SQL, compute them from the result.");
        }

        return result;
    }
}
