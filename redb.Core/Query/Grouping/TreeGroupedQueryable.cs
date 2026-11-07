using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Base;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Tree-aware grouped queryable that preserves TreeQueryContext for proper CTE generation.
/// Uses ITreeQueryProvider.ExecuteTreeGroupedAggregateAsync for tree traversal.
/// </summary>
public class TreeGroupedQueryable<TKey, TProps> : IRedbGroupedQueryable<TKey, TProps>
    where TProps : class, new()
{
    private const string OwnerApi = "Tree GroupBy";

    private readonly ITreeQueryProvider _treeProvider;
    private readonly TreeQueryContext<TProps> _treeContext;
    private readonly Expression _keySelector;
    private readonly string? _baseFilterJson;
    private readonly bool _isBaseFieldGrouping;
    private readonly List<LambdaExpression> _havingPredicates = new();

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
        var groupFields = GroupKeyFields.Parse(_keySelector, _isBaseFieldGrouping, OwnerApi);
        var aggregations = ParseAggregations(selector);
        var json = await _treeProvider.ExecuteTreeGroupedAggregateAsync(
            _treeContext, groupFields, aggregations, BuildHavingJson(), cancellationToken: cancellationToken);
        var rows = ArrayGroupMaterializer.Materialize<TResult>(json, selector, groupFields, _treeProvider.GetType().Name, OwnerApi + ".SelectAsync");
        await ListItemGroupKeys.FillAsync(rows, (_treeProvider as IListItemKeySource)?.ListItems, OwnerApi + ".SelectAsync", cancellationToken);
        return rows;
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        var groupFields = GroupKeyFields.Parse(_keySelector, _isBaseFieldGrouping, OwnerApi);
        var aggregations = new[] { new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } };
        using var json = await _treeProvider.ExecuteTreeGroupedAggregateAsync(
            _treeContext, groupFields, aggregations, BuildHavingJson(), cancellationToken: cancellationToken);
        return ArrayGroupMaterializer.RequireArray(json, _treeProvider.GetType().Name, OwnerApi + ".CountAsync").GetArrayLength();
    }

    public Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TProps>, TResult>> selector, CancellationToken cancellationToken = default)
        => _treeProvider.GetTreeGroupBySqlPreviewAsync(
            _treeContext, GroupKeyFields.Parse(_keySelector, _isBaseFieldGrouping, OwnerApi), ParseAggregations(selector),
            BuildHavingJson(), cancellationToken: cancellationToken);

    /// <summary>
    /// Apply window functions to grouped results (tree-aware): the window runs over the groups that pass Having.
    /// </summary>
    public IGroupedWindowedQueryable<TKey, TProps> WithWindow(
        Action<IGroupedWindowSpec<TKey, TProps>> windowConfig)
    {
        var windowSpec = new GroupedWindowSpec<TKey, TProps>();
        windowConfig(windowSpec);
        return new GroupedWindowedQueryable<TKey, TProps>(new TreeGroupWindowSource(this, BuildHavingJson()), windowSpec);
    }

    /// <inheritdoc />
    public IRedbGroupedQueryable<TKey, TProps> Having(
        Expression<Func<IRedbGrouping<TKey, TProps>, bool>> predicate)
    {
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        // TGR-1 (review 2026-09-24): a copy, as the flat grouping does since G-4. Changing this builder let a
        // second query branched from the same grouping inherit the first one's condition.
        var copy = new TreeGroupedQueryable<TKey, TProps>(
            _treeProvider, _treeContext, _keySelector, _baseFilterJson, _isBaseFieldGrouping);
        copy._havingPredicates.AddRange(_havingPredicates);
        copy._havingPredicates.Add(predicate);
        return copy;
    }

    private string? BuildHavingJson()
    {
        if (_havingPredicates.Count == 0) return null;
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
                $"{OwnerApi}.SelectAsync: member '{name}' uses the group but is neither g.Key nor a direct Agg.* call ('{rawExpr}').");
        }

        return result;
    }

    /// <summary>
    /// The tree grouping as the source of a window over its groups.
    /// </summary>
    private sealed class TreeGroupWindowSource : IGroupedWindowSource
    {
        private readonly TreeGroupedQueryable<TKey, TProps> _owner;
        private readonly string? _havingJson;

        public TreeGroupWindowSource(TreeGroupedQueryable<TKey, TProps> owner, string? havingJson)
        {
            _owner = owner;
            _havingJson = havingJson;
        }

        public string ProviderName => _owner._treeProvider.GetType().Name;
        public redb.Core.Providers.IListProvider? ListItems => (_owner._treeProvider as IListItemKeySource)?.ListItems;

        public GroupItemKind Kind => GroupItemKind.Props;

        public List<GroupFieldRequest> GroupFields(LambdaExpression selector)
            => GroupKeyFields.Parse(_owner._keySelector, _owner._isBaseFieldGrouping, "Tree WithWindow");

        public async Task<JsonDocument> ExecuteAsync(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window, CancellationToken cancellationToken)
            => await _owner._treeProvider.ExecuteTreeGroupedWindowQueryAsync(
                   _owner._treeContext, GroupFields(selector), aggregations, window.Functions, window.PartitionBy, window.OrderBy,
                   _havingJson, cancellationToken)
               ?? throw new InvalidOperationException($"Tree WithWindow: {ProviderName} returned no answer for the grouped window.");

        public Task<string> GetSqlAsync(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window, CancellationToken cancellationToken)
            => _owner._treeProvider.GetTreeGroupedWindowSqlPreviewAsync(
                _owner._treeContext, GroupFields(selector), aggregations, window.Functions, window.PartitionBy, window.OrderBy,
                _havingJson, cancellationToken);
    }
}
