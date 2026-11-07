using System.Linq.Expressions;
using System.Text.Json;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Base;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Utils;
using redb.Core.Query.Window;

namespace redb.Core.Query.Grouping;

/// <summary>
/// Runs an <see cref="ArrayGroupQuery"/> on a provider: the flat one, or the tree one with its subtree.
/// </summary>
internal interface IArrayGroupRunner
{
    /// <summary>Name of the provider for messages.</summary>
    string ProviderName { get; }

    Task<JsonDocument> ExecuteAsync(ArrayGroupQuery query, CancellationToken cancellationToken);

    Task<string> GetSqlAsync(ArrayGroupQuery query, CancellationToken cancellationToken);

    /// <summary>The list provider the list item keys are read through (ListItemGroupKeys).</summary>
    redb.Core.Providers.IListProvider? ListItems { get; }
}

internal sealed class FlatArrayGroupRunner : IArrayGroupRunner
{
    private readonly IRedbQueryProvider _provider;
    public FlatArrayGroupRunner(IRedbQueryProvider provider) => _provider = provider;
    public string ProviderName => _provider.GetType().Name;
    public redb.Core.Providers.IListProvider? ListItems => (_provider as IListItemKeySource)?.ListItems;
    public Task<JsonDocument> ExecuteAsync(ArrayGroupQuery query, CancellationToken cancellationToken)
        => _provider.ExecuteArrayGroupQueryAsync(query, cancellationToken);
    public Task<string> GetSqlAsync(ArrayGroupQuery query, CancellationToken cancellationToken)
        => _provider.GetArrayGroupQuerySqlAsync(query, cancellationToken);
}

internal sealed class TreeArrayGroupRunner<TProps> : IArrayGroupRunner where TProps : class, new()
{
    private readonly ITreeQueryProvider _provider;
    private readonly TreeQueryContext<TProps> _context;
    public TreeArrayGroupRunner(ITreeQueryProvider provider, TreeQueryContext<TProps> context)
    {
        _provider = provider;
        _context = context;
    }
    public string ProviderName => _provider.GetType().Name;
    public redb.Core.Providers.IListProvider? ListItems => (_provider as IListItemKeySource)?.ListItems;
    public Task<JsonDocument> ExecuteAsync(ArrayGroupQuery query, CancellationToken cancellationToken)
        => _provider.ExecuteTreeArrayGroupQueryAsync(_context, query, cancellationToken);
    public Task<string> GetSqlAsync(ArrayGroupQuery query, CancellationToken cancellationToken)
        => _provider.GetTreeArrayGroupQuerySqlAsync(_context, query, cancellationToken);
}

/// <summary>
/// Grouping by the elements of an array: <c>GroupByArray(e =&gt; e.Contacts, c =&gt; c.Type)</c>, an inner array
/// (<c>e =&gt; e.Contacts.SelectMany(c =&gt; c.Metadata)</c>), an array of simple values (<c>e =&gt; e.Skills, s =&gt; s</c>).
/// Every form it cannot translate is refused with <see cref="NotSupportedException"/>.
/// </summary>
public class RedbArrayGroupedQueryable<TKey, TItem, TProps> : IRedbGroupedQueryable<TKey, TItem>
    where TProps : class, new()
{
    private const string OwnerApi = "GroupByArray";

    private readonly IArrayGroupRunner _runner;
    private readonly long _schemeId;
    private readonly FilterExpression? _filter;
    private readonly LambdaExpression _arraySelector;
    private readonly LambdaExpression _keySelector;
    private readonly List<LambdaExpression> _havingPredicates = new();

    private readonly GroupItemKind _kind;

    /// <summary>
    /// Flat grouping; <paramref name="filterJson"/> is not used, the condition comes from <paramref name="filter"/>.
    /// </summary>
    public RedbArrayGroupedQueryable(
        IRedbQueryProvider provider,
        long schemeId,
        string? filterJson,
        FilterExpression? filter,
        Expression<Func<TProps, IEnumerable<TItem>>> arraySelector,
        Expression<Func<TItem, TKey>> keySelector)
        : this(new FlatArrayGroupRunner(provider), schemeId, filter, arraySelector, keySelector)
    {
    }

    internal RedbArrayGroupedQueryable(
        IArrayGroupRunner runner,
        long schemeId,
        FilterExpression? filter,
        LambdaExpression arraySelector,
        LambdaExpression keySelector)
    {
        _runner = runner;
        _schemeId = schemeId;
        _filter = filter;
        _arraySelector = arraySelector;
        _keySelector = keySelector;
        _kind = ResolveItemKind();
        // Parse now: a form that cannot be translated is refused where it is written, not at execution.
        _ = ParseArrayPath(_arraySelector);
        _ = ParseGroupFields(alias: null);
    }

    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector, CancellationToken cancellationToken = default)
    {
        var query = BuildQuery(selector, window: null);
        var json = await _runner.ExecuteAsync(query, cancellationToken);
        var rows = ArrayGroupMaterializer.Materialize<TResult>(json, selector, query.GroupFields, _runner.ProviderName, OwnerApi);
        await ListItemGroupKeys.FillAsync(rows, _runner.ListItems, OwnerApi, cancellationToken);
        return rows;
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        var query = new ArrayGroupQuery
        {
            SchemeId = _schemeId,
            ArrayPath = ParseArrayPath(_arraySelector),
            GroupFields = ParseGroupFields(alias: null),
            Aggregations = new[] { new AggregateRequest { FieldPath = "*", Function = AggregateFunction.Count, Alias = "cnt" } },
            Filter = _filter,
            HavingJson = BuildHavingJson()
        };
        using var json = await _runner.ExecuteAsync(query, cancellationToken);
        return ArrayGroupMaterializer.RequireArray(json, _runner.ProviderName, OwnerApi).GetArrayLength();
    }

    public Task<string> ToSqlStringAsync<TResult>(
        Expression<Func<IRedbGrouping<TKey, TItem>, TResult>> selector, CancellationToken cancellationToken = default)
        => _runner.GetSqlAsync(BuildQuery(selector, window: null), cancellationToken);

    public IGroupedWindowedQueryable<TKey, TItem> WithWindow(Action<IGroupedWindowSpec<TKey, TItem>> windowConfig)
    {
        var windowSpec = new GroupedWindowSpec<TKey, TItem>();
        windowConfig(windowSpec);
        return new GroupedWindowedQueryable<TKey, TItem>(new ArrayWindowSource(this), windowSpec);
    }

    /// <inheritdoc />
    public IRedbGroupedQueryable<TKey, TItem> Having(Expression<Func<IRedbGrouping<TKey, TItem>, bool>> predicate)
    {
        if (predicate is null) throw new ArgumentNullException(nameof(predicate));
        // G-4: a copying builder - a branch does not leak into its sibling.
        var copy = new RedbArrayGroupedQueryable<TKey, TItem, TProps>(_runner, _schemeId, _filter, _arraySelector, _keySelector);
        copy._havingPredicates.AddRange(_havingPredicates);
        copy._havingPredicates.Add(predicate);
        return copy;
    }

    // ----------------------------------------------------------------------------------------------------------------

    private ArrayGroupQuery BuildQuery(LambdaExpression selector, ArrayGroupWindow? window)
        => new()
        {
            SchemeId = _schemeId,
            ArrayPath = ParseArrayPath(_arraySelector),
            GroupFields = ParseGroupFields(KeyAliasFromSelector(selector)),
            Aggregations = ParseAggregations(selector),
            Filter = _filter,
            HavingJson = BuildHavingJson(),
            Window = window
        };

    private string? BuildHavingJson()
    {
        if (_havingPredicates.Count == 0) return null;
        var param = Expression.Parameter(typeof(IRedbGrouping<TKey, TItem>), "g");
        Expression? body = null;
        foreach (var p in _havingPredicates)
        {
            var next = new ParameterReplacer(p.Parameters[0], param).Visit(p.Body)!;
            body = body == null ? next : Expression.AndAlso(body, next);
        }
        var combined = Expression.Lambda<Func<IRedbGrouping<TKey, TItem>, bool>>(body!, param);
        return HavingPredicateParser.ToJson(combined, _kind);
    }

    private sealed class ParameterReplacer : ExpressionVisitor
    {
        private readonly ParameterExpression _from;
        private readonly ParameterExpression _to;
        public ParameterReplacer(ParameterExpression from, ParameterExpression to) { _from = from; _to = to; }
        protected override Expression VisitParameter(ParameterExpression node) => node == _from ? _to : base.VisitParameter(node);
    }

    /// <summary>
    /// What an element of the array is. A class other than string is an element with fields; a list item and an
    /// object reference are refused by name; every other type is a simple value read from the element row.
    /// </summary>
    private static GroupItemKind ResolveItemKind()
    {
        var t = Nullable.GetUnderlyingType(typeof(TItem)) ?? typeof(TItem);
        if (t == typeof(string)) return GroupItemKind.ArrayValueElement;
        if (typeof(IRedbListItem).IsAssignableFrom(t))
            throw new NotSupportedException(
                $"{OwnerApi} over an array of {t.Name} is not supported: group the objects by the list item instead " +
                "(GroupBy(e => e.Field)), or keep the item id in a long field.");
        if (typeof(IRedbObject).IsAssignableFrom(t))
            throw new NotSupportedException(
                $"{OwnerApi} over an array of object references ({t.Name}) is not supported: group the referenced objects " +
                "by their own fields.");
        if (t == typeof(byte[]))
            throw new NotSupportedException($"{OwnerApi} over an array of byte[] is not supported.");
        if (t.IsClass || t.IsInterface) return GroupItemKind.ArrayClassElement;
        return GroupItemKind.ArrayValueElement;
    }

    /// <summary>
    /// The array path: a chain of fields (<c>e =&gt; e.Contacts</c>, <c>e =&gt; e.HomeAddress.Building.Amenities</c>) and
    /// <c>SelectMany</c> over an inner array (<c>e =&gt; e.Contacts.SelectMany(c =&gt; c.Metadata)</c>), joined with dots.
    /// </summary>
    private static string ParseArrayPath(LambdaExpression selector)
        => PathOf(selector.Body, selector.Parameters[0], selector);

    private static string PathOf(Expression expression, ParameterExpression parameter, LambdaExpression whole)
    {
        var e = GroupSelectorMembers.StripConvert(expression);

        if (e is MethodCallExpression mc && mc.Method.Name == "SelectMany" && mc.Method.DeclaringType == typeof(Enumerable))
        {
            if (mc.Arguments.Count != 2)
                throw new NotSupportedException(
                    $"{OwnerApi}: SelectMany with a result selector is not supported in the array path ('{mc}'). Group the " +
                    "inner array alone (e => e.Contacts.SelectMany(c => c.Metadata)); keys and aggregates read its elements.");
            var inner = mc.Arguments[1] is UnaryExpression { NodeType: ExpressionType.Quote } q ? q.Operand : mc.Arguments[1];
            if (inner is not LambdaExpression lambda || lambda.Parameters.Count != 1)
                throw new NotSupportedException(
                    $"{OwnerApi}: SelectMany in the array path takes a lambda over the element (c => c.Inner); got '{mc.Arguments[1]}'.");
            return PathOf(mc.Arguments[0], parameter, whole) + "." + FieldChainPath(lambda.Body, lambda.Parameters[0], whole);
        }

        if (e is MemberExpression)
            return FieldChainPath(e, parameter, whole);

        throw new NotSupportedException(
            $"{OwnerApi}: the array path supports a chain of fields (e => e.Contacts, e => e.HomeAddress.Building.Amenities) " +
            $"and SelectMany over an inner array (e => e.Contacts.SelectMany(c => c.Metadata)); got '{whole}'.");
    }

    private static string FieldChainPath(Expression expression, ParameterExpression parameter, LambdaExpression whole)
    {
        var e = GroupSelectorMembers.StripConvert(expression);
        if (e is MemberExpression member && GroupSelectorMembers.IsFieldChain(member)
            && GroupAggregateParser.TryMemberPath(member, parameter, out var path))
        {
            for (Expression? cur = member; cur is MemberExpression m; cur = m.Expression is null ? null : GroupSelectorMembers.StripConvert(m.Expression))
            {
                if (IsDictionary(m.Type))
                    throw new NotSupportedException(
                        $"{OwnerApi}: a dictionary in the array path is not supported ('{m}' in '{whole}').");
            }
            return path;
        }
        throw new NotSupportedException(
            $"{OwnerApi}: '{expression}' in the array path '{whole}' is not a chain of stored fields.");
    }

    private static bool IsDictionary(Type type)
        => type.GetInterfaces().Concat(new[] { type }).Any(i => i.IsGenericType &&
            (i.GetGenericTypeDefinition() == typeof(IDictionary<,>) || i.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>)));

    /// <summary>
    /// Keys: a field of the element (<c>c =&gt; c.Type</c>, <c>c =&gt; c.Address.City</c>), the element itself for an array
    /// of simple values (<c>s =&gt; s</c>), or an anonymous type of those. A key is a stored value, never a class.
    /// </summary>
    private List<GroupFieldRequest> ParseGroupFields(string? alias)
    {
        var parameter = _keySelector.Parameters[0];
        var body = GroupSelectorMembers.StripConvert(_keySelector.Body);
        var fields = new List<GroupFieldRequest>();

        if (body is NewExpression newExpr && newExpr.Members != null && body.Type.Name.Contains("AnonymousType"))
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var memberName = newExpr.Members[i].Name;
                fields.Add(new GroupFieldRequest
                {
                    FieldPath = KeyPath(newExpr.Arguments[i], parameter, $"key member '{memberName}'"),
                    Alias = memberName
                });
            }
            return fields;
        }

        fields.Add(new GroupFieldRequest
        {
            FieldPath = KeyPath(body, parameter, "the key"),
            Alias = alias ?? DefaultKeyAlias(body)
        });
        return fields;
    }

    private static string DefaultKeyAlias(Expression body)
        => body is MemberExpression m ? m.Member.Name : "Key";

    private string KeyPath(Expression expression, ParameterExpression parameter, string what)
    {
        var e = GroupSelectorMembers.StripConvert(expression);
        var type = Nullable.GetUnderlyingType(e.Type) ?? e.Type;
        if (type != typeof(string) && (type.IsClass || type.IsInterface) && !typeof(IRedbListItem).IsAssignableFrom(type))
            throw new NotSupportedException(
                $"{OwnerApi}: {what} ('{expression}') is a class; a key is a stored value: name a field of it.");
        return GroupAggregateParser.FieldPath(expression, parameter, _kind, OwnerApi, what);
    }

    /// <summary>The name the selector gives to g.Key (single key), used as the key's alias in SQL.</summary>
    private static string? KeyAliasFromSelector(LambdaExpression selector)
    {
        var groupParam = selector.Parameters[0];
        foreach (var (name, _, expr) in GroupSelectorMembers.Extract(selector, OwnerApi + ".SelectAsync"))
        {
            if (GroupSelectorMembers.StripConvert(expr) is MemberExpression { Member.Name: "Key" } m && m.Expression == groupParam)
                return name;
        }
        return null;
    }

    private List<AggregateRequest> ParseAggregations(LambdaExpression selector)
    {
        var aggregations = new List<AggregateRequest>();
        var groupParam = selector.Parameters[0];

        foreach (var (name, _, rawExpr) in GroupSelectorMembers.Extract(selector, OwnerApi + ".SelectAsync"))
        {
            var expr = GroupSelectorMembers.StripConvert(rawExpr);

            if (GroupSelectorMembers.IsKeyAccess(expr, groupParam))
                continue;

            if (GroupAggregateParser.IsAggCall(expr))
            {
                aggregations.Add(GroupAggregateParser.Parse((MethodCallExpression)expr, groupParam, name, _kind, OwnerApi + ".SelectAsync"));
                continue;
            }

            if (expr is MethodCallExpression wc && wc.Method.DeclaringType == typeof(Win))
                continue; // a window function: the window over the groups reads it

            if (!GroupSelectorMembers.ReferencesParameter(expr, groupParam))
                continue; // a client value, computed once at materialization

            throw new NotSupportedException(
                $"{OwnerApi}.SelectAsync: member '{name}' uses the group but is neither g.Key nor a direct Agg.* call ('{rawExpr}').");
        }

        return aggregations;
    }

    /// <summary>
    /// The array grouping as the source of a window over its groups.
    /// </summary>
    private sealed class ArrayWindowSource : IGroupedWindowSource
    {
        private readonly RedbArrayGroupedQueryable<TKey, TItem, TProps> _owner;
        public ArrayWindowSource(RedbArrayGroupedQueryable<TKey, TItem, TProps> owner) => _owner = owner;

        public string ProviderName => _owner._runner.ProviderName;
        public redb.Core.Providers.IListProvider? ListItems => _owner._runner.ListItems;

        public GroupItemKind Kind => _owner._kind;

        public List<GroupFieldRequest> GroupFields(LambdaExpression selector)
            => _owner.ParseGroupFields(KeyAliasFromSelector(selector));

        public Task<JsonDocument> ExecuteAsync(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window, CancellationToken cancellationToken)
            => _owner._runner.ExecuteAsync(Query(selector, aggregations, window), cancellationToken);

        public Task<string> GetSqlAsync(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window, CancellationToken cancellationToken)
            => _owner._runner.GetSqlAsync(Query(selector, aggregations, window), cancellationToken);

        private ArrayGroupQuery Query(LambdaExpression selector, List<AggregateRequest> aggregations, ArrayGroupWindow window) => new()
        {
            SchemeId = _owner._schemeId,
            ArrayPath = ParseArrayPath(_owner._arraySelector),
            GroupFields = GroupFields(selector),
            Aggregations = aggregations,
            Filter = _owner._filter,
            HavingJson = _owner.BuildHavingJson(),
            Window = window
        };
    }
}
