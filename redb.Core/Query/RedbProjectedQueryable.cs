using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Models.Entities;
using redb.Core.Query.Projection;

namespace redb.Core.Query;

/// <summary>
/// Implementation of LINQ query projections in REDB with filtering and sorting support
/// OPTIMIZED: Uses ProjectionFieldExtractor to load only required fields
/// </summary>
public class RedbProjectedQueryable<TProps, TResult> : IRedbProjectedQueryable<TResult>
    where TProps : class, new()
{
    private readonly IRedbQueryable<TProps> _sourceQuery;
    private readonly Expression<Func<RedbObject<TProps>, TResult>> _projection;

    // Provider and SchemeId for optimized loading
    private readonly IRedbQueryProvider? _provider;
    private readonly long _schemeId;

    // Chain of operations to execute after projection
    private readonly List<Expression<Func<TResult, bool>>> _wherePredicates = new();
    private readonly List<(Expression KeySelector, bool IsDescending)> _orderByExpressions = new();
    private readonly bool _isDistinct;  // Distinct flag for projection

    // S-4 (решение владельца 2026-09-04): Take/Skip, вызванные ПОСЛЕ операций, исполняемых в
    // памяти (Where/OrderBy/Distinct после Select), не уходят в источник - иначе сервер отрезал
    // бы страницу ДО фильтра и строки молча терялись. Такие Take/Skip копятся здесь и
    // применяются в памяти. Конвейер: Where* -> OrderBy* -> Distinct -> Skip/Take (в порядке
    // вызова). Take/Skip ДО первой in-memory операции уходят в источник, как раньше.
    private readonly List<(bool IsSkip, int Count)> _pagingOps = new();

    // Источник уже получил серверный Take/Skip (fast-path): пуш последующих Where в источник
    // запрещён - иначе фильтр применился бы ДО лимита и порядок вызовов был бы нарушен.
    private readonly bool _sourcePaged;

    // Extracted structure_ids for optimized loading
    private HashSet<long>? _projectedStructureIds;
    // Text field paths for SQL function search_objects_with_projection_by_paths
    private List<string>? _projectedFieldPaths;
    private readonly ProjectionFieldExtractor _fieldExtractor = new();

    public RedbProjectedQueryable(
        IRedbQueryable<TProps> sourceQuery,
        Expression<Func<RedbObject<TProps>, TResult>> projection)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
    }

    /// <summary>
    /// Constructor with provider for optimized loading
    /// </summary>
    public RedbProjectedQueryable(
        IRedbQueryable<TProps> sourceQuery,
        Expression<Func<RedbObject<TProps>, TResult>> projection,
        IRedbQueryProvider provider,
        long schemeId)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
        _provider = provider;
        _schemeId = schemeId;
    }

    // Private constructor for creating copies with additional operations
    private RedbProjectedQueryable(
        IRedbQueryable<TProps> sourceQuery,
        Expression<Func<RedbObject<TProps>, TResult>> projection,
        List<Expression<Func<TResult, bool>>> wherePredicates,
        List<(Expression KeySelector, bool IsDescending)> orderByExpressions,
        HashSet<long>? projectedStructureIds,
        List<string>? projectedFieldPaths,
        IRedbQueryProvider? provider,
        long schemeId,
        bool isDistinct = false,
        List<(bool IsSkip, int Count)>? pagingOps = null,
        bool sourcePaged = false)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
        _wherePredicates = new List<Expression<Func<TResult, bool>>>(wherePredicates);
        _orderByExpressions = new List<(Expression, bool)>(orderByExpressions);
        _projectedStructureIds = projectedStructureIds;
        _projectedFieldPaths = projectedFieldPaths;
        _provider = provider;
        _schemeId = schemeId;
        _isDistinct = isDistinct;
        if (pagingOps != null)
            _pagingOps = new List<(bool IsSkip, int Count)>(pagingOps);
        _sourcePaged = sourcePaged;
    }

    /// <summary>
    /// Extracted structure_ids for optimization (for provider access)
    /// </summary>
    public HashSet<long>? ProjectedStructureIds => _projectedStructureIds;

    /// <summary>
    /// Text field paths for SQL function search_objects_with_projection_by_paths
    /// </summary>
    public List<string>? ProjectedFieldPaths => _projectedFieldPaths;

    /// <summary>
    /// Projection expression (for provider access)
    /// </summary>
    public Expression<Func<RedbObject<TProps>, TResult>> Projection => _projection;

    public IRedbProjectedQueryable<TResult> Where(Expression<Func<TResult, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        // S-4 шаг 2 (решение владельца 2026-09-04): предикат по простым спроецированным
        // props-членам транслируется ОБРАТНО В ИСТОЧНИК - фильтр и последующие Take/Skip
        // остаются серверными. Всё нетранслируемое (вычисляемые члены, базовые поля, r целиком)
        // корректно фильтруется в памяти, как раньше - страдает только скорость. При уже
        // накопленном локальном Take/Skip пуш запрещён: фильтр обязан видеть отрезанную
        // страницу (семантика порядка вызовов).
        if (!_sourcePaged && _pagingOps.Count == 0 && TryPushPredicateToSource(predicate, out var pushedSource))
        {
            return new RedbProjectedQueryable<TProps, TResult>(
                pushedSource,
                _projection,
                _wherePredicates,
                _orderByExpressions,
                _projectedStructureIds,
                _projectedFieldPaths,
                _provider,
                _schemeId,
                _isDistinct,
                _pagingOps,
                _sourcePaged);
        }

        var newWherePredicates = new List<Expression<Func<TResult, bool>>>(_wherePredicates) { predicate };

        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery,
            _projection,
            newWherePredicates,
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct,
            _pagingOps,
            _sourcePaged);
    }

    /// <summary>
    /// Пытается переписать предикат над результатом проекции в предикат над TProps: члены
    /// результата подставляются их проекционными выражениями, цепочки x.Props.* переносятся на
    /// параметр TProps. Транслируются только простые member-проекции props-полей; любой другой
    /// случай - false, и предикат остаётся в памяти (fail-safe: корректность не зависит от пуша).
    /// </summary>
    private bool TryPushPredicateToSource(
        Expression<Func<TResult, bool>> predicate, out IRedbQueryable<TProps> pushed)
    {
        pushed = _sourceQuery;

        Dictionary<string, Expression>? map = null;
        if (_projection.Body is NewExpression ne && ne.Members != null)
        {
            map = new Dictionary<string, Expression>();
            for (int i = 0; i < ne.Members.Count; i++)
                map[ne.Members[i].Name] = ne.Arguments[i];
        }
        else if (_projection.Body is MemberInitExpression mi && mi.NewExpression.Arguments.Count == 0)
        {
            map = new Dictionary<string, Expression>();
            foreach (var binding in mi.Bindings)
            {
                if (binding is not MemberAssignment assignment) return false;
                map[assignment.Member.Name] = assignment.Expression;
            }
        }
        if (map == null) return false;

        var pParam = Expression.Parameter(typeof(TProps), "p");
        var rewriter = new ProjectedPredicateRewriter(
            predicate.Parameters[0], map, _projection.Parameters[0], pParam);
        var body = rewriter.Visit(predicate.Body);
        if (!rewriter.Success || body == null) return false;

        pushed = _sourceQuery.Where(Expression.Lambda<Func<TProps, bool>>(body, pParam));
        return true;
    }

    private sealed class ProjectedPredicateRewriter : ExpressionVisitor
    {
        private readonly ParameterExpression _rParam;
        private readonly Dictionary<string, Expression> _map;
        private readonly ParameterExpression _xParam;
        private readonly ParameterExpression _pParam;

        public bool Success { get; private set; } = true;

        public ProjectedPredicateRewriter(
            ParameterExpression rParam, Dictionary<string, Expression> map,
            ParameterExpression xParam, ParameterExpression pParam)
        {
            _rParam = rParam;
            _map = map;
            _xParam = xParam;
            _pParam = pParam;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == _rParam)
            {
                if (!_map.TryGetValue(node.Member.Name, out var mapped))
                {
                    Success = false;
                    return node;
                }
                var translated = TranslateProjectionExpr(mapped);
                if (translated == null)
                {
                    Success = false;
                    return node;
                }
                return translated;
            }
            return base.VisitMember(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _rParam)
                Success = false; // r используется целиком - такое не транслируем
            return base.VisitParameter(node);
        }

        /// <summary>x.Props.&lt;chain&gt; -> p.&lt;chain&gt;; всё прочее (x.Id, вычисления) - null.</summary>
        private Expression? TranslateProjectionExpr(Expression expr)
        {
            while (expr is UnaryExpression u &&
                   (u.NodeType == ExpressionType.Convert || u.NodeType == ExpressionType.ConvertChecked))
                expr = u.Operand;

            var chain = new Stack<System.Reflection.MemberInfo>();
            var cur = expr;
            while (cur is MemberExpression m)
            {
                if (m.Member.Name == "Props" && m.Expression == _xParam)
                {
                    Expression acc = _pParam;
                    while (chain.Count > 0)
                        acc = Expression.MakeMemberAccess(acc, chain.Pop());
                    return acc;
                }
                chain.Push(m.Member);
                cur = m.Expression;
            }
            return null;
        }
    }


    public IRedbProjectedQueryable<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        var newOrderByExpressions = new List<(Expression, bool)> { (keySelector, false) };

        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery,
            _projection,
            _wherePredicates,
            newOrderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct,
            _pagingOps,
            _sourcePaged);
    }

    public IRedbProjectedQueryable<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        var newOrderByExpressions = new List<(Expression, bool)> { (keySelector, true) };

        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery,
            _projection,
            _wherePredicates,
            newOrderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct,
            _pagingOps,
            _sourcePaged);
    }

    public IRedbProjectedQueryable<TResult> Take(int count)
    {
        // S-4: после in-memory операций Take применяется в памяти (_pagingOps), иначе сервер
        // отрежет страницу до фильтра; без них - прежний быстрый путь, LIMIT уходит в SQL.
        if (HasInMemoryOps)
            return WithPagingOp(isSkip: false, count);

        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery.Take(count),
            _projection,
            _wherePredicates,
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct,
            _pagingOps,
            sourcePaged: true);
    }

    public IRedbProjectedQueryable<TResult> Skip(int count)
    {
        if (HasInMemoryOps)
            return WithPagingOp(isSkip: true, count);

        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery.Skip(count),
            _projection,
            _wherePredicates,
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct,
            _pagingOps,
            sourcePaged: true);
    }

    private bool HasInMemoryOps =>
        _wherePredicates.Count > 0 || _orderByExpressions.Count > 0 || _isDistinct || _pagingOps.Count > 0;

    private RedbProjectedQueryable<TProps, TResult> WithPagingOp(bool isSkip, int count)
    {
        var copy = new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery,
            _projection,
            _wherePredicates,
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct,
            _pagingOps,
            _sourcePaged);
        copy._pagingOps.Add((isSkip, count));
        return copy;
    }

    public IRedbProjectedQueryable<TResult> Distinct()
    {
        // Distinct for projection is applied in memory after projection
        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery,
            _projection,
            _wherePredicates,
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            isDistinct: true,
            pagingOps: _pagingOps,
            sourcePaged: _sourcePaged);
    }

    public async Task<List<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        // S-7 (решение владельца 2026-09-03): Select с Agg.* маршрутизируется в аггрегатный путь -
        // одна строка агрегатов. Раньше пример из XML-доки Agg
        // (.Select(x => new { Total = Agg.Sum(...) })) бросал NotSupportedException из
        // скомпилированного маркера при материализации.
        if (_fieldExtractor.HasAggregations(_projection))
        {
            var aggregated = await _sourceQuery.AggregateAsync(_projection);
            IEnumerable<TResult> aggRows = [aggregated];
            foreach (var aggWhere in _wherePredicates)
                aggRows = aggRows.Where(aggWhere.Compile());
            return aggRows.ToList();
        }
        // OPTIMIZATION: Load only required structure_ids if possible
        List<RedbObject<TProps>> fullObjects;

        // LOG: Projection information

        // Extract text paths and structure_ids from projection if not yet extracted
        if (_projectedFieldPaths == null && _provider != null && _schemeId > 0)
        {
            // Extract text paths for SQL projection function (search_objects_with_projection_by_paths)
            _projectedFieldPaths = _fieldExtractor.ExtractFieldPathStrings(_projection);

            // Also extract structure_ids (used by Pro's ProLazyPropsLoader for filtered _values loading)
            var scheme = await _provider.GetSchemeAsync(_schemeId, cancellationToken: cancellationToken);
            if (scheme != null)
            {
                _projectedStructureIds = _fieldExtractor.ExtractStructureIds(scheme, _projection);
            }
        }

        // FIX: ALWAYS skip Props re-loading during projection.
        //
        // Projection flow is two-phase:
        //   Phase 1 (SQL):  search_objects_with_projection_by_paths returns JSON with
        //                   "properties" containing ONLY the projected fields.
        //   Phase 2 (C#):   Deserialization maps "properties" → Props via setter,
        //                   which sets _propsLoaded = true. Props is PARTIAL but sufficient.
        //   Phase 3 (C#):   compiledProjection(redbObj) extracts needed fields → TResult.
        //                   RedbObject<TProps> is then discarded.
        //
        // Without skipProps=true, MaterializeResultsFromJson would:
        //   - In EAGER mode: call LazyPropsLoader.LoadPropsForManyAsync which re-loads
        //     the FULL object via get_object_json, OVERWRITING the partial Props from SQL
        //     projection. This causes double DB round-trip and defeats the projection.
        //   - In LAZY mode: set _lazyLoader + _propsLoaded=false, causing the lazy loader
        //     to load full Props on first .Props access, again defeating projection.
        //
        // In both cases the final TResult is correct (projection extracts the right fields),
        // but the optimization is wasted — we load full object data anyway.
        //
        // The fix: tell MaterializeResultsFromJson to skip ALL Props post-processing.
        // Props data is already in the deserialized object from the SQL projection JSON.
        bool skipProps = true;


        // Try to use optimized loading via internal method
        if (_sourceQuery is RedbQueryable<TProps> redbQueryable)
        {
            // Set text paths in context BEFORE call
            // Context is read in PostgresQueryProvider to select SQL function
            redbQueryable.SetProjectedFieldPaths(_projectedFieldPaths);

            fullObjects = await redbQueryable.ToListWithProjectionAsync(_projectedStructureIds, skipProps, cancellationToken);
        }
        else
        {
            fullObjects = await _sourceQuery.ToListAsync(cancellationToken: cancellationToken);
        }


        // CRITICAL: Disable lazy loading BEFORE applying projection!
        // Data already loaded from SQL (search_objects_with_projection_by_paths),
        // result will be anonymous type — lazy loading not needed and dangerous.
        foreach (var obj in fullObjects)
        {
            obj._lazyLoader = null;
            // Ревью 2026-09-03: у объекта, в чьей ЧАСТИЧНОЙ выборке не нашлось ни одной строки
            // _values (все проецируемые поля null), Props приезжает null - а лямбда проекции
            // обязана исполниться. Внутри проекционного пути null-Props означает «пусто», а не
            // «не загружено»: RedbObject тут же выбрасывается, V4-семантика чтения не затронута.
            // Raw access: the getter of an object without loaded Props is a lazy load (or nothing, the loader is gone).
            if (obj.GetPropsDirectly() is null)
                obj.Props = new TProps();
        }

        var compiledProjection = _projection.Compile();

        // Apply projection to each object
        var projectedResults = fullObjects.Select(redbObj => compiledProjection(redbObj));

        // Apply Where filters after projection
        foreach (var wherePredicate in _wherePredicates)
        {
            var compiledPredicate = wherePredicate.Compile();
            projectedResults = projectedResults.Where(compiledPredicate);
        }

        // Apply OrderBy sorting after projection
        if (_orderByExpressions.Count > 0)
        {
            IOrderedEnumerable<TResult>? orderedResults = null;

            // S-2 (ревью 2026-09-03): null-ключ раньше подменялся new object(), и Comparer<object>
            // валил запрос ("does not implement IComparable"). Ключи сравниваются null-безопасно
            // с LINQ-семантикой: null меньше любого значения.
            var nullSafe = Comparer<object?>.Create(static (a, b) =>
                a == null && b == null ? 0 :
                a == null ? -1 :
                b == null ? 1 : System.Collections.Comparer.Default.Compare(a, b));

            for (int i = 0; i < _orderByExpressions.Count; i++)
            {
                var (keySelector, isDescending) = _orderByExpressions[i];
                var compiledDelegate = ((LambdaExpression)keySelector).Compile();
                Func<TResult, object?> key = item => compiledDelegate.DynamicInvoke(item);

                if (i == 0)
                    orderedResults = isDescending
                        ? projectedResults.OrderByDescending(key, nullSafe)
                        : projectedResults.OrderBy(key, nullSafe);
                else
                    orderedResults = isDescending
                        ? orderedResults!.ThenByDescending(key, nullSafe)
                        : orderedResults!.ThenBy(key, nullSafe);
            }

            if (orderedResults != null)
                projectedResults = orderedResults;
        }

        // Apply Distinct if flag is set
        if (_isDistinct)
        {
            projectedResults = projectedResults.Distinct();
        }

        // S-4: Take/Skip, вызванные после in-memory операций, применяются здесь в порядке вызова.
        foreach (var (isSkip, n) in _pagingOps)
            projectedResults = isSkip ? projectedResults.Skip(n) : projectedResults.Take(n);

        return projectedResults.ToList();
    }

    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        // S-3 (ревью 2026-09-03): Distinct применяется в памяти в ToListAsync - Count обязан
        // считать то же самое. Агрегатная проекция (S-7) тем же путём: всегда одна строка.
        if (_wherePredicates.Count > 0 || _isDistinct || _pagingOps.Count > 0 || _fieldExtractor.HasAggregations(_projection))
        {
            var results = await ToListAsync(cancellationToken: cancellationToken);
            return results.Count;
        }

        // Otherwise count does not depend on the projection
        return await _sourceQuery.CountAsync(cancellationToken: cancellationToken);
    }

    public async Task<TResult?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        // For FirstOrDefault apply all operations and take first element
        var results = await ToListAsync(cancellationToken: cancellationToken);
        return results.FirstOrDefault();
    }

    /// <summary>
    /// Get projection info: SQL function and structure_ids that will be loaded.
    /// </summary>
    public async Task<string> GetProjectionInfoAsync(CancellationToken cancellationToken = default)
    {
        HashSet<long>? structureIds = _projectedStructureIds;

        if (structureIds == null && _provider != null && _schemeId > 0)
        {
            var scheme = await _provider.GetSchemeAsync(_schemeId, cancellationToken: cancellationToken);
            if (scheme != null)
            {
                structureIds = _fieldExtractor.ExtractStructureIds(scheme, _projection);
                _projectedStructureIds = structureIds;
            }
        }

        var count = structureIds?.Count ?? 0;

        return count > 0
            ? $"Projection: {count} structure_ids"
            : "Full load";
    }
}
