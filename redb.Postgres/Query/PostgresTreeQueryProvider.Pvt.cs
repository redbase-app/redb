using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Entities;
using redb.Core.Query.Base;
using redb.Core.Query.QueryExpressions;

#pragma warning disable CS0672 // Member overrides obsolete member (legacy fallback retained for HasAncestor/HasDescendant + flat search)
#pragma warning disable CS0618 // Type or member is obsolete

namespace redb.Postgres.Query;

/// <summary>
/// Routes tree-subtree LINQ queries through the v2-pvt engine when the
/// generated filter contains expression-form predicates (<c>$expr</c>) that
/// the legacy <c>search_tree_objects_with_facets_base</c> SQL function does
/// not understand. For all other tree filters we keep the legacy path
/// (faster index plans, no behavioural change).
/// </summary>
public partial class PostgresTreeQueryProvider
{
    /// <summary>
    /// Describes how a tree LINQ query is mapped onto the v2-pvt
    /// <c>pvt_build_query_sql</c> orchestrator: which source mode to use,
    /// which seed ids (if any) feed the recursive walks, whether to keep
    /// the seed rows in the output, and which filter JSON to bind.
    /// </summary>
    private readonly struct PvtTreeRoute
    {
        public PvtTreeRoute(string sourceMode, long[] treeIds, bool includeSeed, string filterJson)
        {
            SourceMode = sourceMode;
            TreeIds = treeIds;
            IncludeSeed = includeSeed;
            FilterJson = filterJson;
        }

        public string SourceMode { get; }
        public long[] TreeIds { get; }
        public bool IncludeSeed { get; }
        public string FilterJson { get; }
    }

    /// <inheritdoc />
    protected override async Task<object> ExecuteTreeToListAsync<TProps>(TreeQueryContext<TProps> context)
    {
        // Free PG: HasAncestor / HasDescendant are routed through PVT here
        // because the legacy search_*_base SQL functions are gone.
        var hasAncestorFilter = GetOptimizableHasAncestorFilter(context);
        if (hasAncestorFilter != null)
            return await ExecutePvtHasAncestorToListAsync(context, hasAncestorFilter);

        var hasDescendantFilter = GetOptimizableHasDescendantFilter(context);
        if (hasDescendantFilter != null)
            return await ExecutePvtHasDescendantToListAsync(context, hasDescendantFilter);

        if (!ShouldRoutePvtTree(context, out var route))
            return await base.ExecuteTreeToListAsync<TProps>(context);

        try
        {
            var objectsJson = await ExecutePvtTreeAsync(context, route, applyPaging: true);
            var result = DeserializeTreeObjects<TProps>(objectsJson);

            if (result.Count > 0 && _lazyPropsLoader != null)
            {
                var useLazyOnDemand = context.UseLazyLoading ?? _configuration.EnableLazyLoadingForProps;
                if (useLazyOnDemand)
                {
                    foreach (var treeObj in result)
                    {
                        if (treeObj.id > 0)
                        {
                            treeObj._lazyLoader = _lazyPropsLoader;
                            treeObj._propsLoaded = false;
                        }
                    }
                }
                else
                {
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    var baseObjects = result.Cast<redb.Core.Models.Entities.RedbObject<TProps>>().ToList();
                    await _lazyPropsLoader.LoadPropsForManyAsync(baseObjects, context.PropsDepth);
                    sw.Stop();
                    _logger?.LogInformation("Props loaded via batch (PVT tree) in {ElapsedMs} ms", sw.ElapsedMilliseconds);
                }
            }

            return (object)result;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error executing PVT tree ToList query");
            throw;
        }
    }

    /// <inheritdoc />
    protected override async Task<int> ExecuteTreeCountAsync<TProps>(TreeQueryContext<TProps> context)
    {
        // Free PG: HasAncestor / HasDescendant — execute list path and count.
        // The two operators rewrite the context via recursive ExecuteTreeToListAsync,
        // so the count cannot be issued as a single SQL aggregate at this layer.
        var hasAncestorFilter = GetOptimizableHasAncestorFilter(context);
        if (hasAncestorFilter != null)
        {
            var list = await ExecutePvtHasAncestorToListAsync(context, hasAncestorFilter);
            return ((IList<TreeRedbObject<TProps>>)list).Count;
        }
        var hasDescendantFilter = GetOptimizableHasDescendantFilter(context);
        if (hasDescendantFilter != null)
        {
            var list = await ExecutePvtHasDescendantToListAsync(context, hasDescendantFilter);
            return ((IList<TreeRedbObject<TProps>>)list).Count;
        }

        if (!ShouldRoutePvtTree(context, out var route))
            return await base.ExecuteTreeCountAsync<TProps>(context);

        var inner = await BuildPvtTreeInnerSqlAsync(context, route, ignoreLimitOffset: true);
        var sql = "SELECT COUNT(*)::bigint AS \"Value\" FROM (" + inner + ") t";
        object filterParam = string.IsNullOrEmpty(route.FilterJson) ? "{}" : route.FilterJson;
        var orderByJson = context.Orderings.Count > 0
            ? _facetBuilder.BuildOrderBy(context.Orderings)
            : null;
        var hasOrder = !string.IsNullOrEmpty(orderByJson) && orderByJson != "null" && orderByJson != "[]";
        long? count;
        if (hasOrder)
            count = await _context.ExecuteScalarAsync<long?>(sql, filterParam, orderByJson!);
        else
            count = await _context.ExecuteScalarAsync<long?>(sql, filterParam);
        return checked((int)(count ?? 0));
    }

    /// <summary>
    /// Returns true when the request can be served by the v2-pvt
    /// orchestrator. Picks the appropriate <c>p_source_mode</c> and seed
    /// id array based on the tree filters present in the context.
    /// </summary>
    /// <remarks>
    /// Routes covered:
    /// <list type="bullet">
    /// <item><c>WhereRoots()</c> → <c>tree_roots</c></item>
    /// <item><c>WhereLeaves()</c> → <c>tree_leaves</c></item>
    /// <item><c>WhereChildrenOf(id)</c> → <c>tree_children</c> + tree_ids=[id]</item>
    /// <item><c>WhereDescendantsOf(id)</c> → <c>tree_descendants</c> + tree_ids=[id]</item>
    /// <item>RootObjectId / ParentIds set → <c>tree_descendants</c></item>
    /// <item>Whole-scheme (no root, no special filter) → <c>flat</c></item>
    /// <item><c>WhereLevel(...)</c> is attached as a <c>$level</c> facet
    /// filter and combined with any of the above modes.</item>
    /// </list>
    /// <c>WhereHasAncestor</c> / <c>WhereHasDescendant</c> are NOT served
    /// here — they use their own optimized base-class path that builds
    /// SQL out of two flat queries with logic inversion.
    /// </remarks>
    private bool ShouldRoutePvtTree<TProps>(TreeQueryContext<TProps> context, out PvtTreeRoute route)
        where TProps : class, new()
    {
        route = new PvtTreeRoute("flat", Array.Empty<long>(), false, "{}");

        if (context.TreeFilters != null && context.TreeFilters.Any(f =>
                f.Operator == TreeFilterOperator.HasAncestor ||
                f.Operator == TreeFilterOperator.HasDescendant))
            return false;
        if (GetOptimizableHasAncestorFilter(context) != null) return false;
        if (GetOptimizableHasDescendantFilter(context) != null) return false;

        var treeFilters = context.TreeFilters ?? new List<TreeFilter>();

        // Determine source_mode / tree_ids / include_seed.
        var sourceMode = "flat";
        long[] treeIds = Array.Empty<long>();
        var includeSeed = false;

        var childrenOf = treeFilters.FirstOrDefault(f => f.Operator == TreeFilterOperator.ChildrenOf);
        var descendantsOf = treeFilters.FirstOrDefault(f => f.Operator == TreeFilterOperator.DescendantsOf);
        var isRoot = treeFilters.Any(f => f.Operator == TreeFilterOperator.IsRoot);
        var isLeaf = treeFilters.Any(f => f.Operator == TreeFilterOperator.IsLeaf);

        if (descendantsOf != null)
        {
            sourceMode = "tree_descendants";
            treeIds = new[] { Convert.ToInt64(descendantsOf.Value, CultureInfo.InvariantCulture) };
            includeSeed = false;
        }
        else if (childrenOf != null)
        {
            sourceMode = "tree_children";
            treeIds = new[] { Convert.ToInt64(childrenOf.Value, CultureInfo.InvariantCulture) };
        }
        else if (isRoot)
        {
            sourceMode = "tree_roots";
        }
        else if (isLeaf)
        {
            sourceMode = "tree_leaves";
        }
        else if (context.RootObjectId.HasValue
                 || (context.ParentIds != null && context.ParentIds.Length > 0))
        {
            sourceMode = "tree_descendants";
            treeIds = (context.ParentIds != null && context.ParentIds.Length > 0)
                ? context.ParentIds
                : new[] { context.RootObjectId!.Value };
            includeSeed = false;
        }

        // Build the filter JSON by combining the user .Where() predicates
        // and any tree-level filters PVT understands natively ($level).
        var merged = new Dictionary<string, object?>();
        if (context.Filter != null)
        {
            var userJson = _facetBuilder.BuildFacetFilters(context.Filter);
            if (!string.IsNullOrEmpty(userJson) && userJson != "{}")
            {
                using var doc = JsonDocument.Parse(userJson);
                foreach (var prop in doc.RootElement.EnumerateObject())
                    merged[prop.Name] = prop.Value.Clone();
            }
        }

        foreach (var tf in treeFilters)
        {
            if (tf.Operator != TreeFilterOperator.Level) continue;
            if (tf.Value is int lvl)
                merged["$level"] = lvl;
            else if (tf.FilterConditions != null)
                merged["$level"] = tf.FilterConditions;
        }

        var filterJson = merged.Count == 0
            ? "{}"
            : JsonSerializer.Serialize(merged, new JsonSerializerOptions
            {
                WriteIndented = false,
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            });

        route = new PvtTreeRoute(sourceMode, treeIds, includeSeed, filterJson);
        return true;
    }

    /// <summary>
    /// Builds the inner <c>SELECT _id ...</c> SQL via
    /// <c>pvt_build_query_sql</c>. The resulting SQL binds
    /// <c>$1::jsonb</c> = filter, and (when present) <c>$2::jsonb</c> = order.
    /// </summary>
    private async Task<string> BuildPvtTreeInnerSqlAsync<TProps>(
        TreeQueryContext<TProps> context,
        PvtTreeRoute route,
        bool ignoreLimitOffset) where TProps : class, new()
    {
        var orderByJson = context.Orderings.Count > 0
            ? _facetBuilder.BuildOrderBy(context.Orderings)
            : null;
        var hasOrder = !string.IsNullOrEmpty(orderByJson) && orderByJson != "null" && orderByJson != "[]";

        var maxDepth = context.MaxRecursionDepth ?? 10;
        var limitArg  = ignoreLimitOffset || !context.Limit.HasValue
            ? "NULL"
            : context.Limit.Value.ToString(CultureInfo.InvariantCulture);
        var offsetArg = ignoreLimitOffset
            ? "0"
            : (context.Offset ?? 0).ToString(CultureInfo.InvariantCulture);

        var treeIdsLiteral = route.TreeIds.Length == 0
            ? "NULL::bigint[]"
            : "ARRAY["
                + string.Join(",", route.TreeIds.Select(id => id.ToString(CultureInfo.InvariantCulture)))
                + "]::bigint[]";

        // pvt_build_query_sql signature:
        //   p_scheme_id, p_filter, p_limit, p_offset, p_order, p_max_depth,
        //   p_distinct, p_source_mode, p_tree_ids, p_include_seed,
        //   p_polymorphic, p_distinct_on
        var orderArg = hasOrder ? "$2::jsonb" : "NULL::jsonb";
        var invocation = "SELECT pvt_build_query_sql("
            + context.SchemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1::jsonb, " + limitArg
            + ", " + offsetArg
            + ", " + orderArg
            + ", " + maxDepth.ToString(CultureInfo.InvariantCulture)
            + ", " + (context.IsDistinct ? "true" : "false")
            + ", '" + route.SourceMode + "'"
            + ", " + treeIdsLiteral
            + ", " + (route.IncludeSeed ? "true" : "false")
            + ", true)"   // p_polymorphic=true matches Pro default
            + " AS \"Value\"";

        object filterParam = string.IsNullOrEmpty(route.FilterJson) ? "{}" : route.FilterJson;

        string? inner;
        if (hasOrder)
            inner = await _context.ExecuteScalarAsync<string>(invocation, filterParam, orderByJson!);
        else
            inner = await _context.ExecuteScalarAsync<string>(invocation, filterParam);

        if (string.IsNullOrWhiteSpace(inner))
            throw new InvalidOperationException(
                "pvt_build_query_sql returned an empty SQL string for tree query (scheme " + context.SchemeId + ").");
        return inner;
    }

    /// <summary>
    /// Wraps the inner <c>_id</c> list with the same <c>jsonb_agg(jsonb_build_object(...))</c>
    /// shape produced by the legacy <c>search_tree_objects_with_facets_base</c>
    /// function and executes it. Returns the raw JSON array string.
    /// </summary>
    private async Task<string?> ExecutePvtTreeAsync<TProps>(
        TreeQueryContext<TProps> context,
        PvtTreeRoute route,
        bool applyPaging) where TProps : class, new()
    {
        var inner = await BuildPvtTreeInnerSqlAsync(context, route, ignoreLimitOffset: !applyPaging);

        var sql =
            "SELECT jsonb_agg(jsonb_build_object("
            + "'id', o._id,"
            + "'name', o._name,"
            + "'scheme_id', o._id_scheme,"
            + "'parent_id', o._id_parent,"
            + "'owner_id', o._id_owner,"
            + "'who_change_id', o._id_who_change,"
            + "'date_create', o._date_create,"
            + "'date_modify', o._date_modify,"
            + "'date_begin', o._date_begin,"
            + "'date_complete', o._date_complete,"
            + "'key', o._key,"
            + "'value_long', o._value_long,"
            + "'value_string', o._value_string,"
            + "'value_guid', o._value_guid,"
            + "'note', o._note,"
            + "'value_bool', o._value_bool,"
            + "'value_double', o._value_double,"
            + "'value_numeric', o._value_numeric,"
            + "'value_datetime', o._value_datetime,"
            + "'value_bytes', o._value_bytes,"
            + "'hash', o._hash"
            + "))::text AS \"Value\""
            + " FROM (" + inner + ") sub JOIN _objects o ON o._id = sub._id";

        object filterParam = string.IsNullOrEmpty(route.FilterJson) ? "{}" : route.FilterJson;
        var orderByJson = context.Orderings.Count > 0
            ? _facetBuilder.BuildOrderBy(context.Orderings)
            : null;
        var hasOrder = !string.IsNullOrEmpty(orderByJson) && orderByJson != "null" && orderByJson != "[]";

        if (hasOrder)
            return await _context.ExecuteScalarAsync<string>(sql, filterParam, orderByJson!);
        return await _context.ExecuteScalarAsync<string>(sql, filterParam);
    }
}
