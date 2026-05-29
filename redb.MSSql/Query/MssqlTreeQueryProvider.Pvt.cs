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

namespace redb.MSSql.Query;

/// <summary>
/// Routes tree-subtree LINQ queries through the MSSql v2-pvt engine.
/// The free MSSql provider has no working <c>search_tree_objects_with_facets_base</c>
/// helper, so every tree request is compiled into a <c>dbo.pvt_build_query_sql</c>
/// invocation and materialized via <c>FOR JSON PATH</c>.
/// Port of the PostgreSQL PostgresTreeQueryProvider PVT partials.
/// </summary>
public partial class MssqlTreeQueryProvider
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
        // Free MSSql: HasAncestor / HasDescendant are routed through PVT here
        // because the legacy search_*_base SQL functions throw on this dialect.
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
        var sql = "SELECT CAST(COUNT(*) AS BIGINT) AS [Value] FROM (" + inner + ") t";
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
    /// Returns true when the request can be served by the v2-pvt orchestrator.
    /// See the PostgresTreeQueryProvider.ShouldRoutePvtTree routing matrix.
    /// </summary>
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
    /// MSSql tree_ids contract: NULL or a JSON array of bigints,
    /// e.g. <c>'[1,2,3]'</c>. <c>dbo.pvt_build_query_sql</c> parses it via OPENJSON.
    /// </summary>
    private static string FormatTreeIds(long[] ids) =>
        ids.Length == 0
            ? "NULL"
            : "N'[" + string.Join(",", ids.Select(id => id.ToString(CultureInfo.InvariantCulture))) + "]'";

    /// <summary>
    /// Builds the inner <c>SELECT _id ...</c> SQL via <c>dbo.pvt_build_query_sql</c>.
    /// The resulting SQL binds <c>@p0</c> = filter, and (when present) <c>@p1</c> = order.
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
        var limitArg = ignoreLimitOffset || !context.Limit.HasValue
            ? "NULL"
            : context.Limit.Value.ToString(CultureInfo.InvariantCulture);
        var offsetArg = ignoreLimitOffset
            ? "0"
            : (context.Offset ?? 0).ToString(CultureInfo.InvariantCulture);

        var treeIdsLiteral = FormatTreeIds(route.TreeIds);

        // dbo.pvt_build_query_sql signature (12 params, all required):
        //   @scheme_id, @filter, @limit, @offset, @order, @max_depth,
        //   @distinct, @source_mode, @tree_ids, @include_seed,
        //   @polymorphic, @distinct_on
        var orderArg = hasOrder ? "$2" : "NULL";
        var invocation = "SELECT dbo.pvt_build_query_sql("
            + context.SchemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, " + limitArg
            + ", " + offsetArg
            + ", " + orderArg
            + ", " + maxDepth.ToString(CultureInfo.InvariantCulture)
            + ", " + (context.IsDistinct ? "1" : "0")
            + ", N'" + route.SourceMode + "'"
            + ", " + treeIdsLiteral
            + ", " + (route.IncludeSeed ? "1" : "0")
            + ", 1"          // @polymorphic=1 matches Pro default
            + ", NULL)"      // @distinct_on
            + " AS [Value]";

        object filterParam = string.IsNullOrEmpty(route.FilterJson) ? "{}" : route.FilterJson;

        string? inner;
        if (hasOrder)
            inner = await _context.ExecuteScalarAsync<string>(invocation, filterParam, orderByJson!);
        else
            inner = await _context.ExecuteScalarAsync<string>(invocation, filterParam);

        if (string.IsNullOrWhiteSpace(inner))
            throw new InvalidOperationException(
                "dbo.pvt_build_query_sql returned an empty SQL string for tree query (scheme " + context.SchemeId + ").");
        return inner;
    }

    /// <summary>
    /// Wraps the inner <c>_id</c> list with <c>FOR JSON PATH</c> over the 21
    /// <c>_objects</c> columns to produce the same shape consumed by
    /// <see cref="TreeQueryProviderBase.DeserializeTreeObjects{TProps}"/>.
    /// </summary>
    private async Task<string?> ExecutePvtTreeAsync<TProps>(
        TreeQueryContext<TProps> context,
        PvtTreeRoute route,
        bool applyPaging) where TProps : class, new()
    {
        var inner = await BuildPvtTreeInnerSqlAsync(context, route, ignoreLimitOffset: !applyPaging);

        // FOR JSON PATH column aliases drive the JSON keys; mirror PG's
        // jsonb_build_object key naming exactly so the deserializer reuses
        // the same property map across providers.
        var sql =
            "SELECT (SELECT "
            + "o.[_id] AS id,"
            + "o.[_name] AS name,"
            + "o.[_id_scheme] AS scheme_id,"
            + "o.[_id_parent] AS parent_id,"
            + "o.[_id_owner] AS owner_id,"
            + "o.[_id_who_change] AS who_change_id,"
            + "o.[_date_create] AS date_create,"
            + "o.[_date_modify] AS date_modify,"
            + "o.[_date_begin] AS date_begin,"
            + "o.[_date_complete] AS date_complete,"
            + "o.[_key] AS [key],"
            + "o.[_value_long] AS value_long,"
            + "o.[_value_string] AS value_string,"
            + "o.[_value_guid] AS value_guid,"
            + "o.[_note] AS note,"
            + "o.[_value_bool] AS value_bool,"
            + "o.[_value_double] AS value_double,"
            + "o.[_value_numeric] AS value_numeric,"
            + "o.[_value_datetime] AS value_datetime,"
            + "o.[_value_bytes] AS value_bytes,"
            + "o.[_hash] AS hash"
            + " FROM (" + inner + ") sub JOIN dbo._objects o ON o._id = sub._id"
            + " FOR JSON PATH, INCLUDE_NULL_VALUES) AS [Value]";

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
