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

#pragma warning disable CS0618 // ExecuteTreeToListAsync base is [Obsolete] (provider overrides it).

namespace redb.SQLite.Query;

/// <summary>
/// Free PG: implements <c>WhereHasAncestor</c> / <c>WhereHasDescendant</c>
/// via the v2-pvt orchestrator (the legacy <c>search_*_with_facets_base</c>
/// SQL helpers were removed). Mirrors the inversion logic used by the Pro
/// CTE+PVT path: find the target ids first, then recurse back into
/// <see cref="SqliteTreeQueryProvider.ExecuteTreeToListAsync"/> with a
/// rewritten <see cref="TreeQueryContext{TProps}"/>.
/// </summary>
public partial class SqliteTreeQueryProvider
{
    /// <summary>
    /// Step 1+2 of <c>WhereHasAncestor</c> inversion:
    ///   * find ancestor ids of <c>hasAncestorFilter.TargetSchemeId</c> matching the inner condition;
    ///   * rewrite the context (<c>ParentIds = ancestorIds</c>, drop the HasAncestor filter)
    ///     and recurse into <see cref="ExecuteTreeToListAsync"/> to fetch the descendants.
    /// </summary>
    private async Task<object> ExecutePvtHasAncestorToListAsync<TProps>(
        TreeQueryContext<TProps> context,
        TreeFilter hasAncestorFilter) where TProps : class, new()
    {
        var ancestorIds = await FindIdsViaPvtAsync(
            hasAncestorFilter,
            scopeRootIds: context.RootObjectId.HasValue ? new[] { context.RootObjectId.Value } : null);

        if (ancestorIds.Length == 0)
            return new List<TreeRedbObject<TProps>>();

        var optimizedContext = context.Clone();
        optimizedContext.TreeFilters = new List<TreeFilter>(
            context.TreeFilters.Where(f => f != hasAncestorFilter));
        optimizedContext.ParentIds = ancestorIds;
        // RootObjectId stays as-is; descendants under it are the answer set.

        return await ExecuteTreeToListAsync(optimizedContext);
    }

    /// <summary>
    /// Step 1+2+3 of <c>WhereHasDescendant</c> inversion:
    ///   * find descendant ids of <c>hasDescendantFilter.TargetSchemeId</c> matching the inner condition;
    ///   * walk up via <c>Query_GetParentIdsFromDescendantsSql</c> to collect every ancestor id;
    ///   * rewrite the context (drop HasDescendant filter, AND-merge an
    ///     <see cref="InExpression"/> on <c>_id</c>) and recurse.
    /// </summary>
    private async Task<object> ExecutePvtHasDescendantToListAsync<TProps>(
        TreeQueryContext<TProps> context,
        TreeFilter hasDescendantFilter) where TProps : class, new()
    {
        var descendantIds = await FindIdsViaPvtAsync(hasDescendantFilter, scopeRootIds: null);
        if (descendantIds.Length == 0)
            return new List<TreeRedbObject<TProps>>();

        // Walk up the parent chain to collect every potential matching object id.
        var depthLimit = hasDescendantFilter.MaxDepth ?? 50;
        var idsString = string.Join(",", descendantIds.Select(id => id.ToString(CultureInfo.InvariantCulture)));
        var sql = _sql.Query_GetParentIdsFromDescendantsSql(idsString, depthLimit);
        var parentIdList = await _context.QueryScalarListAsync<long>(sql);
        var parentIds = parentIdList.ToArray();
        if (parentIds.Length == 0)
            return new List<TreeRedbObject<TProps>>();

        var idsFilter = new InExpression(
            new redb.Core.Query.QueryExpressions.PropertyInfo("_id", typeof(long), true),
            parentIds.Cast<object>().ToList());

        var optimizedContext = context.Clone();
        optimizedContext.TreeFilters = new List<TreeFilter>(
            context.TreeFilters.Where(f => f != hasDescendantFilter));
        optimizedContext.Filter = optimizedContext.Filter != null
            ? new LogicalExpression(LogicalOperator.And, new FilterExpression[] { optimizedContext.Filter, idsFilter })
            : (FilterExpression)idsFilter;

        return await ExecuteTreeToListAsync(optimizedContext);
    }

    /// <summary>
    /// Compiles the inner <c>SELECT _id ...</c> SQL for the given tree filter
    /// (HasAncestor / HasDescendant — both use the same flat or
    /// <c>tree_descendants</c> route depending on <paramref name="scopeRootIds"/>)
    /// via <c>pvt_build_query_sql</c>, then executes it and returns the
    /// matching object ids.
    /// </summary>
    private async Task<long[]> FindIdsViaPvtAsync(TreeFilter filter, long[]? scopeRootIds)
    {
        if (filter.TargetSchemeId is null || filter.TargetSchemeId.Value == 0)
            return Array.Empty<long>();

        var conditionJson = JsonSerializer.Serialize(
            filter.FilterConditions,
            new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });

        var sourceMode = scopeRootIds != null && scopeRootIds.Length > 0 ? "tree_descendants" : "flat";
        // SQLite: p_tree_ids is a JSON array text (parsed by pvtInListFromJsonArray), not a PG bigint[].
        var treeIdsLiteral = scopeRootIds == null || scopeRootIds.Length == 0
            ? "NULL"
            : "'[" + string.Join(",", scopeRootIds.Select(id => id.ToString(CultureInfo.InvariantCulture))) + "]'";
        var maxDepth = filter.MaxDepth ?? 50;

        var invocation = "SELECT pvt_build_query_sql("
            + filter.TargetSchemeId.Value.ToString(CultureInfo.InvariantCulture)
            + ", $1, NULL, 0, NULL, "
            + maxDepth.ToString(CultureInfo.InvariantCulture)
            + ", 0, '" + sourceMode + "', " + treeIdsLiteral
            + ", 0, 1) AS \"Value\"";

        object filterParam = string.IsNullOrEmpty(conditionJson) || conditionJson == "null"
            ? (object)"{}"
            : (object)conditionJson;

        _logger?.LogDebug(
            "PVT HasAncestor/Descendant Build: TargetSchemeId={SchemeId}, Mode={Mode}, Scope={Scope}, Cond={Cond}",
            filter.TargetSchemeId.Value, sourceMode, treeIdsLiteral, conditionJson);

        var inner = await _context.ExecuteScalarAsync<string>(invocation, filterParam);
        if (string.IsNullOrWhiteSpace(inner))
            return Array.Empty<long>();

        var idSql = "SELECT _id AS \"Value\" FROM (" + inner + ") sub";
        var ids = await _context.QueryScalarListAsync<long>(idSql);
        return ids.ToArray();
    }
}
