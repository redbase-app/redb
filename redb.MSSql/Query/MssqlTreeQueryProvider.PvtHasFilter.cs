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

namespace redb.MSSql.Query;

/// <summary>
/// Free MSSql: implements <c>WhereHasAncestor</c> / <c>WhereHasDescendant</c>
/// via the v2-pvt orchestrator (the legacy <c>search_*_with_facets_base</c>
/// SQL helpers throw on this dialect). Port of the PostgreSQL
/// PostgresTreeQueryProvider.PvtHasFilter partial.
/// </summary>
public partial class MssqlTreeQueryProvider
{
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

        return await ExecuteTreeToListAsync(optimizedContext);
    }

    private async Task<object> ExecutePvtHasDescendantToListAsync<TProps>(
        TreeQueryContext<TProps> context,
        TreeFilter hasDescendantFilter) where TProps : class, new()
    {
        var descendantIds = await FindIdsViaPvtAsync(hasDescendantFilter, scopeRootIds: null);
        if (descendantIds.Length == 0)
            return new List<TreeRedbObject<TProps>>();

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
    /// Compiles the inner <c>SELECT _id ...</c> SQL for the given tree filter via
    /// <c>dbo.pvt_build_query_sql</c> and returns the matching object ids.
    /// </summary>
    private async Task<long[]> FindIdsViaPvtAsync(TreeFilter filter, long[]? scopeRootIds)
    {
        if (filter.TargetSchemeId is null || filter.TargetSchemeId.Value == 0)
            return Array.Empty<long>();

        var conditionJson = JsonSerializer.Serialize(
            filter.FilterConditions,
            new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });

        var sourceMode = scopeRootIds != null && scopeRootIds.Length > 0 ? "tree_descendants" : "flat";
        var treeIdsLiteral = scopeRootIds == null || scopeRootIds.Length == 0
            ? "NULL"
            : "N'[" + string.Join(",", scopeRootIds.Select(id => id.ToString(CultureInfo.InvariantCulture))) + "]'";
        var maxDepth = filter.MaxDepth ?? 50;

        // 12-arg call: scheme, $1=filter, NULL limit, 0 offset, NULL order,
        // max_depth, 0 distinct, source_mode, tree_ids, 0 include_seed,
        // 1 polymorphic, NULL distinct_on.
        var invocation = "SELECT dbo.pvt_build_query_sql("
            + filter.TargetSchemeId.Value.ToString(CultureInfo.InvariantCulture)
            + ", $1, NULL, 0, NULL, "
            + maxDepth.ToString(CultureInfo.InvariantCulture)
            + ", 0, N'" + sourceMode + "', " + treeIdsLiteral
            + ", 0, 1, NULL) AS [Value]";

        object filterParam = string.IsNullOrEmpty(conditionJson) || conditionJson == "null"
            ? (object)"{}"
            : (object)conditionJson;

        _logger?.LogDebug(
            "PVT HasAncestor/Descendant Build (MSSql): TargetSchemeId={SchemeId}, Mode={Mode}, Scope={Scope}, Cond={Cond}",
            filter.TargetSchemeId.Value, sourceMode, treeIdsLiteral, conditionJson);

        var inner = await _context.ExecuteScalarAsync<string>(invocation, filterParam);
        if (string.IsNullOrWhiteSpace(inner))
            return Array.Empty<long>();

        var idSql = "SELECT _id AS [Value] FROM (" + inner + ") sub";
        var ids = await _context.QueryScalarListAsync<long>(idSql);
        return ids.ToArray();
    }
}
