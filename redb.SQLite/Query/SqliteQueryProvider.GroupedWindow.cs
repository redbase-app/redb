using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Grouping;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Window;

namespace redb.SQLite.Query;

/// <summary>
/// PVT-backed GroupBy + Window Functions for free SQLite. Builds the
/// inner grouped SQL via <c>pvt_build_groupby_sql</c>, then wraps it in an
/// outer SELECT that applies the requested window functions over the
/// already-grouped row set, finally wrapping the whole thing in
/// <c>json_agg(row_to_json(t))</c> for one-shot materialization.
///
/// Window OVER clause refs:
///   * partition_by: must reference a group alias.
///   * order_by:     either a group alias or the synthetic
///                   <c>Agg_&lt;func&gt;_&lt;field&gt;</c> pattern emitted by
///                   <see cref="GroupedWindowedQueryable{TKey,TProps}"/>;
///                   the latter is resolved against the aggregations list
///                   to recover the actual aggregate alias.
/// </summary>
public partial class SqliteQueryProvider
{
    /// <inheritdoc />
    public override Task<JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null)
    {
        return ExecuteGroupedWindowInternalAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }

    /// <inheritdoc />
    public override Task<JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter)
    {
        var filterJson = filter is null ? null : _facetBuilder.BuildFacetFilters(filter);
        return ExecuteGroupedWindowInternalAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }

    /// <inheritdoc />
    public override async Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null)
    {
        return await BuildGroupedWindowSqlAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }

    /// <inheritdoc />
    public override async Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter)
    {
        var filterJson = filter is null ? null : _facetBuilder.BuildFacetFilters(filter);
        return await BuildGroupedWindowSqlAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }

    private async Task<JsonDocument?> ExecuteGroupedWindowInternalAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson)
    {
        var sql = await BuildGroupedWindowSqlAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);

        // SQLite has no json_agg/row_to_json: run the windowed SELECT and serialize rows in C#.
        var jsonArray = await SqliteConn.QueryRowsAsJsonAsync(sql);
        if (string.IsNullOrEmpty(jsonArray))
            return null;
        return JsonDocument.Parse(jsonArray);
    }

    private async Task<string> BuildGroupedWindowSqlAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson)
    {
        var groupList = groupFields?.ToList() ?? new List<GroupFieldRequest>();
        var aggList = aggregations?.ToList() ?? new List<AggregateRequest>();
        var windowList = windowFuncs?.ToList() ?? new List<WindowFuncRequest>();
        var partitionList = partitionBy?.ToList() ?? new List<WindowFieldRequest>();
        var orderList = orderBy?.ToList() ?? new List<WindowOrderRequest>();

        if (groupList.Count == 0)
            throw new ArgumentException("groupFields must contain at least one entry.", nameof(groupFields));
        if (windowList.Count == 0)
            throw new ArgumentException("windowFuncs must contain at least one entry.", nameof(windowFuncs));

        var groupByJson = BuildPvtGroupByJson(groupList);
        var aggregationsJson = aggList.Count == 0 ? null : BuildPvtAggregationsJson(aggList);

        object filterParam = string.IsNullOrEmpty(filterJson) || filterJson == "{}" || filterJson == "null"
            ? DBNull.Value
            : (object)filterJson!;
        object aggParam = aggregationsJson is null ? DBNull.Value : (object)aggregationsJson;

        var invocation = "SELECT pvt_build_groupby_sql("
            + schemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, $2, $3, NULL, NULL, NULL, 0, 'flat', NULL, NULL, 1, 1) AS \"Value\"";

        _logger?.LogDebug("PVT GroupedWindow Build: SchemeId={SchemeId}, GroupBy={GroupBy}, Aggs={Aggs}, Filter={Filter}",
            schemeId, groupByJson, aggregationsJson ?? "null", filterJson ?? "null");

        var innerSql = await _context.ExecuteScalarAsync<string>(invocation, filterParam, groupByJson, aggParam);
        if (string.IsNullOrWhiteSpace(innerSql))
            throw new InvalidOperationException(
                "pvt_build_groupby_sql returned an empty SQL string for scheme " + schemeId + ".");

        // Returns the windowed SELECT itself; the caller serializes rows to JSON in C#
        // (SQLite has no json_agg/row_to_json). Preview surfaces see the real SQL.
        return BuildWindowedSelect(
            innerSql, groupList, aggList, windowList, partitionList, orderList);
    }

    /// <summary>
    /// Builds the windowed wrapper:
    ///   <c>SELECT g.*, &lt;winExpr&gt; AS "&lt;alias&gt;" FROM (&lt;innerGroupBy&gt;) g</c>.
    /// Window OVER refs are resolved against group/aggregate aliases.
    /// </summary>
    private static string BuildWindowedSelect(
        string innerGroupBySql,
        IList<GroupFieldRequest> groupList,
        IList<AggregateRequest> aggList,
        IList<WindowFuncRequest> windowList,
        IList<WindowFieldRequest> partitionList,
        IList<WindowOrderRequest> orderList)
    {
        var sb = new StringBuilder();
        sb.Append("SELECT g.*");

        var overClause = BuildOverClause(partitionList, orderList, aggList, groupList);

        foreach (var w in windowList)
        {
            var winExpr = BuildWindowFunctionExpr(w, aggList, groupList);
            sb.Append(", ").Append(winExpr).Append(" OVER (").Append(overClause).Append(") AS ");
            sb.Append(QuoteIdent(string.IsNullOrEmpty(w.Alias) ? (w.Func ?? string.Empty).ToLowerInvariant() : w.Alias!));
        }

        sb.Append(" FROM (").Append(innerGroupBySql).Append(") g");
        return sb.ToString();
    }

    private static string BuildOverClause(
        IList<WindowFieldRequest> partitionList,
        IList<WindowOrderRequest> orderList,
        IList<AggregateRequest> aggList,
        IList<GroupFieldRequest> groupList)
    {
        var parts = new List<string>();

        if (partitionList.Count > 0)
        {
            var cols = partitionList.Select(p => "g." + QuoteIdent(ResolveColumnName(p.FieldPath, aggList, groupList)));
            parts.Add("PARTITION BY " + string.Join(", ", cols));
        }

        if (orderList.Count > 0)
        {
            var cols = orderList.Select(o =>
                "g." + QuoteIdent(ResolveColumnName(o.FieldPath, aggList, groupList))
                + (o.Descending ? " DESC" : " ASC"));
            parts.Add("ORDER BY " + string.Join(", ", cols));
        }

        return string.Join(" ", parts);
    }

    /// <summary>
    /// Maps a window OVER-clause field reference to the inner GroupBy
    /// column. Recognises the synthetic <c>Agg_&lt;func&gt;_&lt;field&gt;</c>
    /// pattern emitted by <see cref="GroupedWindowedQueryable{TKey,TProps}"/>
    /// and resolves it against <paramref name="aggList"/> to recover the
    /// real aggregate alias.
    /// </summary>
    private static string ResolveColumnName(
        string? fieldPath,
        IList<AggregateRequest> aggList,
        IList<GroupFieldRequest> groupList)
    {
        var path = fieldPath ?? string.Empty;

        if (path.StartsWith("Agg_", StringComparison.Ordinal))
        {
            var segments = path.Split('_', 3);
            if (segments.Length >= 2)
            {
                var funcName = segments[1];
                var aggField = segments.Length > 2 ? segments[2] : null;
                var match = aggList.FirstOrDefault(a =>
                    string.Equals(a.Function.ToString(), funcName, StringComparison.OrdinalIgnoreCase)
                    && (aggField is null || a.FieldPath == aggField));
                if (match is not null)
                    return match.Alias ?? match.Function.ToString();
            }
        }

        // Group field alias passthrough.
        var grp = groupList.FirstOrDefault(g =>
            g.Alias == path || g.FieldPath == path);
        if (grp is not null)
            return grp.Alias ?? grp.FieldPath;

        // Aggregation alias passthrough.
        var agg = aggList.FirstOrDefault(a => a.Alias == path);
        if (agg is not null)
            return agg.Alias!;

        return path;
    }

    private static string BuildWindowFunctionExpr(
        WindowFuncRequest w,
        IList<AggregateRequest> aggList,
        IList<GroupFieldRequest> groupList)
    {
        var func = (w.Func ?? string.Empty).ToUpperInvariant();
        return func switch
        {
            "ROWNUMBER" or "ROW_NUMBER" => "ROW_NUMBER()",
            "RANK" => "RANK()",
            "DENSERANK" or "DENSE_RANK" => "DENSE_RANK()",
            "PERCENTRANK" or "PERCENT_RANK" => "PERCENT_RANK()",
            "CUMEDIST" or "CUME_DIST" => "CUME_DIST()",
            "NTILE" => "NTILE(" + (w.Buckets ?? 4).ToString(CultureInfo.InvariantCulture) + ")",
            "COUNT" when string.IsNullOrEmpty(w.FieldPath) => "COUNT(*)",
            "SUM" or "AVG" or "MIN" or "MAX" or "COUNT"
                when !string.IsNullOrEmpty(w.FieldPath)
                => func + "(g." + QuoteIdent(ResolveColumnName(w.FieldPath, aggList, groupList)) + ")",
            "LAG" or "LEAD" or "FIRSTVALUE" or "FIRST_VALUE" or "LASTVALUE" or "LAST_VALUE"
                when !string.IsNullOrEmpty(w.FieldPath)
                => NormaliseValueFunc(func) + "(g." + QuoteIdent(ResolveColumnName(w.FieldPath, aggList, groupList)) + ")",
            _ => func + "()"
        };
    }

    private static string NormaliseValueFunc(string func) => func switch
    {
        "FIRSTVALUE" => "FIRST_VALUE",
        "LASTVALUE" => "LAST_VALUE",
        _ => func
    };

    private static string QuoteIdent(string identifier)
    {
        // Match SQLite's standard double-quote identifier escaping.
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }
}
