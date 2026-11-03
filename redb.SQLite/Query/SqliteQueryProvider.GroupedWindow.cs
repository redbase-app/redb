using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
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
        string? filterJson = null,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        return ExecuteGroupedWindowInternalAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson, havingJson);
    }

    /// <inheritdoc />
    public override Task<JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var filterJson = filter is null ? null : _facetBuilder.BuildFacetFilters(filter);
        return ExecuteGroupedWindowInternalAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson, havingJson);
    }

    /// <inheritdoc />
    public override async Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        return await BuildGroupedWindowSqlAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson, havingJson);
    }

    /// <inheritdoc />
    public override async Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var filterJson = filter is null ? null : _facetBuilder.BuildFacetFilters(filter);
        return await BuildGroupedWindowSqlAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson, havingJson);
    }

    private async Task<JsonDocument?> ExecuteGroupedWindowInternalAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson, string? havingJson)
    {
        var sql = await BuildGroupedWindowSqlAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson, havingJson);

        // SQLite has no json_agg/row_to_json: run the windowed SELECT and serialize rows in C#.
        var jsonArray = await _context.QueryRowsAsJsonAsync(sql, System.Array.Empty<object>());
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
        string? filterJson, string? havingJson, CancellationToken cancellationToken = default)
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

        // An aggregate the window sorts, partitions or reads by need not be in the selector: computed under a private alias.
        var shownAggregates = aggList.ToList();
        aggList = GroupedWindowSelect.WithHiddenAggregates(shownAggregates, windowList, partitionList, orderList);

        var groupByJson = BuildPvtGroupByJson(groupList);
        var aggregationsJson = aggList.Count == 0 ? null : BuildPvtAggregationsJson(aggList);

        object filterParam = string.IsNullOrEmpty(filterJson) || filterJson == "{}" || filterJson == "null"
            ? DBNull.Value
            : (object)filterJson!;
        object aggParam = aggregationsJson is null ? DBNull.Value : (object)aggregationsJson;
        object havingParam = string.IsNullOrEmpty(havingJson) || havingJson == "{}" || havingJson == "null"
            ? DBNull.Value
            : (object)havingJson!;

        var invocation = "SELECT pvt_build_groupby_sql("
            + schemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, $2, $3, $4, NULL, NULL, 0, 'flat', NULL, NULL, 1, 1) AS \"Value\"";

        _logger?.LogDebug("PVT GroupedWindow Build: SchemeId={SchemeId}, GroupBy={GroupBy}, Aggs={Aggs}, Filter={Filter}",
            schemeId, groupByJson, aggregationsJson ?? "null", filterJson ?? "null");

        var innerSql = await _context.ExecuteScalarAsync<string>(invocation, new object[] { filterParam, groupByJson, aggParam, havingParam }, cancellationToken);
        if (string.IsNullOrWhiteSpace(innerSql))
            throw new InvalidOperationException(
                "pvt_build_groupby_sql returned an empty SQL string for scheme " + schemeId + ".");

        // Returns the windowed SELECT itself; the caller serializes rows to JSON in C#
        // (SQLite has no json_agg/row_to_json). Preview surfaces see the real SQL.
        return GroupedWindowSelect.Build(
            innerSql, groupList, aggList, shownAggregates, windowList, partitionList, orderList, QuoteIdent, rankingNeedsOrder: false);
    }






    private static string QuoteIdent(string identifier)
    {
        // Match SQLite's standard double-quote identifier escaping.
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }
}
