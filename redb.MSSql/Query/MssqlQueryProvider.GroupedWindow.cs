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

namespace redb.MSSql.Query;

/// <summary>
/// PVT-backed GroupBy + Window Functions for free MSSql. Mirrors
/// <c>PostgresQueryProvider.GroupedWindow.cs</c> but uses T-SQL identifier
/// quoting and <c>FOR JSON PATH, INCLUDE_NULL_VALUES</c> for JSON
/// materialization.
///
/// Architecture: calls <c>BuildGroupByInnerSqlAsync</c> (from
/// <c>MssqlQueryProvider.Grouping.cs</c>, same partial class) to generate
/// the inner GROUP BY query, then wraps it with an outer SELECT that adds
/// window-function columns via <c>OVER (PARTITION BY ... ORDER BY ...)</c>.
/// </summary>
public partial class MssqlQueryProvider
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
        string? filterJson, string? havingJson, CancellationToken cancellationToken = default)
    {
        var outerSql = await BuildGroupedWindowSqlAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson, havingJson);

        var jsonArray = await _context.ExecuteScalarAsync<string>(outerSql, System.Array.Empty<object>(), cancellationToken);
        if (string.IsNullOrEmpty(jsonArray))
            return JsonDocument.Parse("[]");
        return JsonDocument.Parse(jsonArray);
    }

    private async Task<string> BuildGroupedWindowSqlAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson, string? havingJson)
    {
        var groupList  = groupFields?.ToList()  ?? new List<GroupFieldRequest>();
        var aggList    = aggregations?.ToList() ?? new List<AggregateRequest>();
        var windowList = windowFuncs?.ToList()  ?? new List<WindowFuncRequest>();
        var partitionList = partitionBy?.ToList() ?? new List<WindowFieldRequest>();
        var orderList     = orderBy?.ToList()     ?? new List<WindowOrderRequest>();

        if (groupList.Count == 0)
            throw new ArgumentException(
                "groupFields must contain at least one entry.", nameof(groupFields));
        if (windowList.Count == 0)
            throw new ArgumentException(
                "windowFuncs must contain at least one entry.", nameof(windowFuncs));

        // An aggregate the window sorts, partitions or reads by need not be in the selector: computed under a private alias.
        var shownAggregates = aggList.ToList();
        aggList = GroupedWindowSelect.WithHiddenAggregates(shownAggregates, windowList, partitionList, orderList);

        var innerSql = await BuildGroupByInnerSqlAsync(
            schemeId, groupList, aggList, filterJson, havingJson);

        var windowedSelect = GroupedWindowSelect.Build(
            innerSql, groupList, aggList, shownAggregates, windowList, partitionList, orderList, QuoteGwndIdent, rankingNeedsOrder: true);

        return "SELECT (SELECT * FROM ("
            + windowedSelect
            + ") _gwnd_rows FOR JSON PATH, INCLUDE_NULL_VALUES)";
    }

    // ----------------------------------------------------------------
    // Windowed SELECT builder
    // ----------------------------------------------------------------






    /// <summary>
    /// T-SQL square-bracket identifier quoting (mirrors PG's double-quote quoting).
    /// </summary>
    private static string QuoteGwndIdent(string identifier)
        => "[" + identifier.Replace("]", "]]") + "]";
}
