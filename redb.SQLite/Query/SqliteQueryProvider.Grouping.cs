using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Grouping;
using redb.Core.Query.QueryExpressions;

namespace redb.SQLite.Query;

/// <summary>
/// PVT-backed GROUP BY for free SQLite. Generates the projection SQL
/// via the server-side compiler <c>pvt_build_groupby_sql</c> and wraps it
/// with <c>json_agg(row_to_json(t))</c> to materialize a JSON array of
/// group rows in one round-trip after the build step.
///
/// Overrides both string-based and FilterExpression-based virtual methods
/// on <see cref="redb.Core.Query.Base.QueryProviderBase"/> so callers reach
/// the PVT compiler regardless of which surface they use.
/// </summary>
public partial class SqliteQueryProvider
{
    /// <inheritdoc />
    public override Task<JsonDocument?> ExecuteGroupedAggregateAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson = null,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        return ExecuteGroupedAggregateInternalAsync(schemeId, groupFields, aggregations, filterJson, havingJson);
    }

    /// <inheritdoc />
    public override Task<JsonDocument?> ExecuteGroupedAggregateAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        FilterExpression? filter,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var filterJson = filter is null ? null : _facetBuilder.BuildFacetFilters(filter);
        return ExecuteGroupedAggregateInternalAsync(schemeId, groupFields, aggregations, filterJson, havingJson);
    }

    private async Task<JsonDocument?> ExecuteGroupedAggregateInternalAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson,
        string? havingJson = null)
    {
        var groupList = groupFields?.ToList() ?? new List<GroupFieldRequest>();
        var aggList = aggregations?.ToList() ?? new List<AggregateRequest>();

        // Empty group-by => degenerate to a flat batch aggregate; *RedbAsync helpers
        // (SumRedbAsync, AverageRedbAsync, ...) intentionally pass zero group keys to
        // request a single-row aggregate over base IRedbObject columns.
        if (groupList.Count == 0)
        {
            if (!string.IsNullOrEmpty(havingJson))
                throw new NotSupportedException(
                    "HAVING requires GROUP BY keys; cannot be combined with a flat aggregate batch.");
            if (aggList.Count == 0)
                return null;
            var aggResult = await ExecuteAggregateBatchInternalAsync(schemeId, aggList, filterJson);
            var rowJson = JsonSerializer.Serialize(new[] { aggResult.Values });
            return JsonDocument.Parse(rowJson);
        }

        var innerSql = await BuildGroupByInnerSqlAsync(schemeId, groupList, aggList, filterJson, havingJson);

        // SQLite has no json_agg/row_to_json: run the inner SQL and serialize rows in C#.
        var jsonArray = await _context.QueryRowsAsJsonAsync(innerSql, System.Array.Empty<object>());
        if (string.IsNullOrEmpty(jsonArray))
            return null;
        return JsonDocument.Parse(jsonArray);
    }

    /// <summary>
    /// Returns the inner SELECT compiled by <c>pvt_build_groupby_sql</c> for the
    /// requested grouping. This is the same first-pass SQL that
    /// <see cref="ExecuteGroupedAggregateInternalAsync"/> later wraps with
    /// <c>json_agg(row_to_json(t))</c>; exposed so <c>ToSqlStringAsync</c> and
    /// other diagnostics surfaces can see the actual SQL.
    /// </summary>
    public override async Task<string> GetGroupBySqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var groupList = groupFields?.ToList() ?? new List<GroupFieldRequest>();
        var aggList = aggregations?.ToList() ?? new List<AggregateRequest>();
        if (groupList.Count == 0)
            throw new ArgumentException("A GroupBy SQL preview needs at least one key.", nameof(groupFields));
        return await BuildGroupByInnerSqlAsync(schemeId, groupList, aggList, filterJson, havingJson);
    }

    /// <summary>
    /// FilterExpression overload of <see cref="GetGroupBySqlPreviewAsync(long, IEnumerable&lt;GroupFieldRequest&gt;, IEnumerable&lt;AggregateRequest&gt;, string, string)"/>.
    /// </summary>
    public override Task<string> GetGroupBySqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        FilterExpression? filter,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var filterJson = filter is null ? null : _facetBuilder.BuildFacetFilters(filter);
        return GetGroupBySqlPreviewAsync(schemeId, groupFields, aggregations, filterJson, havingJson);
    }

    private async Task<string> BuildGroupByInnerSqlAsync(
        long schemeId,
        IList<GroupFieldRequest> groupList,
        IList<AggregateRequest> aggList,
        string? filterJson,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var groupByJson = BuildPvtGroupByJson(groupList);
        var aggregationsJson = aggList.Count == 0 ? null : BuildPvtAggregationsJson(aggList);

        object filterParam = string.IsNullOrEmpty(filterJson) || filterJson == "{}" || filterJson == "null"
            ? DBNull.Value
            : (object)filterJson!;
        object aggParam = aggregationsJson is null ? DBNull.Value : (object)aggregationsJson;
        object havingParam = string.IsNullOrEmpty(havingJson) || havingJson == "null"
            ? DBNull.Value
            : (object)havingJson!;

        var invocation = "SELECT pvt_build_groupby_sql("
            + schemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, $2, $3, $4, NULL, NULL, 0, 'flat', NULL, NULL, 1, 1) AS \"Value\"";

        _logger?.LogDebug("PVT GroupBy Build: SchemeId={SchemeId}, GroupBy={GroupBy}, Aggs={Aggs}, Filter={Filter}, Having={Having}",
            schemeId, groupByJson, aggregationsJson ?? "null", filterJson ?? "null", havingJson ?? "null");

        var innerSql = await _context.ExecuteScalarAsync<string>(invocation, new object[] { filterParam, groupByJson, aggParam, havingParam }, cancellationToken);
        if (string.IsNullOrWhiteSpace(innerSql))
            throw new InvalidOperationException(
                "pvt_build_groupby_sql returned an empty SQL string for scheme " + schemeId + ".");
        return innerSql!;
    }

    /// <summary>
    /// Serializes group fields to the JSON shape consumed by
    /// <c>pvt_build_groupby_sql</c>: <c>[{"field":"Department","alias":"d"}]</c>.
    /// Base fields (<see cref="GroupFieldRequest.IsBaseField"/>) get the <c>0$:</c>
    /// prefix so PVT's <c>pvt_normalize_base_field_name</c> resolves them to the
    /// underlying <c>_objects</c> column. Underscore-prefixed names and inputs
    /// that already carry <c>0$:</c> are passed through.
    /// </summary>
    private static string BuildPvtGroupByJson(IList<GroupFieldRequest> groupFields)
    {
        var entries = new List<Dictionary<string, object>>(groupFields.Count);
        foreach (var g in groupFields)
        {
            var entry = new Dictionary<string, object>
            {
                ["field"] = NormalizePvtFieldName(g.FieldPath, g.IsBaseField)
            };
            if (!string.IsNullOrEmpty(g.Alias))
                entry["alias"] = g.Alias!;
            entries.Add(entry);
        }
        return JsonSerializer.Serialize(entries);
    }

    /// <summary>
    /// Returns <paramref name="fieldPath"/> with the <c>0$:</c> base-field prefix
    /// applied when <paramref name="isBaseField"/> is true and the path is not
    /// already prefixed / underscore-prefixed. Used by GROUP BY, ORDER BY and
    /// window partition / order entries that flow through PVT.
    /// </summary>
    internal static string NormalizePvtFieldName(string? fieldPath, bool isBaseField)
    {
        var path = fieldPath ?? string.Empty;
        if (!isBaseField) return path;
        if (path.Length == 0 || path.StartsWith("0$:", StringComparison.Ordinal) || path[0] == '_')
            return path;
        return "0$:" + path;
    }

    /// <summary>
    /// GroupByArray on the free SQLite provider: <c>pvt_build_array_groupby_sql</c> of the extension builds the
    /// grouping (and its HAVING); a window over the groups is written around it by <see cref="GroupedWindowSelect"/>.
    /// SQLite has no json_agg, so the rows are packed into JSON in C#, NULL values included.
    /// </summary>
    public override async Task<JsonDocument> ExecuteArrayGroupQueryAsync(ArrayGroupQuery query, CancellationToken cancellationToken = default)
    {
        var sql = await BuildArrayGroupSqlAsync(query, cancellationToken);
        var json = await _context.QueryRowsAsJsonAsync(sql, System.Array.Empty<object>(), cancellationToken);
        if (string.IsNullOrEmpty(json))
            throw new InvalidOperationException("GroupByArray: the grouping returned no rows document for scheme " + query.SchemeId + ".");
        return JsonDocument.Parse(json);
    }

    /// <inheritdoc />
    public override Task<string> GetArrayGroupQuerySqlAsync(ArrayGroupQuery query, CancellationToken cancellationToken = default)
        => BuildArrayGroupSqlAsync(query, cancellationToken);

    private async Task<string> BuildArrayGroupSqlAsync(ArrayGroupQuery query, CancellationToken cancellationToken)
    {
        query.EnsureValid();
        var filterJson = query.Filter != null ? _facetBuilder.BuildFacetFilters(query.Filter) : query.FilterJson;
        var shown = query.Aggregations.ToList();
        var aggregations = query.Window == null
            ? shown
            : GroupedWindowSelect.WithHiddenAggregates(shown, query.Window.Functions, query.Window.PartitionBy, query.Window.OrderBy);

        var invocation = "SELECT pvt_build_array_groupby_sql("
            + query.SchemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, $2, $3, $4, $5, NULL, NULL, 0) AS \"Value\"";
        var innerSql = await _context.ExecuteScalarAsync<string>(invocation, new object[]
        {
            query.ArrayPath,
            ArrayGroupModuleJson.Param(filterJson),
            ArrayGroupModuleJson.GroupBy(query.GroupFields),
            ArrayGroupModuleJson.Param(ArrayGroupModuleJson.Aggregations(aggregations)),
            ArrayGroupModuleJson.Param(query.HavingJson)
        }, cancellationToken);
        if (string.IsNullOrWhiteSpace(innerSql))
            throw new InvalidOperationException(
                "pvt_build_array_groupby_sql returned an empty SQL string for scheme " + query.SchemeId + ".");

        return query.Window == null
            ? innerSql
            : GroupedWindowSelect.Build(innerSql, query.GroupFields, aggregations, shown,
                query.Window.Functions, query.Window.PartitionBy, query.Window.OrderBy, QuoteIdent, rankingNeedsOrder: false);
    }
}
