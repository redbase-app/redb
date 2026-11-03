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

namespace redb.MSSql.Query;

/// <summary>
/// PVT-backed GROUP BY for free MSSql. Generates the projection SQL via the
/// server-side compiler <c>dbo.pvt_build_groupby_sql</c> and wraps it with
/// <c>FOR JSON PATH</c> to materialize a JSON array of group rows in one
/// round-trip after the build step.
///
/// Mirrors <c>PostgresQueryProvider.Grouping.cs</c> — every grouping request
/// is compiled by the SQL UDF so non-.NET callers can submit the same
/// filter/group-by/aggregations JSON directly.
/// </summary>
public partial class MssqlQueryProvider
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
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var groupList = groupFields?.ToList() ?? new List<GroupFieldRequest>();
        var aggList = aggregations?.ToList() ?? new List<AggregateRequest>();

        // Empty group-by => degenerate to a flat batch aggregate.
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

        // Wrap the inner SELECT with FOR JSON PATH to materialize a JSON array
        // of group rows. INCLUDE_NULL_VALUES preserves null group keys.
        var wrapped =
            "SELECT (SELECT * FROM (" + innerSql + ") _grp_rows FOR JSON PATH, INCLUDE_NULL_VALUES)";

        var jsonArray = await _context.ExecuteScalarAsync<string>(wrapped, System.Array.Empty<object>(), cancellationToken);
        if (string.IsNullOrEmpty(jsonArray))
            return JsonDocument.Parse("[]");
        return JsonDocument.Parse(jsonArray);
    }

    /// <summary>
    /// Returns the inner SELECT compiled by <c>dbo.pvt_build_groupby_sql</c> for
    /// the requested grouping. Mirrors PostgreSQL's <c>GetGroupBySqlPreviewAsync</c>
    /// so <c>ToSqlStringAsync</c> on grouped queryables surfaces the actual SQL
    /// instead of a "preview not supported" placeholder.
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
    /// FilterExpression overload of
    /// <see cref="GetGroupBySqlPreviewAsync(long, IEnumerable{GroupFieldRequest}, IEnumerable{AggregateRequest}, string, string)"/>.
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
        // The HAVING used to be dropped here (NULL was passed), and GroupBy(...).Having(...) returned every group.
        object havingParam = string.IsNullOrEmpty(havingJson) ? DBNull.Value : (object)havingJson;

        // pvt_build_groupby_sql(@scheme_id, @filter, @group_by, @aggs, @having, @order, @limit, @offset, @source_mode)
        // $1 = filter, $2 = group_by, $3 = aggregations, $4 = having
        var invocation =
            "SELECT dbo.pvt_build_groupby_sql("
            + schemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, $2, $3, $4, NULL, NULL, 0, N'flat')";

        _logger?.LogDebug(
            "PVT GroupBy Build (MSSql): SchemeId={SchemeId}, GroupBy={GroupBy}, Aggs={Aggs}, Filter={Filter}, Having={Having}",
            schemeId, groupByJson, aggregationsJson ?? "null", filterJson ?? "null", havingJson ?? "null");

        var innerSql = await _context.ExecuteScalarAsync<string>(invocation, new object[] { filterParam, groupByJson, aggParam, havingParam }, cancellationToken);
        if (string.IsNullOrWhiteSpace(innerSql))
            throw new InvalidOperationException(
                "dbo.pvt_build_groupby_sql could not compile this grouping for scheme " + schemeId
                + (havingJson is null ? "." : " - the HAVING " + havingJson + " has a shape it does not support."));
        return innerSql!;
    }

    /// <summary>
    /// Serializes group fields to the JSON shape consumed by
    /// <c>dbo.pvt_build_groupby_sql</c>: <c>[{"field":"Department","alias":"Dept"}]</c>.
    /// Base fields get the <c>0$:</c> prefix so PVT resolves them to the
    /// underlying <c>_objects</c> column.
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
    /// Applies the <c>0$:</c> base-field prefix when <paramref name="isBaseField"/>
    /// is <see langword="true"/> and the path is not already prefixed.
    /// </summary>
    internal static string NormalizePvtFieldName(string? fieldPath, bool isBaseField)
    {
        var path = fieldPath ?? string.Empty;
        if (!isBaseField) return path;
        if (path.Length == 0
            || path.StartsWith("0$:", StringComparison.Ordinal)
            || path[0] == '_')
            return path;
        return "0$:" + path;
    }

    /// <summary>
    /// GroupByArray on the free SQL Server provider: <c>dbo.pvt_build_array_groupby_sql</c> of the module builds the
    /// grouping (and its HAVING); a window over the groups is written around it by <see cref="GroupedWindowSelect"/>.
    /// The module function cannot raise, so it answers what it refuses with NULL, and this refuses in turn.
    /// </summary>
    public override async Task<JsonDocument> ExecuteArrayGroupQueryAsync(ArrayGroupQuery query, CancellationToken cancellationToken = default)
    {
        var sql = await BuildArrayGroupSqlAsync(query, cancellationToken);
        var wrapped = "SELECT (SELECT * FROM (" + sql + ") _grp_rows FOR JSON PATH, INCLUDE_NULL_VALUES)";
        var json = await _context.ExecuteScalarAsync<string>(wrapped, System.Array.Empty<object>(), cancellationToken);
        // FOR JSON gives NULL for no rows.
        return JsonDocument.Parse(string.IsNullOrEmpty(json) ? "[]" : json);
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

        // dbo.pvt_build_array_groupby_sql(@scheme_id, @array_path, @filter, @group_by, @aggregations, @having, @source_mode)
        var invocation = "SELECT dbo.pvt_build_array_groupby_sql("
            + query.SchemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, $2, $3, $4, $5, N'flat')";
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
                "dbo.pvt_build_array_groupby_sql refused the grouping of '" + query.ArrayPath + "' for scheme " + query.SchemeId
                + ": a key, an aggregate, a condition or the outer filter names something the elements or the scheme do not have.");
        if (innerSql.StartsWith("--error:", StringComparison.Ordinal))
            throw new NotSupportedException("GroupByArray (MssqlQueryProvider): " + innerSql["--error:".Length..].Trim());

        return query.Window == null
            ? innerSql
            : GroupedWindowSelect.Build(innerSql, query.GroupFields, aggregations, shown,
                query.Window.Functions, query.Window.PartitionBy, query.Window.OrderBy, QuoteGwndIdent, rankingNeedsOrder: true);
    }
}
