using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;

namespace redb.MSSql.Query;

/// <summary>
/// PVT-backed terminal aggregations for free MSSql. Generates the
/// aggregate SQL via the server-side compiler <c>dbo.pvt_build_aggregate_sql</c>
/// (free PVT v2 module) and executes the produced statement client-side,
/// wrapping the single result row with <c>FOR JSON PATH, WITHOUT_ARRAY_WRAPPER</c>
/// to materialize <see cref="AggregateResult"/> in one round-trip after the
/// build step.
///
/// Mirrors <c>PostgresQueryProvider.Aggregation.cs</c> — the goal is to keep
/// every aggregation request expressible as facet/aggregations JSON that
/// non-.NET clients can submit directly (e.g. a Python service composing a
/// facet AST and dispatching it through the SQL dialect's UDF).
///
/// Overrides the FilterExpression-typed virtual methods on the base provider.
/// Legacy facet-JSON overloads are also overridden because the MSSql
/// dialect's <c>Query_AggregateField/Batch</c> functions throw — we route
/// every aggregate path through the v2-pvt compiler instead.
/// </summary>
public partial class MssqlQueryProvider
{
    /// <summary>
    /// Single-aggregate path. Delegates to <see cref="ExecuteAggregateBatchAsync(long, IEnumerable{AggregateRequest}, FilterExpression?)"/>
    /// with a one-element request list and extracts the scalar value.
    /// </summary>
    public override async Task<decimal?> ExecuteAggregateAsync(
        long schemeId,
        string fieldPath,
        AggregateFunction function,
        FilterExpression? filter)
    {
        var request = new AggregateRequest
        {
            FieldPath = fieldPath ?? string.Empty,
            Function = function,
            Alias = "value"
        };

        var batch = await ExecuteAggregateBatchAsync(schemeId, new[] { request }, filter);
        if (!batch.Values.TryGetValue("value", out var raw) || raw is null)
            return null;
        return raw switch
        {
            decimal d => d,
            double dbl => (decimal)dbl,
            long l => l,
            int i => i,
            _ => Convert.ToDecimal(raw, CultureInfo.InvariantCulture)
        };
    }

    /// <summary>
    /// Single-aggregate path for facet-JSON callers (legacy
    /// <see cref="ExecuteAggregateAsync(long, string, AggregateFunction, string?)"/>
    /// overload). Bypasses the throwing dialect SQL and routes through the
    /// v2-pvt aggregate compiler.
    /// </summary>
    public override async Task<decimal?> ExecuteAggregateAsync(
        long schemeId,
        string fieldPath,
        AggregateFunction function,
        string? filterJson = null)
    {
        var request = new AggregateRequest
        {
            FieldPath = fieldPath ?? string.Empty,
            Function = function,
            Alias = "value"
        };

        var batch = await ExecuteAggregateBatchInternalAsync(schemeId, new[] { request }, filterJson);
        if (!batch.Values.TryGetValue("value", out var raw) || raw is null)
            return null;
        return raw switch
        {
            decimal d => d,
            double dbl => (decimal)dbl,
            long l => l,
            int i => i,
            _ => Convert.ToDecimal(raw, CultureInfo.InvariantCulture)
        };
    }

    /// <summary>
    /// Batch aggregate path: builds aggregations JSON, asks
    /// <c>dbo.pvt_build_aggregate_sql</c> to compile the inner SQL, then executes
    /// it through <c>FOR JSON PATH, WITHOUT_ARRAY_WRAPPER</c> to capture every
    /// alias in one row.
    /// </summary>
    public override Task<AggregateResult> ExecuteAggregateBatchAsync(
        long schemeId,
        IEnumerable<AggregateRequest> requests,
        FilterExpression? filter)
    {
        var facetFilters = filter is null ? null : _facetBuilder.BuildFacetFilters(filter);
        return ExecuteAggregateBatchInternalAsync(schemeId, requests, facetFilters);
    }

    /// <summary>
    /// Batch aggregate path with pre-built facet-JSON filter. Same PVT pipeline
    /// as the <see cref="FilterExpression"/> overload — overriding the base
    /// virtual ensures custom-projection callers
    /// (<see cref="redb.Core.Query.RedbQueryable{TProps}.AggregateAsync{TResult}"/>)
    /// hit the v2-pvt module instead of the throwing legacy SQL function on the
    /// MSSql dialect.
    /// </summary>
    public override Task<AggregateResult> ExecuteAggregateBatchAsync(
        long schemeId,
        IEnumerable<AggregateRequest> requests,
        string? filterJson = null)
    {
        return ExecuteAggregateBatchInternalAsync(schemeId, requests, filterJson);
    }

    internal async Task<AggregateResult> ExecuteAggregateBatchInternalAsync(
        long schemeId,
        IEnumerable<AggregateRequest> requests,
        string? filterJson)
    {
        var list = requests?.ToList()
            ?? throw new ArgumentNullException(nameof(requests));
        if (list.Count == 0)
            return new AggregateResult();

        var aggregationsJson = BuildPvtAggregationsJson(list);

        // pvt_build_aggregate_sql expects NULL for the empty / missing filter
        // (a literal "{}" or "null" string would defeat the IS NULL branch in
        // pvt_collect_fields and produce a missing-meta marker downstream).
        object filterParam = string.IsNullOrEmpty(filterJson) || filterJson == "{}" || filterJson == "null"
            ? DBNull.Value
            : (object)filterJson!;

        // Step 1: ask the server-side compiler for the inner aggregate SELECT.
        // $1 → @p0 = filter, $2 → @p1 = aggregations (mirrors PG ordering).
        var invocation =
            "SELECT dbo.pvt_build_aggregate_sql(" +
            schemeId.ToString(CultureInfo.InvariantCulture) +
            ", $1, $2, N'flat')";

        _logger?.LogDebug("PVT Aggregate Build (MSSql): SchemeId={SchemeId}, Aggs={Aggs}, Filter={Filter}",
            schemeId, aggregationsJson, filterJson ?? "null");

        var innerSql = await _context.ExecuteScalarAsync<string>(invocation, filterParam, aggregationsJson);
        if (string.IsNullOrWhiteSpace(innerSql))
            throw new InvalidOperationException(
                "dbo.pvt_build_aggregate_sql returned an empty SQL string for scheme " + schemeId + ".");

        // Step 2: wrap with FOR JSON to capture every alias in one row.
        // INCLUDE_NULL_VALUES preserves nulls so MIN/MAX over empty sets
        // round-trip as JSON null instead of being silently dropped.
        var wrapped =
            "SELECT (SELECT * FROM (" + innerSql + ") _agg_row FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES)";

        var jsonRow = await _context.ExecuteScalarAsync<string>(wrapped);

        var result = new AggregateResult();
        if (!string.IsNullOrEmpty(jsonRow))
        {
            using var doc = JsonDocument.Parse(jsonRow);
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                result.Values[prop.Name] = ConvertJsonValue(prop.Value);
            }
        }
        return result;
    }

    /// <summary>
    /// Serializes an aggregate request list to the JSON shape consumed by
    /// <c>dbo.pvt_build_aggregate_sql</c>:
    /// <c>[{"alias":"x","$sum":{"$field":"Price"}}, {"alias":"y","$count":"*"}]</c>.
    /// Count with empty <see cref="AggregateRequest.FieldPath"/> becomes
    /// <c>$count: "*"</c>.
    /// </summary>
    private static string BuildPvtAggregationsJson(IList<AggregateRequest> requests)
    {
        var auto = 0;
        var entries = new List<Dictionary<string, object>>(requests.Count);
        foreach (var r in requests)
        {
            auto++;
            var alias = string.IsNullOrEmpty(r.Alias) ? "_agg_" + auto : r.Alias!;
            var opKey = r.Function switch
            {
                AggregateFunction.Sum => "$sum",
                AggregateFunction.Average => "$avg",
                AggregateFunction.Min => "$min",
                AggregateFunction.Max => "$max",
                AggregateFunction.Count => "$count",
                _ => throw new NotSupportedException(
                    "Aggregation function " + r.Function + " is not supported by dbo.pvt_build_aggregate_sql.")
            };

            var entry = new Dictionary<string, object> { ["alias"] = alias };
            var isCountStar = r.Function == AggregateFunction.Count
                && (string.IsNullOrEmpty(r.FieldPath) || r.FieldPath == "*");
            if (isCountStar)
            {
                entry[opKey] = "*";
            }
            else
            {
                entry[opKey] = new Dictionary<string, object>
                {
                    ["$field"] = r.FieldPath ?? string.Empty
                };
            }
            entries.Add(entry);
        }
        return JsonSerializer.Serialize(entries);
    }

    private static object? ConvertJsonValue(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.Number => el.TryGetDecimal(out var d) ? d : (object)el.GetDouble(),
        JsonValueKind.String => el.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        _ => el.GetRawText()
    };
}
