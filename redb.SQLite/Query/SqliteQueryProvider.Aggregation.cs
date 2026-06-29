using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;

namespace redb.SQLite.Query;

/// <summary>
/// PVT-backed terminal aggregations for free SQLite. Generates the
/// aggregate SQL via the server-side compiler <c>pvt_build_aggregate_sql</c>
/// (free PVT v2 module) and executes the produced statement client-side,
/// wrapping the single result row with <c>row_to_json</c> to materialize
/// <see cref="AggregateResult"/> in a single round-trip after the build step.
///
/// Overrides the FilterExpression-typed virtual methods on the base provider.
/// Legacy facet-JSON overloads stay on the base type for dialects without
/// the PVT module (e.g. MSSql).
/// </summary>
public partial class SqliteQueryProvider
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
    /// Batch aggregate path: builds aggregations JSON, asks
    /// <c>pvt_build_aggregate_sql</c> to compile the inner SQL, then executes
    /// it through <c>row_to_json</c> to capture every alias in one row.
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
    /// virtual ensures custom-projection callers (RedbQueryable.AggregateAsync)
    /// hit the v2-pvt module instead of the deprecated <c>aggregate_batch</c>
    /// SQL function (now removed from PG dialect).
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

        object filterParam = string.IsNullOrEmpty(filterJson) || filterJson == "{}" || filterJson == "null"
            ? DBNull.Value
            : (object)filterJson!;

        // SQLite arg list mirrors PG positionally but without ::type casts (SQLite
        // chokes on "::") and with 1/1 for the include_seed/polymorphic booleans.
        var invocation = "SELECT pvt_build_aggregate_sql("
            + schemeId.ToString(CultureInfo.InvariantCulture)
            + ", $1, $2, 'flat', NULL, NULL, 1, 1) AS \"Value\"";

        _logger?.LogDebug("PVT Aggregate Build: SchemeId={SchemeId}, Aggs={Aggs}, Filter={Filter}",
            schemeId, aggregationsJson, filterJson ?? "null");

        var innerSql = await _context.ExecuteScalarAsync<string>(invocation, filterParam, aggregationsJson);
        if (string.IsNullOrWhiteSpace(innerSql))
            throw new InvalidOperationException(
                "pvt_build_aggregate_sql returned an empty SQL string for scheme " + schemeId + ".");

        // SQLite has no row_to_json: execute the (fully-literal) inner SQL and
        // serialize its single result row to a JSON object in C#.
        var jsonRow = await SqliteConn.QueryFirstRowAsJsonAsync(innerSql!);

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
    /// <c>pvt_build_aggregate_sql</c>:
    /// <c>[{"alias":"x","$sum":{"$field":"Price"}}, {"alias":"y","$count":"*"}]</c>.
    /// Count with empty FieldPath becomes <c>$count: "*"</c>.
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
                    "Aggregation function " + r.Function + " is not supported by pvt_build_aggregate_sql.")
            };

            var entry = new Dictionary<string, object> { ["alias"] = alias };
            // RedbQueryable / RedbGroupedQueryable encode COUNT(*) as FieldPath "*";
            // PVT expects the literal star as the operand string, not as a field reference.
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
