using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using redb.Core.Query.Aggregation;

namespace redb.Core.Query.Grouping;

/// <summary>
/// The JSON arguments of the free providers' <c>pvt_build_array_groupby_sql</c> (PostgreSQL and SQL Server modules,
/// SQLite extension): keys <c>[{field, alias}]</c> and aggregates <c>[{field, func, alias, filter?}]</c>, where
/// <c>field</c> may be <see cref="GroupFieldRequest.ElementPath"/> and <c>filter</c> is the condition of a conditional
/// count in the HAVING language. One writer for the three providers.
/// </summary>
public static class ArrayGroupModuleJson
{
    public static string GroupBy(IEnumerable<GroupFieldRequest> groupFields)
        => JsonSerializer.Serialize(groupFields.Select(g => new Dictionary<string, object?>
        {
            ["field"] = g.FieldPath,
            ["alias"] = g.Alias
        }));

    public static string? Aggregations(IReadOnlyList<AggregateRequest> aggregations)
    {
        if (aggregations.Count == 0) return null;
        return JsonSerializer.Serialize(aggregations.Select(a =>
        {
            var entry = new Dictionary<string, object?>
            {
                ["field"] = string.IsNullOrEmpty(a.FieldPath) ? "*" : a.FieldPath,
                ["func"] = a.Function switch
                {
                    AggregateFunction.Average => "AVG",
                    AggregateFunction.Sum => "SUM",
                    AggregateFunction.Min => "MIN",
                    AggregateFunction.Max => "MAX",
                    AggregateFunction.Count => "COUNT",
                    _ => throw new NotSupportedException($"Aggregation function {a.Function} is not supported for GroupByArray.")
                },
                ["alias"] = a.Alias ?? a.Function.ToString()
            };
            if (a.ConditionJson != null)
                entry["filter"] = JsonDocument.Parse(a.ConditionJson).RootElement.Clone();
            return entry;
        }));
    }

    /// <summary>A JSON argument or DBNull for an empty one.</summary>
    public static object Param(string? json)
        => string.IsNullOrEmpty(json) || json == "{}" || json == "null" ? DBNull.Value : json;
}
