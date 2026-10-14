using System;
using System.Collections.Generic;
using System.Linq;
using redb.Core.Query.Aggregation;

namespace redb.Core.Query.Grouping;

/// <summary>
/// How a grouped window (<c>GroupBy(...).WithWindow(...)</c>) names what it sorts and partitions by,
/// and what a provider has to make of those names. Shared by every provider: the names are written
/// by <see cref="GroupedWindowedQueryable{TKey,TProps}"/> and its tree twin, in three shapes:
///   "Agg_Sum_Salary"  - the aggregate Sum over Salary;
///   "Agg_Count_..."   - Count(*), whatever follows: Agg.Count(g) has no field to name;
///   a member name     - a part of a composite key, or "Key" for a key of a single field.
/// IRedbGrouping spells the average "Avg", the Agg class spells it "Average"; both arrive here.
/// </summary>
public static class GroupedWindowNames
{
    /// <summary>
    /// Aggregates the window refers to that the selector does not compute. Each comes back as a
    /// request under a private alias: the provider computes it inside the grouping and leaves it
    /// out of the result, so sorting by an aggregate does not oblige the caller to select it.
    /// </summary>
    public static List<AggregateRequest> HiddenAggregates(
        IEnumerable<string?> windowFieldPaths,
        IReadOnlyList<AggregateRequest> aggregations)
    {
        var hidden = new List<AggregateRequest>();

        foreach (var path in windowFieldPaths.Where(p => p != null && p.StartsWith("Agg_", StringComparison.Ordinal)).Distinct())
        {
            if (!TryParseAggregateName(path!, out var function, out var field))
                continue;

            var isCount = function == AggregateFunction.Count;
            if (!isCount && string.IsNullOrEmpty(field))
                continue;

            bool Same(AggregateRequest a) => Matches(a, function, field);
            if (aggregations.Any(Same) || hidden.Any(Same))
                continue;

            hidden.Add(new AggregateRequest
            {
                Function = function,
                FieldPath = isCount ? "" : field!,
                Alias = $"__window_{hidden.Count}"
            });
        }

        return hidden;
    }

    /// <summary>
    /// The column of the grouped result a window name refers to: the alias of the matching
    /// aggregate, or the alias of the group key. Null when the name matches nothing; the caller
    /// decides whether that is an error.
    /// </summary>
    public static string? TryResolveColumn(
        string? fieldPath,
        IReadOnlyList<AggregateRequest> aggregations,
        IReadOnlyList<GroupFieldRequest> groupFields)
    {
        var path = fieldPath ?? string.Empty;

        if (path.StartsWith("Agg_", StringComparison.Ordinal))
        {
            if (!TryParseAggregateName(path, out var function, out var field))
                return null;

            var aggregate = aggregations.FirstOrDefault(a => Matches(a, function, field));
            return aggregate == null ? null : aggregate.Alias ?? aggregate.Function.ToString();
        }

        if (path == "Key" && groupFields.Count == 1)
            return Alias(groupFields[0]);

        var group = groupFields.FirstOrDefault(g => g.Alias == path || g.FieldPath == path);
        if (group != null)
            return Alias(group);

        return aggregations.FirstOrDefault(a => a.Alias == path)?.Alias;
    }

    /// <summary>
    /// <see cref="TryResolveColumn"/> for a provider that refuses what matches nothing instead of
    /// sending the database a column that does not exist.
    /// </summary>
    public static string ResolveColumn(
        string? fieldPath,
        IReadOnlyList<AggregateRequest> aggregations,
        IReadOnlyList<GroupFieldRequest> groupFields)
        => TryResolveColumn(fieldPath, aggregations, groupFields)
           ?? throw new NotSupportedException(
               $"The window refers to '{fieldPath}', which is neither a key of the grouping nor an aggregate it computes.");

    private static string Alias(GroupFieldRequest group)
        => string.IsNullOrEmpty(group.Alias) ? group.FieldPath : group.Alias;

    private static bool Matches(AggregateRequest aggregate, AggregateFunction function, string? field)
        => aggregate.Function == function
           && (function == AggregateFunction.Count
               ? string.IsNullOrEmpty(aggregate.FieldPath) || aggregate.FieldPath == "*" || aggregate.FieldPath == field
               : aggregate.FieldPath == field);

    private static bool TryParseAggregateName(string path, out AggregateFunction function, out string? field)
    {
        var parts = path.Split('_', 3);
        var name = parts.Length > 1 ? parts[1] : "";
        field = parts.Length > 2 ? parts[2] : null;

        if (name.Equals("Avg", StringComparison.OrdinalIgnoreCase))
            name = "Average";

        return Enum.TryParse(name, ignoreCase: true, out function) && Enum.IsDefined(function);
    }
}
