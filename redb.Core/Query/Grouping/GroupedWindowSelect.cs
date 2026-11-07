using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;

namespace redb.Core.Query.Grouping;

/// <summary>
/// The window over the rows of a grouping, as the free providers write it around the grouping SQL their module
/// builds: <c>SELECT g.&lt;columns&gt;, &lt;func&gt; OVER (...) AS &lt;alias&gt; FROM (&lt;grouping&gt;) g</c>. One builder
/// for GroupBy and GroupByArray on every free provider; a reference that names no key and no aggregate is refused.
/// </summary>
public static class GroupedWindowSelect
{
    /// <summary>
    /// The aggregates the grouping must compute: those of the selector, then those the window partitions, orders or
    /// reads by without the selector asking for them, under private aliases.
    /// </summary>
    public static List<AggregateRequest> WithHiddenAggregates(
        IReadOnlyList<AggregateRequest> shown,
        IEnumerable<WindowFuncRequest> functions,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy)
    {
        var all = shown.ToList();
        all.AddRange(GroupedWindowNames.HiddenAggregates(
            partitionBy.Select(p => p.FieldPath)
                .Concat(orderBy.Select(o => o.FieldPath))
                .Concat(functions.Select(f => f.FieldPath)),
            shown));
        return all;
    }

    /// <param name="innerSql">The grouping, computing every aggregate of <paramref name="aggregations"/>.</param>
    /// <param name="groupFields">Keys of the grouping.</param>
    /// <param name="aggregations">Every aggregate the grouping computes, hidden ones included.</param>
    /// <param name="shownAggregates">The aggregates of the selector; hidden ones are left out of the result.</param>
    /// <param name="functions">Window functions with their aliases.</param>
    /// <param name="partitionBy">PARTITION BY references.</param>
    /// <param name="orderBy">ORDER BY references.</param>
    /// <param name="quote">Identifier quoting of the dialect.</param>
    /// <param name="rankingNeedsOrder">True for a dialect whose window functions require ORDER BY (SQL Server): without
    /// an order of its own the window is ordered by the first key.</param>
    public static string Build(
        string innerSql,
        IReadOnlyList<GroupFieldRequest> groupFields,
        IReadOnlyList<AggregateRequest> aggregations,
        IReadOnlyList<AggregateRequest> shownAggregates,
        IReadOnlyList<WindowFuncRequest> functions,
        IReadOnlyList<WindowFieldRequest> partitionBy,
        IReadOnlyList<WindowOrderRequest> orderBy,
        Func<string, string> quote,
        bool rankingNeedsOrder)
    {
        if (functions.Count == 0)
            throw new ArgumentException("A window over groups needs at least one window function.", nameof(functions));

        string Column(string? reference) => "g." + quote(GroupedWindowNames.ResolveColumn(reference, aggregations, groupFields));

        var over = new List<string>();
        if (partitionBy.Count > 0)
            over.Add("PARTITION BY " + string.Join(", ", partitionBy.Select(p => Column(p.FieldPath))));
        if (orderBy.Count > 0)
            over.Add("ORDER BY " + string.Join(", ", orderBy.Select(o => Column(o.FieldPath) + (o.Descending ? " DESC" : " ASC"))));
        else if (rankingNeedsOrder)
            over.Add("ORDER BY g." + quote(groupFields[0].Alias) + " ASC");
        var overClause = string.Join(" ", over);

        var columns = groupFields.Select(g => g.Alias)
            .Concat(shownAggregates.Select(a => a.Alias ?? a.Function.ToString()))
            .Select(c => "g." + quote(c));

        var sb = new StringBuilder("SELECT ");
        sb.Append(string.Join(", ", columns));
        foreach (var f in functions)
        {
            var alias = string.IsNullOrEmpty(f.Alias) ? (f.Func ?? string.Empty).ToLowerInvariant() : f.Alias;
            sb.Append(", ").Append(FunctionCall(f, Column)).Append(" OVER (").Append(overClause).Append(") AS ").Append(quote(alias));
        }
        sb.Append(" FROM (").Append(innerSql).Append(") g");
        return sb.ToString();
    }

    private static string FunctionCall(WindowFuncRequest f, Func<string?, string> column)
    {
        var hasArg = !string.IsNullOrEmpty(f.FieldPath);
        return (f.Func ?? string.Empty).ToUpperInvariant() switch
        {
            "ROWNUMBER" or "ROW_NUMBER" => "ROW_NUMBER()",
            "RANK" => "RANK()",
            "DENSERANK" or "DENSE_RANK" => "DENSE_RANK()",
            "PERCENTRANK" or "PERCENT_RANK" => "PERCENT_RANK()",
            "CUMEDIST" or "CUME_DIST" => "CUME_DIST()",
            "NTILE" => "NTILE(" + (f.Buckets ?? 4).ToString(CultureInfo.InvariantCulture) + ")",
            "SUM" when hasArg => "SUM(" + column(f.FieldPath) + ")",
            "AVG" when hasArg => "AVG(" + column(f.FieldPath) + ")",
            "MIN" when hasArg => "MIN(" + column(f.FieldPath) + ")",
            "MAX" when hasArg => "MAX(" + column(f.FieldPath) + ")",
            "COUNT" when hasArg => "COUNT(" + column(f.FieldPath) + ")",
            "COUNT" => "COUNT(*)",
            "LAG" when hasArg => "LAG(" + column(f.FieldPath) + ")",
            "LEAD" when hasArg => "LEAD(" + column(f.FieldPath) + ")",
            "FIRSTVALUE" or "FIRST_VALUE" when hasArg => "FIRST_VALUE(" + column(f.FieldPath) + ")",
            "LASTVALUE" or "LAST_VALUE" when hasArg => "LAST_VALUE(" + column(f.FieldPath) + ")",
            _ => WindowFunctionName.ZeroArgCall(f.Func)
        };
    }
}
