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
        var outerSql = await BuildGroupedWindowSqlAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);

        var jsonArray = await _context.ExecuteScalarAsync<string>(outerSql);
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
        string? filterJson)
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

        var innerSql = await BuildGroupByInnerSqlAsync(
            schemeId, groupList, aggList, filterJson);

        var windowedSelect = BuildWindowedSelect(
            innerSql, groupList, aggList, windowList, partitionList, orderList);

        return "SELECT (SELECT * FROM ("
            + windowedSelect
            + ") _gwnd_rows FOR JSON PATH, INCLUDE_NULL_VALUES)";
    }

    // ----------------------------------------------------------------
    // Windowed SELECT builder
    // ----------------------------------------------------------------

    /// <summary>
    /// Builds:
    ///   <c>SELECT g.*, &lt;winExpr&gt; OVER (...) AS [alias] FROM (&lt;innerGroupBy&gt;) g</c>
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
            var alias = QuoteGwndIdent(
                string.IsNullOrEmpty(w.Alias)
                    ? (w.Func ?? string.Empty).ToLowerInvariant()
                    : w.Alias!);
            sb.Append(", ").Append(winExpr)
              .Append(" OVER (").Append(overClause).Append(") AS ")
              .Append(alias);
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
            var cols = partitionList.Select(p =>
                "g." + QuoteGwndIdent(ResolveGwndColumnName(p.FieldPath, aggList, groupList)));
            parts.Add("PARTITION BY " + string.Join(", ", cols));
        }

        if (orderList.Count > 0)
        {
            var cols = orderList.Select(o =>
                "g." + QuoteGwndIdent(ResolveGwndColumnName(o.FieldPath, aggList, groupList))
                + (o.Descending ? " DESC" : " ASC"));
            parts.Add("ORDER BY " + string.Join(", ", cols));
        }

        return string.Join(" ", parts);
    }

    /// <summary>
    /// Maps a window OVER-clause field reference to the inner GroupBy
    /// column alias. Recognises the synthetic <c>Agg_&lt;func&gt;_&lt;field&gt;</c>
    /// pattern emitted by <c>GroupedWindowedQueryable</c>.
    /// </summary>
    private static string ResolveGwndColumnName(
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
                    string.Equals(a.Function.ToString(), funcName,
                        StringComparison.OrdinalIgnoreCase)
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
            "ROWNUMBER" or "ROW_NUMBER"     => "ROW_NUMBER()",
            "RANK"                          => "RANK()",
            "DENSERANK" or "DENSE_RANK"     => "DENSE_RANK()",
            "PERCENTRANK" or "PERCENT_RANK" => "PERCENT_RANK()",
            "CUMEDIST" or "CUME_DIST"       => "CUME_DIST()",
            "NTILE" => "NTILE("
                + (w.Buckets ?? 4).ToString(CultureInfo.InvariantCulture) + ")",
            "COUNT" when string.IsNullOrEmpty(w.FieldPath) => "COUNT(*)",
            "SUM" or "AVG" or "MIN" or "MAX" or "COUNT"
                when !string.IsNullOrEmpty(w.FieldPath)
                => func + "(g."
                    + QuoteGwndIdent(ResolveGwndColumnName(w.FieldPath, aggList, groupList))
                    + ")",
            "LAG" or "LEAD"
            or "FIRSTVALUE" or "FIRST_VALUE"
            or "LASTVALUE"  or "LAST_VALUE"
                when !string.IsNullOrEmpty(w.FieldPath)
                => NormaliseGwndValueFunc(func) + "(g."
                    + QuoteGwndIdent(ResolveGwndColumnName(w.FieldPath, aggList, groupList))
                    + ")",
            _ => func + "()"
        };
    }

    private static string NormaliseGwndValueFunc(string func) => func switch
    {
        "FIRSTVALUE" => "FIRST_VALUE",
        "LASTVALUE"  => "LAST_VALUE",
        _            => func
    };

    /// <summary>
    /// T-SQL square-bracket identifier quoting (mirrors PG's double-quote quoting).
    /// </summary>
    private static string QuoteGwndIdent(string identifier)
        => "[" + identifier.Replace("]", "]]") + "]";
}
