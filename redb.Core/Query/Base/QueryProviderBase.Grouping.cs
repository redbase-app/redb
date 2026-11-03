using System.Text.Json;
using Microsoft.Extensions.Logging;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Grouping;
using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query.Base;

/// <summary>
/// QueryProviderBase: GroupBy functionality
/// </summary>
public abstract partial class QueryProviderBase
{
    /// <summary>
    /// Performs GroupBy aggregation via the aggregate_grouped SQL function
    /// </summary>
    public virtual async Task<JsonDocument?> ExecuteGroupedAggregateAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson = null,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        if (!string.IsNullOrEmpty(havingJson))
        {
            // The legacy aggregate_grouped SQL function has no HAVING contract; PVT-capable
            // dialects override this method and consume the JSON shape there. When this base
            // implementation is reached with a HAVING predicate the caller is using a backend
            // that has not yet been wired for it, so we refuse loudly.
            throw new NotSupportedException(
                "HAVING is not supported by the legacy aggregate_grouped SQL function. " +
                "Use a PVT-capable provider (PostgreSQL) or the Pro builder.");
        }
        // Form JSON for group_fields
        // CRITICAL: Adding the "0$:" prefix for base RedbObject fields!
        var groupFieldsJson = JsonSerializer.Serialize(
            groupFields.Select(g => new {
                field = g.IsBaseField ? $"0$:{g.FieldPath}" : g.FieldPath,
                alias = g.Alias
            }));

        // Form JSON for aggregations (AVERAGE -> AVG for PostgreSQL)
        var aggregationsJson = JsonSerializer.Serialize(
            aggregations.Select(a => new {
                field = a.FieldPath,
                func = a.Function switch
                {
                    AggregateFunction.Average => "AVG",
                    _ => a.Function.ToString().ToUpper()
                },
                alias = a.Alias
            }));

        _logger?.LogDebug("GroupBy: groupFields={GroupFields}, aggregations={Aggregations}", groupFieldsJson, aggregationsJson);

        var result = await _context.QueryFirstOrDefaultAsync<GroupedResult>(_sql.Query_AggregateGroupedSql(), new object[] { schemeId,
                groupFieldsJson,
                aggregationsJson,
                filterJson ?? "null" }, cancellationToken);

        if (result?.result == null)
            return null;

        return JsonDocument.Parse(result.result);
    }

    /// <summary>
    /// Performs GroupBy aggregation with FilterExpression (Pro version).
    /// Free version fallback: converts FilterExpression to facet-JSON and calls the string-based method.
    /// </summary>
    public virtual async Task<JsonDocument?> ExecuteGroupedAggregateAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        FilterExpression? filter,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        // Free version fallback: convert FilterExpression to facet-JSON
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return await ExecuteGroupedAggregateAsync(schemeId, groupFields, aggregations, filterJson, havingJson);
    }

    private class GroupedResult
    {
        public string? result { get; set; }
    }

    /// <summary>
    /// Grouping over the elements of an array. Every provider that supports GroupByArray overrides it; a provider
    /// that does not refuses with its own name instead of answering with nothing.
    /// </summary>
    public virtual Task<JsonDocument> ExecuteArrayGroupQueryAsync(
        ArrayGroupQuery query, CancellationToken cancellationToken = default)
        => throw new NotSupportedException($"GroupByArray is not supported by {GetType().Name}.");

    /// <summary>
    /// SQL of <see cref="ExecuteArrayGroupQueryAsync"/>; see there.
    /// </summary>
    public virtual Task<string> GetArrayGroupQuerySqlAsync(
        ArrayGroupQuery query, CancellationToken cancellationToken = default)
        => throw new NotSupportedException($"GroupByArray is not supported by {GetType().Name}.");
}
