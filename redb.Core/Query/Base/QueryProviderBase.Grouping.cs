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
        string? filterJson = null)
    {
        // Form JSON for group_fields
        // 🔥 CRITICAL: Adding the "0$:" prefix for base RedbObject fields!
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
        
        var result = await _context.QueryFirstOrDefaultAsync<GroupedResult>(_sql.Query_AggregateGroupedSql(), 
                schemeId, 
                groupFieldsJson, 
                aggregationsJson,
                filterJson ?? "null");
        
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
        FilterExpression? filter)
    {
        // Free version fallback: convert FilterExpression to facet-JSON
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return await ExecuteGroupedAggregateAsync(schemeId, groupFields, aggregations, filterJson);
    }
    
    private class GroupedResult
    {
        public string? result { get; set; }
    }
    
    /// <summary>
    /// Performs GroupBy aggregation on an array via the aggregate_array_grouped SQL function
    /// </summary>
    public virtual async Task<JsonDocument?> ExecuteArrayGroupedAggregateAsync(
        long schemeId,
        string arrayPath,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson = null)
    {
        // 🔥 CRITICAL: Adding the "0$:" prefix for base RedbObject fields!
        var groupFieldsJson = JsonSerializer.Serialize(
            groupFields.Select(g => new { 
                field = g.IsBaseField ? $"0$:{g.FieldPath}" : g.FieldPath, 
                alias = g.Alias 
            }));
        
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
        
        _logger?.LogDebug("GroupByArray: arrayPath={Array}, groupFields={GroupFields}, aggregations={Aggregations}", 
            arrayPath, groupFieldsJson, aggregationsJson);
        
        var result = await _context.QueryFirstOrDefaultAsync<GroupedResult>(_sql.Query_AggregateArrayGroupedSql(), 
                schemeId, 
                arrayPath,
                groupFieldsJson, 
                aggregationsJson,
                filterJson ?? "null");
        
        if (result?.result == null)
            return null;
        
        return JsonDocument.Parse(result.result);
    }
}
