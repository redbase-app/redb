using System.Text.Json;
using Microsoft.Extensions.Logging;
using redb.Core.Exceptions;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Grouping;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.Window;

namespace redb.Core.Query.Base;

/// <summary>
/// QueryProviderBase: GroupBy + Window Functions
/// </summary>
public abstract partial class QueryProviderBase
{
    /// <summary>
    /// Execute GroupBy with Window Functions.
    /// Base implementation: throws NotSupportedException - Pro feature.
    /// </summary>
    public virtual Task<JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null)
    {
        throw new RedbProRequiredException("GroupBy + Window Functions", ProFeatureCategory.WindowFunction);
    }
    
    /// <summary>
    /// Execute GroupBy with Window Functions with FilterExpression (Pro version).
    /// Free fallback: converts FilterExpression to facet-JSON.
    /// </summary>
    public virtual Task<JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter)
    {
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return ExecuteGroupedWindowQueryAsync(schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }
    
    /// <summary>
    /// SQL preview for GroupBy + Window query.
    /// Base implementation returns placeholder (overridden in Pro).
    /// </summary>
    public virtual Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null)
    {
        return Task.FromResult(
            $"-- GroupBy + Window SQL Preview not available in Open Source version\n" +
            $"-- SchemeId: {schemeId}\n" +
            $"-- GroupFields: {string.Join(", ", groupFields.Select(g => g.FieldPath))}\n" +
            $"-- Aggregations: {string.Join(", ", aggregations.Select(a => $"{a.Function}({a.FieldPath})"))}\n" +
            $"-- WindowFuncs: {string.Join(", ", windowFuncs.Select(w => w.Func))}\n" +
            $"-- Use Pro version for SQL preview");
    }
    
    /// <summary>
    /// SQL preview for GroupBy + Window query with FilterExpression (Pro version).
    /// Free fallback: converts FilterExpression to facet-JSON.
    /// </summary>
    public virtual Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter)
    {
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return GetGroupedWindowSqlPreviewAsync(schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }
}
