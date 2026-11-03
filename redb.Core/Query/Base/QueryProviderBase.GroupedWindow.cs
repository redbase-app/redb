using System.Text.Json;
using Microsoft.Extensions.Logging;
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
    /// Execute GroupBy with Window Functions. A provider that supports it overrides this; one that does not refuses
    /// with its own name.
    /// </summary>
    public virtual Task<JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? havingJson = null, CancellationToken cancellationToken = default)
        => throw new NotSupportedException($"GroupBy + WithWindow is not supported by {GetType().Name}.");

    /// <summary>
    /// Execute GroupBy with Window Functions with FilterExpression.
    /// Free fallback: converts FilterExpression to facet-JSON.
    /// </summary>
    public virtual Task<JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return ExecuteGroupedWindowQueryAsync(schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson, havingJson, cancellationToken);
    }

    /// <summary>
    /// SQL preview for GroupBy + Window query. A provider that supports it overrides this; one that does not refuses
    /// with its own name.
    /// </summary>
    public virtual Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? havingJson = null, CancellationToken cancellationToken = default)
        => throw new NotSupportedException($"GroupBy + WithWindow is not supported by {GetType().Name}.");

    /// <summary>
    /// SQL preview for GroupBy + Window query with FilterExpression.
    /// Free fallback: converts FilterExpression to facet-JSON.
    /// </summary>
    public virtual Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        IEnumerable<WindowFuncRequest> windowFuncs,
        IEnumerable<WindowFieldRequest> partitionBy,
        IEnumerable<WindowOrderRequest> orderBy,
        FilterExpression? filter,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return GetGroupedWindowSqlPreviewAsync(schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson, havingJson, cancellationToken);
    }

    /// <summary>
    /// SQL preview of a GroupBy. A provider that supports it overrides this; one that does not refuses with its own name.
    /// </summary>
    public virtual Task<string> GetGroupBySqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson,
        string? havingJson = null, CancellationToken cancellationToken = default)
        => throw new NotSupportedException($"GroupBy SQL preview is not supported by {GetType().Name}.");

    /// <summary>
    /// SQL preview of a GroupBy with FilterExpression. Free fallback: converts FilterExpression to facet-JSON.
    /// </summary>
    public virtual Task<string> GetGroupBySqlPreviewAsync(
        long schemeId,
        IEnumerable<GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        FilterExpression? filter,
        string? havingJson = null, CancellationToken cancellationToken = default)
    {
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return GetGroupBySqlPreviewAsync(schemeId, groupFields, aggregations, filterJson, havingJson, cancellationToken);
    }
}
