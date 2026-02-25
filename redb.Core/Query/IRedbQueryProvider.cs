using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Query.Aggregation;
using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query;

/// <summary>
/// Provider for executing LINQ queries.
/// </summary>
public interface IRedbQueryProvider
{
    /// <summary>
    /// Create new query for specified scheme.
    /// </summary>
    IRedbQueryable<TProps> CreateQuery<TProps>(long schemeId, long? userId = null, bool checkPermissions = false) 
        where TProps : class, new();
    
    /// <summary>
    /// Execute query asynchronously.
    /// </summary>
    Task<object> ExecuteAsync(Expression expression, Type elementType);
    
    /// <summary>
    /// Get scheme by ID (for projections).
    /// </summary>
    Task<IRedbScheme?> GetSchemeAsync(long schemeId);
    
    // ===== AGGREGATIONS (EAV) =====
    
    /// <summary>
    /// Execute aggregation on EAV field (SQL strategy for simple fields).
    /// </summary>
    /// <param name="schemeId">Scheme ID</param>
    /// <param name="fieldPath">Field path (e.g. "Price")</param>
    /// <param name="function">Aggregation function</param>
    /// <param name="filterJson">JSON filter (as for search_objects)</param>
    /// <returns>Aggregation result</returns>
    Task<decimal?> ExecuteAggregateAsync(
        long schemeId, 
        string fieldPath, 
        AggregateFunction function,
        string? filterJson = null);
    
    /// <summary>
    /// Execute batch aggregation (multiple fields in one query).
    /// </summary>
    Task<AggregateResult> ExecuteAggregateBatchAsync(
        long schemeId,
        IEnumerable<AggregateRequest> requests,
        string? filterJson = null);
    
    /// <summary>
    /// Execute single-field aggregation with FilterExpression (Pro version).
    /// </summary>
    Task<decimal?> ExecuteAggregateAsync(
        long schemeId,
        string fieldPath,
        AggregateFunction function,
        FilterExpression? filter);
    
    /// <summary>
    /// Execute batch aggregation with FilterExpression (Pro version).
    /// </summary>
    Task<AggregateResult> ExecuteAggregateBatchAsync(
        long schemeId,
        IEnumerable<AggregateRequest> requests,
        FilterExpression? filter);
    
    // ===== GROUPBY AGGREGATIONS =====
    
    /// <summary>
    /// Execute GroupBy aggregation (SQL function aggregate_grouped).
    /// Uses filterJson for Free version compatibility.
    /// </summary>
    Task<System.Text.Json.JsonDocument?> ExecuteGroupedAggregateAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson = null);
    
    /// <summary>
    /// Execute GroupBy aggregation with FilterExpression (Pro version).
    /// Provides direct access to filter for proper SQL compilation.
    /// </summary>
    Task<System.Text.Json.JsonDocument?> ExecuteGroupedAggregateAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        FilterExpression? filter);
    
    /// <summary>
    /// Execute GroupBy aggregation on array (SQL function aggregate_array_grouped).
    /// </summary>
    Task<System.Text.Json.JsonDocument?> ExecuteArrayGroupedAggregateAsync(
        long schemeId,
        string arrayPath,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson = null);
    
    // ===== WINDOW FUNCTIONS =====
    
    /// <summary>
    /// Execute query with window functions (SQL function query_with_window).
    /// </summary>
    Task<System.Text.Json.JsonDocument?> ExecuteWindowQueryAsync(
        long schemeId,
        IEnumerable<Window.WindowFieldRequest> selectFields,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? frameJson = null,
        int? take = null,
        int? skip = null);
    
    /// <summary>
    /// Execute query with window functions with FilterExpression (Pro version).
    /// </summary>
    Task<System.Text.Json.JsonDocument?> ExecuteWindowQueryAsync(
        long schemeId,
        IEnumerable<Window.WindowFieldRequest> selectFields,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        FilterExpression? filter,
        string? frameJson = null,
        int? take = null,
        int? skip = null);
    
    /// <summary>
    /// Get SQL preview for window query (for debugging).
    /// </summary>
    Task<string> GetWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<Window.WindowFieldRequest> selectFields,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? frameJson = null,
        int? take = null,
        int? skip = null);
    
    // ===== GROUPED WINDOW (GroupBy + Window Functions) =====
    
    /// <summary>
    /// Execute GroupBy with Window Functions (ranking, running totals on aggregated data).
    /// </summary>
    Task<System.Text.Json.JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? filterJson = null);
    
    /// <summary>
    /// Execute GroupBy with Window Functions with FilterExpression (Pro version).
    /// </summary>
    Task<System.Text.Json.JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        FilterExpression? filter);
    
    /// <summary>
    /// Get SQL preview for GroupBy + Window query.
    /// </summary>
    Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? filterJson = null);
    
    /// <summary>
    /// Get SQL preview for GroupBy + Window query with FilterExpression (Pro version).
    /// </summary>
    Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        FilterExpression? filter);
    
    // ===== DELETE =====
    
    /// <summary>
    /// Delete objects by filter (OpenSource version with JSON filter).
    /// </summary>
    /// <param name="schemeId">Scheme ID</param>
    /// <param name="filterJson">JSON filter</param>
    /// <returns>Number of deleted objects</returns>
    Task<int> ExecuteDeleteAsync(long schemeId, string? filterJson = null);
    
    /// <summary>
    /// Delete objects by filter (Pro version with FilterExpression for PVT-based deletion).
    /// </summary>
    /// <param name="schemeId">Scheme ID</param>
    /// <param name="filter">Filter expression from LINQ</param>
    /// <returns>Number of deleted objects</returns>
    Task<int> ExecuteDeleteAsync(long schemeId, FilterExpression? filter);
    
    // ===== SQL PREVIEW =====
    
    /// <summary>
    /// Get SQL query for debugging (analogous to ToQueryString in EF Core).
    /// Pro version returns PVT SQL, Open Source - redb_json_objects.
    /// </summary>
    Task<string> GetSqlPreviewAsync<TProps>(QueryContext<TProps> context) where TProps : class, new();
    
    /// <summary>
    /// Returns the JSON filter that will be sent to SQL function (for diagnostics)
    /// </summary>
    Task<string> GetFilterJsonAsync<TProps>(QueryContext<TProps> context) where TProps : class, new();
}
