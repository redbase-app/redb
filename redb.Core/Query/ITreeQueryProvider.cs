using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using redb.Core.Caching;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Query;

/// <summary>
/// Extended provider for executing tree LINQ queries.
/// Inherits base IRedbQueryProvider functionality and adds hierarchical constraint support.
/// </summary>
public interface ITreeQueryProvider : IRedbQueryProvider
{
    /// <summary>
    /// Metadata cache for this provider (domain-isolated).
    /// </summary>
    GlobalMetadataCache Cache { get; }
    
    /// <summary>
    /// Create tree query with hierarchical constraint support.
    /// Uses SQL function search_tree_objects_with_facets() instead of search_objects_with_facets().
    /// Returns IRedbQueryable with full tree method support.
    /// </summary>
    /// <param name="schemeId">Object scheme ID</param>
    /// <param name="userId">User ID for permission check (null = current user)</param>
    /// <param name="checkPermissions">Check object access permissions</param>
    /// <param name="rootObjectId">Limit search to subtree of specified root (null = entire forest)</param>
    /// <param name="maxDepth">Maximum tree search depth (null = unlimited)</param>
    /// <returns>Typed query with tree operation support</returns>
    IRedbQueryable<TProps> CreateTreeQuery<TProps>(
        long schemeId, 
        long? userId = null, 
        bool checkPermissions = false,
        long? rootObjectId = null,
        int? maxDepth = null
    ) where TProps : class, new();
    
    /// <summary>
    /// Get IDs of all objects and their parents to root via recursive CTE.
    /// Used for building hierarchical relationships in ToTreeListAsync.
    /// </summary>
    /// <param name="filteredIds">IDs of filtered objects</param>
    /// <returns>List of all IDs including parents to root</returns>
    Task<List<long>> GetIdsWithAncestorsAsync<TProps>(List<long> filteredIds) where TProps : class, new();
    
    /// <summary>
    /// Load full typed objects by ID list via v_objects_json.
    /// Used for loading homogeneous objects of one type.
    /// </summary>
    /// <param name="objectIds">Object IDs to load</param>
    /// <param name="propsDepth">Maximum depth for nested RedbObject loading (null = use config default)</param>
    /// <returns>List of TreeRedbObject with full data</returns>
    Task<List<TreeRedbObject<TProps>>> LoadObjectsByIdsAsync<TProps>(List<long> objectIds, int? propsDepth = null) where TProps : class, new();
    
    /// <summary>
    /// Load full untyped objects by ID list via v_objects_json.
    /// Used for loading polymorphic trees (objects of different types).
    /// Returns ITreeRedbObject for polymorphism support (each object of its own type).
    /// </summary>
    /// <param name="objectIds">Object IDs to load</param>
    /// <param name="propsDepth">Maximum depth for nested RedbObject loading (null = use config default)</param>
    /// <returns>List of polymorphic ITreeRedbObject (each of its own TProps type)</returns>
    Task<List<ITreeRedbObject>> LoadObjectsByIdsAsync(List<long> objectIds, int? propsDepth = null);
    
    /// <summary>
    /// Delete objects by ID array.
    /// Used in TreeQueryable.DeleteAsync() to delete objects after filtering.
    /// </summary>
    /// <param name="objectIds">Object IDs to delete</param>
    /// <returns>Number of deleted objects</returns>
    Task<int> ExecuteTreeDeleteAsync(long[] objectIds);
    
    /// <summary>
    /// Execute GROUP BY aggregation with tree context (CTE for tree traversal).
    /// Used by TreeGroupedQueryable to perform tree-aware grouping.
    /// </summary>
    /// <typeparam name="TProps">Object properties type</typeparam>
    /// <param name="context">Tree query context with rootObjectId, maxDepth, filters</param>
    /// <param name="groupFields">Fields to group by</param>
    /// <param name="aggregations">Aggregation functions to apply</param>
    /// <returns>JSON document with grouped results</returns>
    Task<System.Text.Json.JsonDocument?> ExecuteTreeGroupedAggregateAsync<TProps>(
        Base.TreeQueryContext<TProps> context,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations) where TProps : class, new();
    
    /// <summary>
    /// Execute Window Functions query with tree context (CTE for tree traversal).
    /// Used by TreeWindowedQueryable to perform tree-aware window functions.
    /// </summary>
    /// <typeparam name="TProps">Object properties type</typeparam>
    /// <param name="context">Tree query context with rootObjectId, maxDepth, filters</param>
    /// <param name="selectFields">Fields to select</param>
    /// <param name="windowFuncs">Window functions to apply</param>
    /// <param name="partitionBy">Partition by fields</param>
    /// <param name="orderBy">Order by fields</param>
    /// <param name="frameJson">Optional window frame specification</param>
    /// <returns>JSON document with windowed results</returns>
    Task<System.Text.Json.JsonDocument?> ExecuteTreeWindowQueryAsync<TProps>(
        Base.TreeQueryContext<TProps> context,
        IEnumerable<Window.WindowFieldRequest> selectFields,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? frameJson = null) where TProps : class, new();
    
    /// <summary>
    /// Get SQL preview for tree window query (for debugging).
    /// </summary>
    Task<string> GetTreeWindowSqlPreviewAsync<TProps>(
        Base.TreeQueryContext<TProps> context,
        IEnumerable<Window.WindowFieldRequest> selectFields,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? frameJson = null) where TProps : class, new();
    
    /// <summary>
    /// Get SQL preview for tree GROUP BY query (for debugging).
    /// </summary>
    Task<string> GetTreeGroupBySqlPreviewAsync<TProps>(
        Base.TreeQueryContext<TProps> context,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations) where TProps : class, new();
    
    // ===== TREE GROUPED WINDOW (GroupBy + Window for Trees) =====
    
    /// <summary>
    /// Execute GroupBy + Window Functions with tree context.
    /// Allows ranking, running totals on aggregated tree data.
    /// </summary>
    Task<System.Text.Json.JsonDocument?> ExecuteTreeGroupedWindowQueryAsync<TProps>(
        Base.TreeQueryContext<TProps> context,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy) where TProps : class, new();
    
    /// <summary>
    /// Get SQL preview for tree GroupBy + Window query.
    /// </summary>
    Task<string> GetTreeGroupedWindowSqlPreviewAsync<TProps>(
        Base.TreeQueryContext<TProps> context,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy) where TProps : class, new();
}
