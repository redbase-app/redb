using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Core.Query.Aggregation;
using redb.Core.Query.FacetFilters;
using redb.Core.Query.QueryExpressions;
using redb.Core.Serialization;
using redb.Core.Utils;
using redb.Core.Caching;

namespace redb.Core.Query.Base;

/// <summary>
/// Base tree query provider for executing hierarchical LINQ queries via search_tree_objects_with_facets.
/// Database-specific implementations should inherit and provide ISqlDialect.
/// </summary>
public abstract class TreeQueryProviderBase : ITreeQueryProvider
{
    protected readonly IRedbContext _context;
    protected readonly IRedbObjectSerializer _serializer;
    protected readonly IFilterExpressionParser _filterParser;
    protected readonly IOrderingExpressionParser _orderingParser;
    protected readonly IFacetFilterBuilder _facetBuilder;
    protected readonly ILogger? _logger;
    protected readonly ILazyPropsLoader? _lazyPropsLoader;
    protected readonly RedbServiceConfiguration _configuration;
    protected readonly ISqlDialect _sql;
    protected readonly ISchemeSyncProvider? _schemeSync;
    
    /// <summary>
    /// Metadata cache for this provider (domain-isolated).
    /// </summary>
    public GlobalMetadataCache Cache { get; }

    protected TreeQueryProviderBase(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ISqlDialect dialect,
        string? cacheDomain = null,
        ILazyPropsLoader? lazyPropsLoader = null,
        RedbServiceConfiguration? configuration = null,
        ILogger? logger = null,
        ISchemeSyncProvider? schemeSync = null)
    {
        _context = context;
        _serializer = serializer;
        _sql = dialect;
        _lazyPropsLoader = lazyPropsLoader;
        _configuration = configuration ?? new RedbServiceConfiguration();
        _logger = logger;
        _schemeSync = schemeSync;
        Cache = new GlobalMetadataCache(cacheDomain ?? _configuration.GetEffectiveCacheDomain());
        _filterParser = CreateFilterParser();
        _orderingParser = CreateOrderingParser();
        _facetBuilder = CreateFacetBuilder();
    }
    
    /// <summary>
    /// Creates filter expression parser. Override for Pro features.
    /// </summary>
    protected abstract IFilterExpressionParser CreateFilterParser();
    
    /// <summary>
    /// Creates ordering expression parser.
    /// </summary>
    protected virtual IOrderingExpressionParser CreateOrderingParser() => new OrderingExpressionParser();
    
    /// <summary>
    /// Creates facet filter builder.
    /// </summary>
    protected virtual IFacetFilterBuilder CreateFacetBuilder() => new FacetFilterBuilder(_logger);
    
    /// <summary>
    /// Creates query provider for delegation. Override in derived classes.
    /// </summary>
    protected abstract IRedbQueryProvider CreateQueryProvider();
    
    /// <summary>
    /// Creates tree queryable instance. Override in derived classes.
    /// </summary>
    protected abstract IRedbQueryable<TProps> CreateTreeQueryable<TProps>(TreeQueryContext<TProps> context) 
        where TProps : class, new();
    
    /// <summary>
    /// Determines if lazy loading should be used for this query.
    /// </summary>
    protected bool ShouldUseLazyLoading<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        // If explicitly specified in context - use this value (priority)
        if (context.UseLazyLoading.HasValue)
            return context.UseLazyLoading.Value && _lazyPropsLoader != null;

        // Global setting disabled - immediately false
        if (!_configuration.EnableLazyLoadingForProps)
            return false;

        // If lazy loader not available - false
        return _lazyPropsLoader != null;
    }
    
    /// <summary>
    /// Determines if lazy loading should be used for tree query.
    /// </summary>
    protected bool ShouldUseLazyLoading<TProps>(TreeQueryContext<TProps> context) where TProps : class, new()
    {
        // If explicitly specified in context - use this value (priority)
        if (context.UseLazyLoading.HasValue)
            return context.UseLazyLoading.Value && _lazyPropsLoader != null;

        // Global setting disabled - immediately false
        if (!_configuration.EnableLazyLoadingForProps)
            return false;

        // If lazy loader not available - false
        return _lazyPropsLoader != null;
    }

    /// <summary>
    /// Check for Pro-only Distinct features (DistinctBy, DistinctByRedb, DistinctRedb).
    /// Throws NotSupportedException in Free version.
    /// </summary>
    protected virtual void CheckProOnlyDistinctFeatures<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        if (context.IsDistinctRedb)
        {
            throw new RedbProRequiredException("DistinctRedb()", ProFeatureCategory.DistinctQuery);
        }
        if (context.DistinctByField != null)
        {
            throw new RedbProRequiredException(context.DistinctByIsBaseField ? "DistinctByRedb()" : "DistinctBy()", ProFeatureCategory.DistinctQuery);
        }
    }

    // ===== IRedbQueryProvider IMPLEMENTATION (BASE FUNCTIONALITY) =====
    
    public IRedbQueryable<TProps> CreateQuery<TProps>(long schemeId, long? userId = null, bool checkPermissions = false) 
        where TProps : class, new()
    {
        // For regular queries create standard RedbQueryable
        // It will use search_objects_with_facets() through base QueryProviderBase
        // Pass _lazyPropsLoader and _configuration for lazy loading support
        var baseProvider = CreateQueryProvider();
        return baseProvider.CreateQuery<TProps>(schemeId, userId, checkPermissions);
    }

    // ===== AGGREGATIONS IMPLEMENTATION (delegate to QueryProviderBase) =====
    // ✅ virtual for override in Pro version
    
    public virtual async Task<decimal?> ExecuteAggregateAsync(long schemeId, string fieldPath, AggregateFunction function, string? filterJson = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteAggregateAsync(schemeId, fieldPath, function, filterJson);
    }
    
    public virtual async Task<decimal?> ExecuteAggregateAsync(long schemeId, string fieldPath, AggregateFunction function, QueryExpressions.FilterExpression? filter)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteAggregateAsync(schemeId, fieldPath, function, filter);
    }
    
    public virtual async Task<AggregateResult> ExecuteAggregateBatchAsync(long schemeId, IEnumerable<AggregateRequest> requests, string? filterJson = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteAggregateBatchAsync(schemeId, requests, filterJson);
    }
    
    public virtual async Task<AggregateResult> ExecuteAggregateBatchAsync(long schemeId, IEnumerable<AggregateRequest> requests, QueryExpressions.FilterExpression? filter)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteAggregateBatchAsync(schemeId, requests, filter);
    }
    
    /// <summary>
    /// Get scheme by ID (for projections) - delegate to base provider
    /// </summary>
    public virtual async Task<IRedbScheme?> GetSchemeAsync(long schemeId)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.GetSchemeAsync(schemeId);
    }
    
    // ===== GROUPBY (delegate to QueryProviderBase) =====
    // ✅ virtual for override in Pro version
    
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteGroupedAggregateAsync(
        long schemeId,
        IEnumerable<redb.Core.Query.Grouping.GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteGroupedAggregateAsync(schemeId, groupFields, aggregations, filterJson);
    }
    
    /// <summary>
    /// Execute GroupBy aggregation with FilterExpression (Pro version).
    /// Delegates to QueryProvider.
    /// </summary>
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteGroupedAggregateAsync(
        long schemeId,
        IEnumerable<redb.Core.Query.Grouping.GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        QueryExpressions.FilterExpression? filter)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteGroupedAggregateAsync(schemeId, groupFields, aggregations, filter);
    }
    
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteArrayGroupedAggregateAsync(
        long schemeId,
        string arrayPath,
        IEnumerable<redb.Core.Query.Grouping.GroupFieldRequest> groupFields,
        IEnumerable<AggregateRequest> aggregations,
        string? filterJson = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteArrayGroupedAggregateAsync(schemeId, arrayPath, groupFields, aggregations, filterJson);
    }
    
    // ===== WINDOW FUNCTIONS (delegate to QueryProviderBase) =====
    // ✅ virtual for override in Pro version
    
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteWindowQueryAsync(
        long schemeId,
        IEnumerable<redb.Core.Query.Window.WindowFieldRequest> selectFields,
        IEnumerable<redb.Core.Query.Window.WindowFuncRequest> windowFuncs,
        IEnumerable<redb.Core.Query.Window.WindowFieldRequest> partitionBy,
        IEnumerable<redb.Core.Query.Window.WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteWindowQueryAsync(schemeId, selectFields, windowFuncs, partitionBy, orderBy, filterJson, frameJson, take, skip);
    }
    
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteWindowQueryAsync(
        long schemeId,
        IEnumerable<redb.Core.Query.Window.WindowFieldRequest> selectFields,
        IEnumerable<redb.Core.Query.Window.WindowFuncRequest> windowFuncs,
        IEnumerable<redb.Core.Query.Window.WindowFieldRequest> partitionBy,
        IEnumerable<redb.Core.Query.Window.WindowOrderRequest> orderBy,
        QueryExpressions.FilterExpression? filter,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteWindowQueryAsync(schemeId, selectFields, windowFuncs, partitionBy, orderBy, filter, frameJson, take, skip);
    }
    
    public virtual async Task<string> GetWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<redb.Core.Query.Window.WindowFieldRequest> selectFields,
        IEnumerable<redb.Core.Query.Window.WindowFuncRequest> windowFuncs,
        IEnumerable<redb.Core.Query.Window.WindowFieldRequest> partitionBy,
        IEnumerable<redb.Core.Query.Window.WindowOrderRequest> orderBy,
        string? filterJson = null,
        string? frameJson = null,
        int? take = null,
        int? skip = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.GetWindowSqlPreviewAsync(schemeId, selectFields, windowFuncs, partitionBy, orderBy, filterJson, frameJson, take, skip);
    }
    
    // ===== GROUPED WINDOW (delegated to base provider) =====
    
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? filterJson = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteGroupedWindowQueryAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }
    
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteGroupedWindowQueryAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        QueryExpressions.FilterExpression? filter)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteGroupedWindowQueryAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filter);
    }
    
    public virtual async Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? filterJson = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.GetGroupedWindowSqlPreviewAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }
    
    public virtual async Task<string> GetGroupedWindowSqlPreviewAsync(
        long schemeId,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        QueryExpressions.FilterExpression? filter)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.GetGroupedWindowSqlPreviewAsync(
            schemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filter);
    }

    // ===== ITreeQueryProvider IMPLEMENTATION (TREE FUNCTIONALITY) =====
    
    public IRedbQueryable<TProps> CreateTreeQuery<TProps>(
        long schemeId, 
        long? userId = null, 
        bool checkPermissions = false,
        long? rootObjectId = null,
        int? maxDepth = null
    ) where TProps : class, new()
    {
        var context = new TreeQueryContext<TProps>(schemeId, userId, checkPermissions, rootObjectId, maxDepth);
        return CreateTreeQueryable(context);
    }

    // ===== QUERY EXECUTION =====
    
    public async Task<object> ExecuteAsync(Expression expression, Type elementType)
    {
        // Extract context from expression
        if (expression is ConstantExpression constantExpr && constantExpr.Value != null)
        {
            // Determine operation type by elementType
            if (elementType == typeof(int))
            {
                return await ExecuteCountAsyncGeneric(constantExpr.Value);
            }
            else if (elementType.IsGenericType)
            {
                var genericType = elementType.GetGenericTypeDefinition();
                if (genericType == typeof(List<>))
                {
                    // Determine result type (RedbObject or TreeRedbObject)
                    var itemType = elementType.GetGenericArguments()[0];
                    if (itemType.IsGenericType && itemType.GetGenericTypeDefinition() == typeof(TreeRedbObject<>))
                    {
                        return await ExecuteTreeToListAsyncGeneric(constantExpr.Value);
                    }
                    else
                    {
                        return await ExecuteToListAsyncGeneric(constantExpr.Value);
                    }
                }
            }
        }

        throw new NotSupportedException($"Tree query expression type {expression.GetType().Name} with element type {elementType.Name} is not supported");
    }

    // ===== COUNT QUERY EXECUTION =====
    
    private async Task<int> ExecuteCountAsyncGeneric(object contextObj)
    {
        // Use reflection to call typed method
        // ✅ this.GetType() - support override in Pro version
        var contextType = contextObj.GetType();

        if (contextType.IsGenericType && contextType.GetGenericTypeDefinition() == typeof(TreeQueryContext<>))
        {
            // Tree query
            var propsType = contextType.GetGenericArguments()[0];
            var method = this.GetType().GetMethod(nameof(ExecuteTreeCountAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var genericMethod = method!.MakeGenericMethod(propsType);
            var task = (Task<int>)genericMethod.Invoke(this, new[] { contextObj })!;
            return await task;
        }
        else if (contextType.IsGenericType && contextType.GetGenericTypeDefinition() == typeof(QueryContext<>))
        {
            // Regular query - delegate to base provider
            // Pass _lazyPropsLoader and _configuration for lazy loading support
            var baseProvider = CreateQueryProvider();
            return await (Task<int>)typeof(QueryProviderBase)
                .GetMethod("ExecuteCountAsyncGeneric", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(baseProvider, new[] { contextObj })!;
        }
        
        throw new NotSupportedException($"Unsupported context type: {contextType.Name}");
    }

    // ===== TOLIST QUERY EXECUTION =====
    
    private async Task<object> ExecuteToListAsyncGeneric(object contextObj)
    {
        var contextType = contextObj.GetType();
        
        if (contextType.IsGenericType && contextType.GetGenericTypeDefinition() == typeof(QueryContext<>))
        {
            // Regular query - delegate to base provider
            // Pass _lazyPropsLoader and _configuration for lazy loading support
            var baseProvider = CreateQueryProvider();
            return await (Task<object>)typeof(QueryProviderBase)
                .GetMethod("ExecuteToListAsyncGeneric", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .Invoke(baseProvider, new[] { contextObj })!;
        }
        else if (contextType.IsGenericType && contextType.GetGenericTypeDefinition() == typeof(TreeQueryContext<>))
        {
            // Tree query - use our method
            // ✅ this.GetType() - support override in Pro version
            var propsType = contextType.GetGenericArguments()[0];
            var method = this.GetType().GetMethod(nameof(ExecuteTreeToListAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var genericMethod = method!.MakeGenericMethod(propsType);
            var task = (Task<object>)genericMethod.Invoke(this, new[] { contextObj })!;
            return await task;
        }
        
        throw new NotSupportedException($"Unsupported context type for ToList: {contextType.Name}");
    }
    
    private async Task<object> ExecuteTreeToListAsyncGeneric(object contextObj)
    {
        var contextType = contextObj.GetType();
        
        if (contextType.IsGenericType && contextType.GetGenericTypeDefinition() == typeof(TreeQueryContext<>))
        {
            // Tree query
            // ✅ this.GetType() - support override in Pro version
            var propsType = contextType.GetGenericArguments()[0];
            var method = this.GetType().GetMethod(nameof(ExecuteTreeToListAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var genericMethod = method!.MakeGenericMethod(propsType);
            var task = (Task<object>)genericMethod.Invoke(this, new[] { contextObj })!;
            return await task;
        }
        
        throw new NotSupportedException($"Unsupported context type for TreeToList: {contextType.Name}");
    }

    // ===== TREE EXECUTION METHODS =====

    /// <summary>
    /// Execute COUNT for tree query through search_tree_objects_with_facets
    /// </summary>
    protected virtual async Task<int> ExecuteTreeCountAsync<TProps>(TreeQueryContext<TProps> context) where TProps : class, new()
    {
        // Check for Pro-only Distinct features (DistinctBy, DistinctByRedb, DistinctRedb)
        CheckProOnlyDistinctFeatures(context);
        
        try
        {
            // Build JSON filter with tree operators
            var facetFilter = BuildTreeFacetFilter(context);
            var filterJson = facetFilter?.RootElement.ToString() ?? "{}";

            // OPTIMIZATION: ALWAYS use _base functions for fast search (without Props)
            // Props will be loaded separately via LoadPropsForManyAsync (with cache check)
            var treeFunctionName = _sql.Query_SearchTreeObjectsBaseFunction();
            var normalFunctionName = _sql.Query_SearchObjectsBaseFunction();
            
            // Use dialect-specific SQL for COUNT
            int totalCount;
            
            // 🚀 FIX: If rootObjectId=null, use normal search across entire scheme
            if (!context.RootObjectId.HasValue)
            {
                // Search entire scheme - use normal function
                var sqlCount = _sql.Query_TreeCountNormalSql(normalFunctionName);
                var countResult = await _context.ExecuteScalarAsync<int?>(
                    sqlCount, 
                    context.SchemeId,
                    filterJson,
                    context.MaxRecursionDepth ?? 10);  // Use default value instead of DBNull
                totalCount = countResult ?? 0;
            }
            else
            {
                // Subtree search - use dialect-specific tree count SQL
                var sql = _sql.Query_TreeCountWithParentIdsSql(treeFunctionName);
                
                var countResult = await _context.ExecuteScalarAsync<int?>(
                    sql, 
                    context.SchemeId, 
                    new[] { context.RootObjectId.Value },  // ✅ BATCH: Array with single ID
                    filterJson, 
                    context.MaxDepth ?? 1000,              // Use default value instead of DBNull
                    context.MaxRecursionDepth ?? 10);    // Use default value instead of DBNull
                totalCount = countResult ?? 0;
            }

            return totalCount;
        }
        catch (NotSupportedException)
        {
            // Expected exception for Pro features in Free version - don't log as error
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error executing tree COUNT query");
            throw;
        }
    }

    /// <summary>
    /// Execute ToList for tree query through search_tree_objects_with_facets
    /// </summary>
    protected virtual async Task<object> ExecuteTreeToListAsync<TProps>(TreeQueryContext<TProps> context) where TProps : class, new()
    {
        // Check for Pro-only Distinct features (DistinctBy, DistinctByRedb, DistinctRedb)
        CheckProOnlyDistinctFeatures(context);
        
        try
        {
            // 🚀 AUTOMATIC OPTIMIZATION WhereHasAncestor/WhereHasDescendant
            // _logger?.LogInformation($"🔍 ExecuteTreeToListAsync: TreeFilters.Count = {context.TreeFilters?.Count ?? 0}");
            // if (context.TreeFilters != null)
            // {
            //     foreach (var filter in context.TreeFilters)
            //     {
            //         _logger?.LogInformation($"   - Filter: {filter.Operator}, TargetSchemeId={filter.TargetSchemeId}");
            //     }
            // }
            
            var hasAncestorFilter = GetOptimizableHasAncestorFilter(context);
            // _logger?.LogInformation($"🔍 GetOptimizableHasAncestorFilter returned: {(hasAncestorFilter != null ? "NOT NULL" : "NULL")}");
            
            if (hasAncestorFilter != null)
            {
                return (object)await ExecuteOptimizedWhereHasAncestor(context, hasAncestorFilter);
            }

            var hasDescendantFilter = GetOptimizableHasDescendantFilter(context);
            if (hasDescendantFilter != null)
            {
                return (object)await ExecuteOptimizedWhereHasDescendant(context, hasDescendantFilter);
            }

            // Build JSON filters
            var facetFilter = BuildTreeFacetFilter(context);
            var orderBy = BuildOrderByFilter(context);

            var filterJson = facetFilter?.RootElement.ToString() ?? "{}";
            var orderByJson = orderBy?.RootElement.ToString() ?? "null";

            // OPTIMIZATION: ALWAYS use _base functions for fast search (without Props)
            // Props will be loaded separately via LoadPropsForManyAsync (with cache check)
            var treeFunctionName = _sql.Query_SearchTreeObjectsBaseFunction();
            var normalFunctionName = _sql.Query_SearchObjectsBaseFunction();
            
            // Calculate lazy loading mode ONCE for entire method
            // UseLazyLoading overrides config if explicitly set (via .WithLazyLoading())
            var useLazyOnDemand = context.UseLazyLoading ?? _configuration.EnableLazyLoadingForProps;

            // Call function with correct parameters  
            // Parameters: scheme_id, parent_ids, facet_filters, limit, offset, order_by, max_depth, max_recursion_depth
            var sql = _sql.Query_TreeSearchWithParentIdsSql(treeFunctionName);

            string objectsJson;
            
            // ✅ BATCH OPTIMIZATION: Process multiple parents with ONE request!
            if (context.ParentIds != null && context.ParentIds.Length > 0)
            {
                // 🔍 DEBUG: Log SQL and parameters
                _logger?.LogInformation($"🔍 SQL: {sql}");
                _logger?.LogInformation($"🔍 Params: scheme_id={context.SchemeId}, parent_ids=[{string.Join(",", context.ParentIds)}], max_depth={context.MaxDepth ?? 1000}");
                
                // ✅ ONE SQL query instead of loop! Pass entire parent_ids array
                objectsJson = await _context.ExecuteJsonAsync(
                    sql,
                    context.SchemeId,
                    context.ParentIds,           // ✅ BATCH: All parent IDs!
                    filterJson,
                    context.Limit ?? int.MaxValue,
                    context.Offset ?? 0,
                    orderByJson,
                    context.MaxDepth ?? 1000,
                    context.MaxRecursionDepth ?? 10);
                
                if (string.IsNullOrEmpty(objectsJson))
                    return (object)new List<TreeRedbObject<TProps>>();
                
                var combinedResults = DeserializeTreeObjects<TProps>(objectsJson);
                
                // Apply common limits and sorting
                if (context.Offset.HasValue && context.Offset.Value > 0)
                {
                    combinedResults = combinedResults.Skip(context.Offset.Value).ToList();
                }
                
                if (context.Limit.HasValue)
                {
                    combinedResults = combinedResults.Take(context.Limit.Value).ToList();
                }
                
                // SET _lazyLoader ONLY if useLazyOnDemand=true (on-demand loading)
                if (useLazyOnDemand && combinedResults.Count > 0 && _lazyPropsLoader != null)
                {
                    foreach (var treeObj in combinedResults)
                    {
                        if (treeObj.id > 0)
                        {
                            treeObj._lazyLoader = _lazyPropsLoader;
                            treeObj._propsLoaded = false;
                            _logger?.LogDebug("🔄 Lazy loader set for object {ObjectId} (on-demand mode)", treeObj.id);
                        }
                    }
                }
                else if (combinedResults.Count > 0 && _lazyPropsLoader != null)
                {
                    // BULK LOAD Props via LoadPropsForManyAsync (two-phase loading)
                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    var baseObjects = combinedResults.Cast<RedbObject<TProps>>().ToList();
                    await _lazyPropsLoader.LoadPropsForManyAsync(baseObjects, context.PropsDepth);
                    sw.Stop();
                    _logger?.LogInformation("⏱️  Props loaded via batch (tree, multiple roots) in {ElapsedMs} ms", sw.ElapsedMilliseconds);
                }
                
                // ✅ DISTINCT is now performed at SQL level (search_tree_objects_with_facets_base)
                
                return (object)combinedResults;
            }
            // 🚀 FIX: If rootObjectId=null, use normal search across entire scheme
            else if (!context.RootObjectId.HasValue)
            {
                // Search across ENTIRE scheme - use search_objects_with_facets (without tree restrictions)
                var sqlSearch = _sql.Query_TreeSearchNormalSql(normalFunctionName);
                objectsJson = await _context.ExecuteJsonAsync(
                    sqlSearch,
                    context.SchemeId,
                    filterJson,
                    context.Limit ?? int.MaxValue,  // limit_count - if not specified, get all
                    context.Offset ?? 0,        // offset_count
                    orderByJson,                // order_by
                    context.MaxRecursionDepth ?? 10);
            }
            else
            {
                // Subtree search - use search_tree_objects_with_facets
                objectsJson = await _context.ExecuteJsonAsync(
                    sql,
                    context.SchemeId,
                    new[] { context.RootObjectId.Value },  // ✅ BATCH: Array with single ID for compatibility
                    filterJson,
                    context.Limit ?? int.MaxValue,  // limit_count - if not specified, get all
                    context.Offset ?? 0,        // offset_count
                    orderByJson,                // order_by
                    context.MaxDepth ?? 1000,     // max_depth
                    context.MaxRecursionDepth ?? 10);
            }

            // Deserialize result to tree objects
            var result = DeserializeTreeObjects<TProps>(objectsJson);
            
            // SET _lazyLoader ONLY if useLazyOnDemand=true (on-demand loading)
            if (useLazyOnDemand && result.Count > 0 && _lazyPropsLoader != null)
            {
                foreach (var treeObj in result)
                {
                    if (treeObj.id > 0)
                    {
                        treeObj._lazyLoader = _lazyPropsLoader;
                        treeObj._propsLoaded = false;
                    }
                }
            }
            else if (result.Count > 0 && _lazyPropsLoader != null)
            {
                // BULK LOAD Props via LoadPropsForManyAsync (two-phase loading)
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var baseObjects = result.Cast<RedbObject<TProps>>().ToList();
                await _lazyPropsLoader.LoadPropsForManyAsync(baseObjects, context.PropsDepth);
                sw.Stop();
                _logger?.LogInformation("⏱️  Props loaded via batch (tree) in {ElapsedMs} ms", sw.ElapsedMilliseconds);
            }
            
            // ✅ DISTINCT is now performed at SQL level (search_tree_objects_with_facets)
            
            return (object)result;
        }
        catch (NotSupportedException)
        {
            // Expected exception for Pro features in Free version - don't log as error
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error executing tree ToList query");
            throw;
        }
    }

    // ===== FILTER BUILDING =====

    /// <summary>
    /// Build JSON facet filter with tree operators support
    /// </summary>
    private JsonDocument? BuildTreeFacetFilter<TProps>(TreeQueryContext<TProps> context) where TProps : class, new()
    {
        var filters = new Dictionary<string, object>();

        // 1. 🚀 POWERFUL FILTER SYSTEM (use PostgresFacetFilterBuilder)
        if (context.Filter != null)
        {
            // Use the same powerful system as in regular LINQ - ALL 25+ operators!
            var facetFiltersJson = _facetBuilder.BuildFacetFilters(context.Filter);
            
            // Parse JSON back to Dictionary for merging with tree filters
            if (facetFiltersJson != "{}")
            {
                var facetFiltersDict = JsonSerializer.Deserialize<Dictionary<string, object>>(facetFiltersJson);
                if (facetFiltersDict != null)
                {
                    foreach (var kvp in facetFiltersDict)
            {
                filters[kvp.Key] = kvp.Value;
                    }
                }
            }
        }

        // 2. 🌳 Tree operators (new functionality)
        if (context.TreeFilters != null && context.TreeFilters.Any())
        {
            foreach (var treeFilter in context.TreeFilters)
            {
                AddTreeFilterToJson(filters, treeFilter);
            }
        }

        // 3. 🌳 REMOVED: DO NOT add $descendantsOf if rootObjectId already exists
        // Reason: search_tree_objects_with_facets ALREADY restricts by parent_id
        // Additional $descendantsOf creates CONFLICT and returns 0 results!
        // if (context.RootObjectId.HasValue) - REMOVED!

        // Return JSON document if filters exist
        if (!filters.Any()) return null;
        
        var jsonString = JsonSerializer.Serialize(filters, new JsonSerializerOptions 
        { 
            WriteIndented = false,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping  // Preserve Cyrillic without escaping
        });
        return JsonDocument.Parse(jsonString);
    }

    /// <summary>
    /// Determines if WhereHasAncestor can be optimized through logic inversion
    /// </summary>
    protected TreeFilter? GetOptimizableHasAncestorFilter<TProps>(TreeQueryContext<TProps> context) 
        where TProps : class, new()
    {
        // Find first WhereHasAncestor filter (optimize one by one)
        // TargetSchemeId can be 0 if type is not registered in AutomaticTypeRegistry
        var filter = context.TreeFilters?.FirstOrDefault(f => 
            f.Operator == TreeFilterOperator.HasAncestor);
        
        // Check that TargetSchemeId exists and is not equal to 0
        if (filter != null && (!filter.TargetSchemeId.HasValue || filter.TargetSchemeId.Value == 0))
        {
            _logger?.LogWarning("⚠️ WhereHasAncestor filter found, but TargetSchemeId not set (type not registered in AutomaticTypeRegistry). Optimization skipped.");
            return null;
        }
        
        return filter;
    }

    /// <summary>
    /// Determines if WhereHasDescendant can be optimized through logic inversion
    /// </summary>
    protected TreeFilter? GetOptimizableHasDescendantFilter<TProps>(TreeQueryContext<TProps> context) 
        where TProps : class, new()
    {
        return context.TreeFilters?.FirstOrDefault(f => 
            f.Operator == TreeFilterOperator.HasDescendant && 
            f.TargetSchemeId.HasValue);
    }

    /// <summary>
    /// Optimized execution of WhereHasAncestor through logic inversion:
    /// 1. Find ancestors with condition
    /// 2. Find their descendants (using ParentIds)
    /// 3. Apply remaining filters
    /// </summary>
    protected async Task<List<TreeRedbObject<TProps>>> ExecuteOptimizedWhereHasAncestor<TProps>(
        TreeQueryContext<TProps> context,
        TreeFilter hasAncestorFilter) where TProps : class, new()
    {
        _logger?.LogInformation("   🔍 [DETAILED ANALYSIS] OPTION A - Step by step:");
        
        var swTotal = System.Diagnostics.Stopwatch.StartNew();
        
        // Step 1: Find matching ancestors
        var sw1 = System.Diagnostics.Stopwatch.StartNew();
        var ancestorConditionJson = JsonSerializer.Serialize(
            hasAncestorFilter.FilterConditions,
            new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
        sw1.Stop();
        _logger?.LogInformation($"      ⏱️ A.1 - Condition serialization: {sw1.ElapsedMilliseconds} ms");
        
        // OPTIMIZATION: ALWAYS use _base functions for fast search
        string ancestorsSql;
        List<string> ancestorIdStrings;
        
        // If RootObjectId exists - find ancestors as children of root
        if (context.RootObjectId.HasValue)
        {
            var treeFunctionName = _sql.Query_SearchTreeObjectsBaseFunction();
            
            // Use dialect-specific SQL for HasAncestor tree search
            ancestorsSql = _sql.Query_HasAncestorTreeSql(treeFunctionName);
            
            ancestorIdStrings = await _context.ExecuteJsonListAsync(ancestorsSql,
                    hasAncestorFilter.TargetSchemeId.Value,
                    new[] { context.RootObjectId.Value },  // ✅ BATCH: Array with single ID
                    ancestorConditionJson,
                    context.MaxDepth ?? 1000);
        }
        else
        {
            // Find ancestors across entire scheme (without parent restriction)
            var functionName = _sql.Query_SearchObjectsBaseFunction();
            
            // Use dialect-specific SQL for HasAncestor normal search
            ancestorsSql = _sql.Query_HasAncestorNormalSql(functionName);
            
            ancestorIdStrings = await _context.ExecuteJsonListAsync(ancestorsSql,
                    hasAncestorFilter.TargetSchemeId.Value,
                    ancestorConditionJson);
        }
        
        // _logger?.LogInformation($"   🔍 DEBUG: Received {ancestorIdStrings.Count} rows from SQL");
        // foreach (var idStr in ancestorIdStrings.Take(5))
        // {
        //     _logger?.LogInformation($"      - ID string: '{idStr}'");
        // }
        
        var ancestorIds = ancestorIdStrings
            .Where(s => !string.IsNullOrEmpty(s))
            .Select(s => long.Parse(s))
            .ToArray();
        
        var sw2Elapsed = swTotal.ElapsedMilliseconds;
        _logger?.LogInformation($"      ⏱️ A.2 - SQL ancestor search: {sw2Elapsed - sw1.ElapsedMilliseconds} ms");
        _logger?.LogInformation($"      ✅ A.2 - Found {ancestorIds.Length} ancestors");
        
        if (!ancestorIds.Any())
        {
            return new List<TreeRedbObject<TProps>>();
        }
        
        // Step 2: Create optimized context
        var sw2 = System.Diagnostics.Stopwatch.StartNew();
        var optimizedContext = context.Clone();
        optimizedContext.TreeFilters = new List<TreeFilter>(
            context.TreeFilters.Where(f => f != hasAncestorFilter));
        optimizedContext.ParentIds = ancestorIds;
        
        sw2.Stop();
        _logger?.LogInformation($"      ⏱️ A.3 - Context creation: {sw2.ElapsedMilliseconds} ms");
        
        // Step 3: Execute with remaining filters (preserves Where, OrderBy, Limit/Offset)
        var sw3 = System.Diagnostics.Stopwatch.StartNew();
        var result = (List<TreeRedbObject<TProps>>)await ExecuteTreeToListAsync(optimizedContext);
        sw3.Stop();
        
        swTotal.Stop();
        _logger?.LogInformation($"      ⏱️ A.4 - Loading descendants: {sw3.ElapsedMilliseconds} ms ({result.Count} objects)");
        _logger?.LogInformation($"      ⏱️ A - TOTAL TIME: {swTotal.ElapsedMilliseconds} ms");
        
        return result;
    }

    /// <summary>
    /// Optimized execution of WhereHasDescendant through logic inversion:
    /// 1. Find descendants with condition
    /// 2. Get their parents (via Parent/Ancestor chain)
    /// 3. Apply remaining filters
    /// </summary>
    protected async Task<List<TreeRedbObject<TProps>>> ExecuteOptimizedWhereHasDescendant<TProps>(
        TreeQueryContext<TProps> context,
        TreeFilter hasDescendantFilter) where TProps : class, new()
    {
        _logger?.LogInformation("🚀 OPTIMIZATION: Applying logic inversion for WhereHasDescendant");
        
        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        // Step 1: Find matching descendants
        var descendantConditionJson = JsonSerializer.Serialize(
            hasDescendantFilter.FilterConditions,
            new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
        
        // OPTIMIZATION: ALWAYS use _base function for fast search
        var functionName = _sql.Query_SearchObjectsBaseFunction();
        
        // Use dialect-specific SQL for HasDescendant search
        var descendantsSql = _sql.Query_HasDescendantSql(functionName);
        
        var descendantIdStrings = await _context.ExecuteJsonListAsync(descendantsSql,
                hasDescendantFilter.TargetSchemeId.Value,
                descendantConditionJson);
        
        var descendantIds = descendantIdStrings
            .Where(s => !string.IsNullOrEmpty(s))
            .Select(s => long.Parse(s))
            .ToList();
        
        sw.Stop();
        _logger?.LogInformation($"   ⏱️  Step 1: Found {descendantIds.Count} descendants in {sw.ElapsedMilliseconds} ms");
        
        if (!descendantIds.Any())
        {
            _logger?.LogInformation("   ⚠️  Descendants not found, returning empty result");
            return new List<TreeRedbObject<TProps>>();
        }
        
        // Step 2: Get unique ancestors of all found descendants
        sw.Restart();
        var parentIds = await GetParentIdsFromDescendants(descendantIds, hasDescendantFilter.MaxDepth);
        sw.Stop();
        
        _logger?.LogInformation($"   ⏱️  Step 2: Found {parentIds.Length} unique ancestors in {sw.ElapsedMilliseconds} ms");
        
        if (!parentIds.Any())
        {
            _logger?.LogInformation("   ⚠️  Ancestors not found, returning empty result");
            return new List<TreeRedbObject<TProps>>();
        }
        
        // Step 3: Load these objects with remaining filters
        sw.Restart();
        var result = await LoadObjectsByIdsWithFilters(parentIds, context);
        sw.Stop();
        
        _logger?.LogInformation($"   ⏱️  Step 3: Loaded {result.Count} objects in {sw.ElapsedMilliseconds} ms");
        _logger?.LogInformation($"✅ WhereHasDescendant optimization completed successfully");
        
        return result;
    }

    /// <summary>
    /// Get parent IDs for found descendants considering depth
    /// </summary>
    private async Task<long[]> GetParentIdsFromDescendants(List<long> descendantIds, int? maxDepth)
    {
        if (!descendantIds.Any())
            return Array.Empty<long>();
        
        var idsString = string.Join(",", descendantIds);
        var depthLimit = maxDepth ?? 50;
        
        // Use dialect-specific SQL for recursive CTE (PostgreSQL: WITH RECURSIVE, MSSQL: WITH)
        var sql = _sql.Query_GetParentIdsFromDescendantsSql(idsString, depthLimit);
        
        var parentIds = await _context.QueryScalarListAsync<long>(sql);
        
        return parentIds.ToArray();
    }

    /// <summary>
    /// Load objects by ID with remaining filters applied
    /// </summary>
    private async Task<List<TreeRedbObject<TProps>>> LoadObjectsByIdsWithFilters<TProps>(
        long[] objectIds,
        TreeQueryContext<TProps> context) where TProps : class, new()
    {
        if (!objectIds.Any())
            return new List<TreeRedbObject<TProps>>();
        
        // Create WhereIn filter by ID
        // ✅ FIX: IsBaseField = true to search in _objects._id instead of EAV field
        var idsFilter = new InExpression(
            new redb.Core.Query.QueryExpressions.PropertyInfo("_id", typeof(long), true),
            objectIds.Cast<object>().ToList());
        
        // Create new context with ID filter + existing filters
        var optimizedContext = context.Clone();
        optimizedContext.TreeFilters = new List<TreeFilter>(
            context.TreeFilters.Where(f => f.Operator != TreeFilterOperator.HasDescendant));
        
        // Add ID filter to existing Filter
        if (optimizedContext.Filter != null)
        {
            optimizedContext.Filter = new LogicalExpression(
                LogicalOperator.And,
                new FilterExpression[] { optimizedContext.Filter, idsFilter });
        }
        else
        {
            optimizedContext.Filter = idsFilter;
        }
        
        // Execute query through regular path
        return (List<TreeRedbObject<TProps>>)await ExecuteTreeToListAsync(optimizedContext);
    }

    /// <summary>
    /// Add tree filter to JSON dictionary
    /// </summary>
    private void AddTreeFilterToJson(Dictionary<string, object> filters, TreeFilter treeFilter)
    {
        switch (treeFilter.Operator)
        {
            case TreeFilterOperator.HasAncestor:
                // Polymorphic filter by ancestors with scheme_id and maxDepth support
                var hasAncestorFilter = new Dictionary<string, object>();
                
                // Add filtering condition
                if (treeFilter.FilterConditions != null)
                {
                    hasAncestorFilter["condition"] = treeFilter.FilterConditions;
                }
                
                // Add scheme_id for polymorphic queries
                if (treeFilter.TargetSchemeId.HasValue)
                {
                    hasAncestorFilter["scheme_id"] = treeFilter.TargetSchemeId.Value;
                }
                
                // Add depth limit
                if (treeFilter.MaxDepth.HasValue)
                {
                    hasAncestorFilter["max_depth"] = treeFilter.MaxDepth.Value;
                }
                
                filters["$hasAncestor"] = hasAncestorFilter;
                break;
            
            case TreeFilterOperator.HasDescendant:
                // Polymorphic filter by descendants with scheme_id and maxDepth support
                var hasDescendantFilter = new Dictionary<string, object>();
                
                // Add filtering condition
                if (treeFilter.FilterConditions != null)
                {
                    hasDescendantFilter["condition"] = treeFilter.FilterConditions;
                }
                
                // Add scheme_id for polymorphic queries
                if (treeFilter.TargetSchemeId.HasValue)
                {
                    hasDescendantFilter["scheme_id"] = treeFilter.TargetSchemeId.Value;
                }
                
                // Add depth limit
                if (treeFilter.MaxDepth.HasValue)
                {
                    hasDescendantFilter["max_depth"] = treeFilter.MaxDepth.Value;
                }
                
                filters["$hasDescendant"] = hasDescendantFilter;
                break;
            
            case TreeFilterOperator.Level:
                if (treeFilter.Value is int level)
                {
                    filters["$level"] = level;
                }
                else if (treeFilter.FilterConditions != null)
                {
                    filters["$level"] = treeFilter.FilterConditions;
                }
                break;
            
            case TreeFilterOperator.IsRoot:
                filters["$isRoot"] = true;
                break;
            
            case TreeFilterOperator.IsLeaf:
                filters["$isLeaf"] = true;
                break;

            case TreeFilterOperator.ChildrenOf:
                filters["$childrenOf"] = treeFilter.Value;
                break;

            case TreeFilterOperator.DescendantsOf:
                filters["$descendantsOf"] = new { 
                    ancestor_id = treeFilter.Value,
                    max_depth = treeFilter.MaxDepth 
                };
                break;

            default:
                throw new NotSupportedException($"Tree operator {treeFilter.Operator} is not supported");
        }
    }



    /// <summary>
    /// Build JSON for sorting (similar to base provider)
    /// 🆕 FIXED: Uses _facetBuilder.BuildOrderBy to support 0$: prefix for base fields
    /// </summary>
    private JsonDocument? BuildOrderByFilter<TProps>(TreeQueryContext<TProps> context) where TProps : class, new()
    {
        if (context.Orderings == null || !context.Orderings.Any())
            return null;

        // 🆕 Use _facetBuilder.BuildOrderBy which correctly handles IsBaseField
        var jsonString = _facetBuilder.BuildOrderBy(context.Orderings);
        return JsonDocument.Parse(jsonString);
    }

    // 💀 ConvertFilterToFacets() REMOVED! REPLACED WITH PostgresFacetFilterBuilder!
    // Now Tree uses THE SAME POWERFUL SYSTEM as regular LINQ - ALL 25+ operators!

    /// <summary>
    /// Deserialize JSON result to list of TreeRedbObject
    /// </summary>
    private List<TreeRedbObject<TProps>> DeserializeTreeObjects<TProps>(string? objectsJson) where TProps : class, new()
    {
        if (string.IsNullOrEmpty(objectsJson) || objectsJson == "null")
            return new List<TreeRedbObject<TProps>>();

        try
        {
            // Handle both array format and object format {"objects":[...]}
            JsonElement[]? jsonArray;
            var trimmed = objectsJson.TrimStart();
            if (trimmed.StartsWith("{"))
            {
                // MSSQL returns full object: {"objects":[...],"total_count":N,...}
                var doc = JsonDocument.Parse(objectsJson);
                if (doc.RootElement.TryGetProperty("objects", out var objectsElement))
                {
                    jsonArray = JsonSerializer.Deserialize<JsonElement[]>(objectsElement.GetRawText());
                }
                else
                {
                    return new List<TreeRedbObject<TProps>>();
                }
            }
            else
            {
                // PostgreSQL returns array directly: [{...},{...}]
                jsonArray = JsonSerializer.Deserialize<JsonElement[]>(objectsJson);
            }
            var result = new List<TreeRedbObject<TProps>>();

            if (jsonArray == null) return result;

            foreach (var jsonElement in jsonArray)
            {
                try
                {
                    // Deserialize as regular RedbObject
                    var redbObj = _serializer.Deserialize<TProps>(jsonElement.GetRawText());
                    if (redbObj == null) continue;

                    // Convert to TreeRedbObject
                    var treeObj = ConvertToTreeObject(redbObj);
                    
                    // NOTE: _lazyLoader setup happens later in ExecuteTreeToListAsync
                    // after DeserializeTreeObjects call, when we have access to context
                    
                    result.Add(treeObj);
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Error deserializing tree object from JSON");
                    // Continue processing other objects
                }
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error deserializing JSON array of tree objects");
            return new List<TreeRedbObject<TProps>>();
        }
    }

    /// <summary>
    /// Convert RedbObject to TreeRedbObject
    /// ✅ FIXED: Props copied from source (fixed bug with empty Props)
    /// </summary>
    private TreeRedbObject<TProps> ConvertToTreeObject<TProps>(RedbObject<TProps> source) where TProps : class, new()
    {
        var treeObj = new TreeRedbObject<TProps>
        {
            id = source.id,
            parent_id = source.parent_id,
            scheme_id = source.scheme_id,
            owner_id = source.owner_id,
            who_change_id = source.who_change_id,
            date_create = source.date_create,
            date_modify = source.date_modify,
            date_begin = source.date_begin,
            date_complete = source.date_complete,
            key = source.key,
            value_long = source.value_long,
            value_string = source.value_string,
            value_guid = source.value_guid,
            value_bool = source.value_bool,
            value_double = source.value_double,
            value_numeric = source.value_numeric,
            value_datetime = source.value_datetime,
            value_bytes = source.value_bytes,
            name = source.name,
            note = source.note,
            hash = source.hash
        };
        
        // ✅ CRITICAL FIX: Copy Props directly
        // Use GetPropsDirectly() to NOT trigger lazy load in source
        // Props setter is safe - it just sets the value
        var sourceProps = source.GetPropsDirectly();
        if (sourceProps != null)
        {
            treeObj.Props = sourceProps;  // Setter will set _propsLoaded=true
        }
        else if (source._propsLoaded)
        {
            // Props was loaded but it's null - set flag
            treeObj._propsLoaded = true;
        }
        
        return treeObj;
    }

    // ===== METHODS FOR BUILDING HIERARCHICAL RELATIONSHIPS =====

    /// <summary>
    /// Get IDs of all objects and their parents up to root through recursive CTE
    /// </summary>
    public async Task<List<long>> GetIdsWithAncestorsAsync<TProps>(List<long> filteredIds) where TProps : class, new()
    {
        if (filteredIds == null || !filteredIds.Any())
            return new List<long>();

        try
        {
            // Use dialect-specific SQL for recursive CTE
            var idsString = string.Join(",", filteredIds);
            var sql = _sql.Query_GetIdsWithAncestorsSql(idsString);

            var idsResult = await _context.QueryScalarListAsync<long>(sql);

            return idsResult;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting IDs with parents");
            throw;
        }
    }

    /// <summary>
    /// Load full objects by ID list via get_object_json function
    /// </summary>
    public async Task<List<TreeRedbObject<TProps>>> LoadObjectsByIdsAsync<TProps>(List<long> objectIds, int? propsDepth = null) where TProps : class, new()
    {
        if (objectIds == null || !objectIds.Any())
            return new List<TreeRedbObject<TProps>>();

        try
        {
            // Use dialect-specific SQL for loading objects by IDs
            var idsString = string.Join(",", objectIds);
            var effectiveDepth = propsDepth ?? _configuration.DefaultMaxTreeDepth;
            var sql = _sql.Query_LoadObjectsByIdsSql(idsString, effectiveDepth);

            var objectsJsonList = await _context.ExecuteJsonListAsync(sql);

            var result = new List<TreeRedbObject<TProps>>();

            foreach (var objectJson in objectsJsonList)
            {
                try
                {
                    var redbObj = _serializer.Deserialize<TProps>(objectJson);
                    if (redbObj != null)
                    {
                        var treeObj = ConvertToTreeObject(redbObj);
                        result.Add(treeObj);
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Error deserializing object from get_object_json");
                }
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error loading objects by ID");
            throw;
        }
    }

    /// <summary>
    /// Load full polymorphic objects by ID list via get_object_json function.
    /// Used for polymorphic trees where objects can be of different types.
    /// Each object is deserialized to its real type based on scheme_id.
    /// </summary>
    public async Task<List<ITreeRedbObject>> LoadObjectsByIdsAsync(List<long> objectIds, int? propsDepth = null)
    {
        if (objectIds == null || !objectIds.Any())
            return new List<ITreeRedbObject>();

        try
        {
            // Use dialect-specific SQL for loading objects by IDs
            var idsString = string.Join(",", objectIds);
            var effectiveDepth = propsDepth ?? _configuration.DefaultMaxTreeDepth;
            var sql = _sql.Query_LoadObjectsByIdsSql(idsString, effectiveDepth);

            var objectsJsonList = await _context.ExecuteJsonListAsync(sql);


            var result = new List<ITreeRedbObject>();

            foreach (var objectJson in objectsJsonList)
            {
                // 1. Extract scheme_id from JSON for polymorphic deserialization
                using var jsonDoc = JsonDocument.Parse(objectJson);
                var schemeId = jsonDoc.RootElement.GetProperty("scheme_id").GetInt64();
                
                // 2. Get real C# type via AutomaticTypeRegistry
                var propsType = Cache.GetClrType(schemeId)
                    ?? throw new InvalidOperationException(
                        $"Type not found for scheme_id={schemeId}. Register type in AutomaticTypeRegistry.");
                
                // 3. Polymorphic deserialization: first RedbObject, then TreeRedbObject
                var redbObj = _serializer.DeserializeRedbDynamic(objectJson, propsType);
                
                if (redbObj != null)
                {
                    // Convert to TreeRedbObject via reflection (call ConvertToTreeObject<TProps>)
                    var method = typeof(TreeQueryProviderBase).GetMethod(nameof(ConvertToTreeObject), 
                        System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                    
                    if (method != null)
                    {
                        var genericMethod = method.MakeGenericMethod(propsType);
                        var convertResult = genericMethod.Invoke(this, new[] { redbObj });
                        if (convertResult is ITreeRedbObject treeObj)
                        {
                            result.Add(treeObj);
                        }
                    }
                }
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error loading polymorphic objects by ID");
            throw;
        }
    }

    /// <summary>
    /// Returns SQL query for tree search that will be executed (for debugging)
    /// </summary>
    public virtual async Task<string> GetSqlPreviewAsync<TProps>(TreeQueryContext<TProps> context) 
        where TProps : class, new()
    {
        var facetFilters = _facetBuilder.BuildFacetFilters(context.Filter);
        var parameters = _facetBuilder.BuildQueryParameters(context.Limit, context.Offset);
        var orderByDoc = BuildOrderByFilter(context);
        var orderByJson = orderByDoc?.RootElement.GetRawText();
        
        // OPTIMIZATION: ALWAYS use _base preview function
        var functionName = _sql.Query_TreeSqlPreviewBaseFunction();
        
        _logger?.LogDebug("🔍 Getting Tree SQL Preview: Function={FunctionName}, SchemeId={SchemeId}, RootObjectId={RootObjectId}, MaxDepth={MaxDepth}", 
            functionName, context.SchemeId, context.RootObjectId, context.MaxDepth);
        
        // Build parent_ids array for the SQL function
        var parentIds = context.ParentIds ?? (context.RootObjectId.HasValue ? new[] { context.RootObjectId.Value } : Array.Empty<long>());
        
        var sqlQuery = _sql.Query_TreeSqlPreviewTemplate(functionName);
        var result = await _context.QueryFirstOrDefaultAsync<SqlPreviewResult>(
            sqlQuery,
            context.SchemeId,
            parentIds,
            facetFilters, 
            parameters.Limit ?? int.MaxValue,
            parameters.Offset ?? 0,
            orderByJson ?? "null",
            context.MaxDepth ?? 100,
            context.MaxRecursionDepth ?? 10);
        
        return result?.sql_preview ?? "-- Tree SQL preview not available";
    }
    
    // ===== DELETE =====
    
    /// <summary>
    /// Get SQL preview for standard QueryContext (for IRedbQueryProvider compatibility)
    /// </summary>
    public Task<string> GetSqlPreviewAsync<TProps>(QueryContext<TProps> context) 
        where TProps : class, new()
    {
        // For Tree provider convert to TreeQueryContext
        if (context is TreeQueryContext<TProps> treeContext)
        {
            return GetSqlPreviewAsync(treeContext);
        }
        
        // Create TreeQueryContext from regular QueryContext
        var newTreeContext = new TreeQueryContext<TProps>(
            context.SchemeId, 
            context.UserId, 
            context.CheckPermissions, 
            null, // rootObjectId
            context.MaxDepth)
        {
            Filter = context.Filter,
            Orderings = context.Orderings.ToList(),
            Limit = context.Limit,
            Offset = context.Offset,
            UseLazyLoading = context.UseLazyLoading
        };
        
        return GetSqlPreviewAsync(newTreeContext);
    }
    
    /// <summary>
    /// Returns the JSON filter that will be sent to SQL function (for diagnostics)
    /// </summary>
    public Task<string> GetFilterJsonAsync<TProps>(QueryContext<TProps> context) 
        where TProps : class, new()
    {
        var facetFilters = _facetBuilder.BuildFacetFilters(context.Filter);
        return Task.FromResult(facetFilters);
    }
    
    /// <summary>
    /// Delete objects by filter. Delegates to base QueryProvider.
    /// Cascade delete in DB handles children automatically.
    /// </summary>
    public async Task<int> ExecuteDeleteAsync(long schemeId, string? filterJson = null)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteDeleteAsync(schemeId, filterJson);
    }
    
    /// <summary>
    /// Delete objects by FilterExpression. Delegates to base QueryProvider.
    /// Cascade delete in DB handles children automatically.
    /// </summary>
    public async Task<int> ExecuteDeleteAsync(long schemeId, QueryExpressions.FilterExpression? filter)
    {
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteDeleteAsync(schemeId, filter);
    }
    
    /// <summary>
    /// Delete objects by ID array (cascade with children).
    /// </summary>
    public async Task<int> ExecuteTreeDeleteAsync(long[] objectIds)
    {
        if (objectIds.Length == 0)
            return 0;
        
        // Use dialect's cascade delete (handles children automatically)
        // MSSQL context auto-converts long[] to comma-separated string
        var deletedCount = await _context.ExecuteAsync(
            _sql.ObjectStorage_DeleteByIds(), objectIds);
        
        _logger?.LogDebug("TreeDelete: Deleted {Count} objects (cascade)", deletedCount);
        
        return deletedCount;
    }
    
    /// <summary>
    /// Execute GROUP BY with tree context (CTE for tree traversal).
    /// Base implementation: gets tree object IDs first, then delegates to base GroupBy with filter.
    /// Override in Pro version for optimized CTE-based implementation.
    /// </summary>
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteTreeGroupedAggregateAsync<TProps>(
        TreeQueryContext<TProps> context,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations) where TProps : class, new()
    {
        // Base implementation: get tree object IDs first, then filter grouping
        var treeObjectIds = await GetTreeObjectIdsAsync(context);
        
        if (treeObjectIds.Count == 0)
            return System.Text.Json.JsonDocument.Parse("[]");
        
        // Build filter JSON with object IDs
        var filterJson = BuildObjectIdsFilterJson(treeObjectIds);
        
        // Delegate to base grouped aggregate with ID filter
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteGroupedAggregateAsync(
            context.SchemeId, groupFields, aggregations, filterJson);
    }
    
    /// <summary>
    /// Gets object IDs matching tree query context.
    /// </summary>
    private async Task<List<long>> GetTreeObjectIdsAsync<TProps>(TreeQueryContext<TProps> context) 
        where TProps : class, new()
    {
        // Use existing tree query infrastructure to get matching IDs
        var treeQueryable = new TreeQueryableBase<TProps>(
            this, context.Clone(), _filterParser, _orderingParser, _facetBuilder);
        
        var objects = await treeQueryable.ToListAsync();
        return objects.Select(o => o.Id).ToList();
    }
    
    /// <summary>
    /// Builds filter JSON with object ID constraint.
    /// Uses 0$: prefix for base fields - required for PostgreSQL.
    /// </summary>
    private string BuildObjectIdsFilterJson(List<long> objectIds)
    {
        // Format: {"0$:id":{"$in":[1,2,3]}} - base fields require 0$: prefix!
        var idsJson = System.Text.Json.JsonSerializer.Serialize(objectIds);
        return $"{{\"0$:id\":{{\"$in\":{idsJson}}}}}";
    }
    
    /// <summary>
    /// Execute Window Functions with tree context.
    /// Base implementation: get tree object IDs first, then execute window functions with ID filter.
    /// Override in Pro version for optimized CTE-based implementation.
    /// </summary>
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteTreeWindowQueryAsync<TProps>(
        TreeQueryContext<TProps> context,
        IEnumerable<Window.WindowFieldRequest> selectFields,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? frameJson = null) where TProps : class, new()
    {
        // Base implementation: get tree object IDs first, then filter window query
        var treeObjectIds = await GetTreeObjectIdsAsync(context);
        
        if (treeObjectIds.Count == 0)
        {
            return System.Text.Json.JsonDocument.Parse("[]");
        }
        
        // Build filter JSON with object IDs
        var filterJson = BuildObjectIdsFilterJson(treeObjectIds);
        
        // Delegate to base window query with ID filter
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteWindowQueryAsync(
            context.SchemeId, selectFields, windowFuncs, partitionBy, orderBy, filterJson, frameJson, 
            context.Limit, context.Offset);
    }
    
    /// <summary>
    /// Get SQL preview for tree window query.
    /// Base implementation: returns placeholder, override in Pro for actual SQL.
    /// </summary>
    public virtual Task<string> GetTreeWindowSqlPreviewAsync<TProps>(
        TreeQueryContext<TProps> context,
        IEnumerable<Window.WindowFieldRequest> selectFields,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy,
        string? frameJson = null) where TProps : class, new()
    {
        return Task.FromResult($"-- Tree Window SQL Preview not available in Open Source version\n-- SchemeId: {context.SchemeId}\n-- RootObjectId: {context.RootObjectId}\n-- Use Pro version for SQL preview");
    }
    
    /// <summary>
    /// Get SQL preview for tree GROUP BY query.
    /// Base implementation: returns placeholder, override in Pro for actual SQL.
    /// </summary>
    public virtual Task<string> GetTreeGroupBySqlPreviewAsync<TProps>(
        TreeQueryContext<TProps> context,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations) where TProps : class, new()
    {
        return Task.FromResult(
            $"-- Tree GroupBy SQL Preview not available in Open Source version\n" +
            $"-- SchemeId: {context.SchemeId}\n" +
            $"-- RootObjectId: {context.RootObjectId}\n" +
            $"-- GroupFields: {string.Join(", ", groupFields.Select(g => g.FieldPath))}\n" +
            $"-- Aggregations: {string.Join(", ", aggregations.Select(a => $"{a.Function}({a.FieldPath})"))}\n" +
            $"-- Use Pro version for SQL preview");
    }
    
    // ===== TREE GROUPED WINDOW =====
    
    /// <summary>
    /// Execute GroupBy + Window with tree context.
    /// Base implementation: gets tree object IDs first, then delegates to base GroupBy + Window with filter.
    /// Override in Pro version for optimized CTE-based implementation.
    /// </summary>
    public virtual async Task<System.Text.Json.JsonDocument?> ExecuteTreeGroupedWindowQueryAsync<TProps>(
        TreeQueryContext<TProps> context,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy) where TProps : class, new()
    {
        // Base implementation: get tree object IDs first, then filter grouping + window
        var treeObjectIds = await GetTreeObjectIdsAsync(context);
        
        if (treeObjectIds.Count == 0)
            return System.Text.Json.JsonDocument.Parse("[]");
        
        // Build filter JSON with object IDs
        var filterJson = BuildObjectIdsFilterJson(treeObjectIds);
        
        // Delegate to base grouped window query with ID filter
        var baseProvider = CreateQueryProvider();
        return await baseProvider.ExecuteGroupedWindowQueryAsync(
            context.SchemeId, groupFields, aggregations, windowFuncs, partitionBy, orderBy, filterJson);
    }
    
    /// <summary>
    /// Get SQL preview for tree GroupBy + Window.
    /// Base implementation: returns placeholder.
    /// </summary>
    public virtual Task<string> GetTreeGroupedWindowSqlPreviewAsync<TProps>(
        TreeQueryContext<TProps> context,
        IEnumerable<Grouping.GroupFieldRequest> groupFields,
        IEnumerable<Aggregation.AggregateRequest> aggregations,
        IEnumerable<Window.WindowFuncRequest> windowFuncs,
        IEnumerable<Window.WindowFieldRequest> partitionBy,
        IEnumerable<Window.WindowOrderRequest> orderBy) where TProps : class, new()
    {
        return Task.FromResult(
            $"-- Tree GroupBy + Window SQL Preview not available in Open Source version\n" +
            $"-- SchemeId: {context.SchemeId}\n" +
            $"-- RootObjectId: {context.RootObjectId}\n" +
            $"-- GroupFields: {string.Join(", ", groupFields.Select(g => g.FieldPath))}\n" +
            $"-- Aggregations: {string.Join(", ", aggregations.Select(a => $"{a.Function}({a.FieldPath})"))}\n" +
            $"-- WindowFuncs: {string.Join(", ", windowFuncs.Select(w => w.Func))}\n" +
            $"-- Use Pro version for SQL preview");
    }
}

// ===== HELPER CLASSES =====

/// <summary>
/// Tree query context - extends QueryContext with tree parameters support
/// </summary>
public class TreeQueryContext<TProps> : QueryContext<TProps> where TProps : class, new()
{
    public long? RootObjectId { get; set; }               // Limit search to subtree
    public List<TreeFilter> TreeFilters { get; set; }     // Tree filters
    
    // ✅ FIX: MaxDepth now inherited from base QueryContext

    public TreeQueryContext(long schemeId, long? userId, bool checkPermissions, long? rootObjectId, int? maxDepth) 
        : base(schemeId, userId, checkPermissions, null, maxDepth)  // ✅ Pass maxDepth to base constructor
    {
        RootObjectId = rootObjectId;
        TreeFilters = new List<TreeFilter>();
    }

    /// <summary>
    /// Create a copy of tree context
    /// </summary>
    public new TreeQueryContext<TProps> Clone()
    {
        // ✅ FIX: MaxDepth now passed via base constructor
        var clone = new TreeQueryContext<TProps>(SchemeId, UserId, CheckPermissions, RootObjectId, MaxDepth)
        {
            ParentIds = ParentIds,      // ✅ SYNC: copy batch array
            Filter = Filter,
            Orderings = new List<OrderingExpression>(Orderings),
            Limit = Limit,
            Offset = Offset,
            IsDistinct = IsDistinct,
            IsDistinctRedb = IsDistinctRedb,
            DistinctByField = DistinctByField,
            DistinctByIsBaseField = DistinctByIsBaseField,
            MaxRecursionDepth = MaxRecursionDepth,
            IsEmpty = IsEmpty,          // ✅ FIX: copy IsEmpty flag for TreeQueryContext
            UseLazyLoading = UseLazyLoading,           // ✅ copy lazy loading flag
            ProjectedStructureIds = ProjectedStructureIds,
            ProjectedFieldPaths = ProjectedFieldPaths,
            SkipPropsLoading = SkipPropsLoading,
            PropsDepth = PropsDepth
        };
        
        // Copy tree filters
        clone.TreeFilters = new List<TreeFilter>(TreeFilters);
        return clone;
    }
}

/// <summary>
/// Tree filter - representation of hierarchical operator
/// </summary>
public class TreeFilter
{
    public TreeFilterOperator Operator { get; set; }
    public object? Value { get; set; }
    public int? MaxDepth { get; set; }
    public long? TargetSchemeId { get; set; }  // For polymorphic queries (WhereHasAncestor&lt;TTarget&gt;)
    public Dictionary<string, object>? FilterConditions { get; set; }  // For Open Source: JSON for SQL functions
    public FilterExpression? OriginalFilter { get; set; }  // For Pro: original FilterExpression

    public TreeFilter(TreeFilterOperator op, object? value = null)
    {
        Operator = op;
        Value = value;
        FilterConditions = new Dictionary<string, object>();
    }
}

/// <summary>
/// Tree operator types (correspond to SQL operators from search_tree_objects_with_facets)
/// </summary>
public enum TreeFilterOperator
{
    HasAncestor,      // $hasAncestor - find objects with ancestor matching condition
    HasDescendant,    // $hasDescendant - find objects with descendant matching condition  
    Level,            // $level - filter by level in tree
    IsRoot,           // $isRoot - only root objects
    IsLeaf,           // $isLeaf - only leaves
    ChildrenOf,       // $childrenOf - direct children of object
    DescendantsOf     // $descendantsOf - all descendants of object
}
