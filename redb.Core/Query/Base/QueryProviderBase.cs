using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Core.Query.Aggregation;
using redb.Core.Query.FacetFilters;
using redb.Core.Query.Parsing;
using redb.Core.Query.QueryExpressions;
using redb.Core.Serialization;

namespace redb.Core.Query.Base;

/// <summary>
/// Base query provider for executing LINQ queries via search_objects_with_facets.
/// Database-specific implementations should inherit and provide ISqlDialect.
/// </summary>
public abstract partial class QueryProviderBase : IRedbQueryProvider
{
    protected readonly IRedbContext _context;
    protected readonly IRedbObjectSerializer _serializer;
    protected readonly IFilterExpressionParser _filterParser;
    protected readonly IOrderingExpressionParser _orderingParser;
    protected readonly IFacetFilterBuilder _facetBuilder;
    protected readonly ILogger? _logger;
    protected readonly ILazyPropsLoader? _lazyPropsLoader;
    protected readonly RedbServiceConfiguration _configuration;
    protected readonly ISchemeSyncProvider? _schemeSync;
    protected readonly ISqlDialect _sql;

    protected QueryProviderBase(
        IRedbContext context,
        IRedbObjectSerializer serializer,
        ISqlDialect dialect,
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
        _filterParser = CreateFilterParser();
        _orderingParser = CreateOrderingParser();
        _facetBuilder = CreateFacetBuilder();
    }
    
    /// <summary>
    /// Creates filter expression parser. Override for Pro features.
    /// </summary>
    protected virtual IFilterExpressionParser CreateFilterParser() => new FilterExpressionParser();
    
    /// <summary>
    /// Creates ordering expression parser.
    /// </summary>
    protected virtual IOrderingExpressionParser CreateOrderingParser() => new OrderingExpressionParser();
    
    /// <summary>
    /// Creates facet filter builder.
    /// </summary>
    protected virtual IFacetFilterBuilder CreateFacetBuilder() => new FacetFilterBuilder(_logger);
    
    /// <summary>
    /// Determines if lazy loading should be used for this query
    /// </summary>
    protected bool ShouldUseLazyLoading<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        // If explicitly specified in the context - use that value (priority)
        if (context.UseLazyLoading.HasValue)
            return context.UseLazyLoading.Value && _lazyPropsLoader != null;
            
        // Global setting disabled - return false immediately
        if (!_configuration.EnableLazyLoadingForProps)
            return false;
            
        // If lazy loader is not available - return false
        return _lazyPropsLoader != null;
    }

    public IRedbQueryable<TProps> CreateQuery<TProps>(long schemeId, long? userId = null, bool checkPermissions = false) 
        where TProps : class, new()
    {
        var context = new QueryContext<TProps>(schemeId, userId, checkPermissions);
        return new RedbQueryable<TProps>(this, context, _filterParser, _orderingParser, _facetBuilder);
    }
    
    /// <summary>
    /// Hook for Distinct-related feature checks. No-op in OSS — DistinctRedb /
    /// DistinctBy are evaluated by SQL builders directly and either work or
    /// surface a concrete error from the dialect.
    /// </summary>
    protected virtual void CheckProOnlyDistinctFeatures<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
    }
    
    /// <summary>
    /// ⭐ Get scheme by ID (for projections)
    /// </summary>
    public async Task<IRedbScheme?> GetSchemeAsync(long schemeId)
    {
        if (_schemeSync != null)
        {
            return await _schemeSync.GetSchemeByIdAsync(schemeId);
        }
        return null;
    }

    public async Task<object> ExecuteAsync(Expression expression, Type elementType)
    {
        // Extract QueryContext from the expression
        if (expression is ConstantExpression constantExpr && constantExpr.Value != null)
        {
            // Determine operation type by elementType
            if (elementType == typeof(int))
            {
                return await ExecuteCountAsyncGeneric(constantExpr.Value);
            }
            else if (elementType.IsGenericType && elementType.GetGenericTypeDefinition() == typeof(List<>))
            {
                return await ExecuteToListAsyncGeneric(constantExpr.Value);
            }
        }

        throw new NotSupportedException($"Expression type {expression.GetType().Name} with element type {elementType.Name} is not supported");
    }

    private async Task<int> ExecuteCountAsyncGeneric(object contextObj)
    {
        // Use reflection to call the generic method
        var contextType = contextObj.GetType();
        if (contextType.IsGenericType && contextType.GetGenericTypeDefinition() == typeof(QueryContext<>))
        {
            var propsType = contextType.GetGenericArguments()[0];
            // ⭐ Look for the method in the ACTUAL type (to support inheritance in Pro)
            var method = GetType().GetMethod(nameof(ExecuteCountAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var genericMethod = method!.MakeGenericMethod(propsType);
            var task = (Task<int>)genericMethod.Invoke(this, new[] { contextObj })!;
            return await task;
        }
        
        throw new NotSupportedException($"Unsupported context type: {contextType.Name}");
    }

    private async Task<object> ExecuteToListAsyncGeneric(object contextObj)
    {
        // Use reflection to call the generic method
        var contextType = contextObj.GetType();
        if (contextType.IsGenericType && contextType.GetGenericTypeDefinition() == typeof(QueryContext<>))
        {
            var propsType = contextType.GetGenericArguments()[0];
            // ⭐ Look for the method in the ACTUAL type (to support inheritance in Pro)
            var method = GetType().GetMethod(nameof(ExecuteToListAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var genericMethod = method!.MakeGenericMethod(propsType);
            var task = (Task<object>)genericMethod.Invoke(this, new[] { contextObj, propsType })!;
            return await task;
        }
        
        throw new NotSupportedException($"Unsupported context type: {contextType.Name}");
    }

    /// <summary>
    /// Executes COUNT query. Override in Pro for PVT-based count with computed expressions.
    /// </summary>
    protected virtual async Task<int> ExecuteCountAsync<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        // v2-pvt path (free Postgres): build inner SQL via pvt_build_query_sql,
        // then COUNT(*) it on the client side. Dialect that returns null falls
        // through to the legacy search_objects_with_facets_base pipeline below.
        if (CanUsePvtPipeline(context))
        {
            _logger?.LogDebug("PVT Count Query: SchemeId={SchemeId}", context.SchemeId);
            return await ExecuteCountAsyncPvt(context);
        }

        var facetFilters = _facetBuilder.BuildFacetFilters(context.Filter);
        var orderByJson = BuildOrderByJson(context);
        
        // OPTIMIZATION: ALWAYS use _base function for COUNT (faster, no Props needed)
        var functionName = _sql.Query_SearchObjectsBaseFunction();

        _logger?.LogDebug("LINQ Count Query: SchemeId={SchemeId}, Filters={Filters}, OrderBy={OrderBy}", 
            context.SchemeId, facetFilters, orderByJson);

        var sqlWithParams = string.Format(_sql.Query_SearchTemplate(), functionName);
        var result = await _context.QueryFirstOrDefaultAsync<SearchJsonResult>(sqlWithParams, 
            context.SchemeId, facetFilters, 0, 0, orderByJson ?? "null", 
            context.MaxRecursionDepth ?? 10);

                if (result?.result != null)
        {
            var jsonDoc = System.Text.Json.JsonDocument.Parse(result.result);
            if (jsonDoc.RootElement.TryGetProperty("total_count", out var totalCountElement))
            {
                var count = totalCountElement.GetInt32();
                _logger?.LogDebug("LINQ Count Result: {Count} objects found", count);
                return count;
            }
        }
        
        _logger?.LogDebug("LINQ Count Result: No result returned, count = 0");
        return 0;
    }

    protected virtual async Task<object> ExecuteToListAsync<TProps>(QueryContext<TProps> context, Type propsType) where TProps : class, new()
    {
        // ⚠️ Pro-only features check
        CheckProOnlyDistinctFeatures(context);

        var hasFieldPathsEarly = context.ProjectedFieldPaths != null && context.ProjectedFieldPaths.Count > 0;

        // v2-pvt native projection path: when there are projected field paths
        // AND the dialect ships pvt_build_projection_sql. The outer SELECT
        // yields the requested scalar columns, wrapped into a JSON-row shape
        // that flows through the standard materializer.
        if (hasFieldPathsEarly && CanUsePvtProjection(context))
        {
            _logger?.LogDebug("PVT Projection ToList Query: SchemeId={SchemeId}, Paths={Paths}",
                context.SchemeId, string.Join(",", context.ProjectedFieldPaths!));
            return await ExecuteToListAsyncPvtProjection<TProps>(context);
        }

        // v2-pvt full-object path (free Postgres). Projection without native
        // pvt_build_projection_sql falls through to this branch — the compiled
        // selector then trims columns client-side from full RedbObject<T>s.
        if (CanUsePvtPipeline(context))
        {
            _logger?.LogDebug("PVT ToList Query: SchemeId={SchemeId}", context.SchemeId);
            return await ExecuteToListAsyncPvt<TProps>(context, propsType);
        }

        var facetFilters = _facetBuilder.BuildFacetFilters(context.Filter);
        var parameters = _facetBuilder.BuildQueryParameters(context.Limit, context.Offset);
        var orderByJson = BuildOrderByJson(context);

        // Choose function based on projection settings
        // OPTIMIZATION: We ALWAYS use _base function for search (fast, without Props)
        // Then load Props via LoadPropsForManyAsync with cache check
        var useLazyLoading = ShouldUseLazyLoading(context); // For logging only
        var hasFieldPaths = context.ProjectedFieldPaths != null && context.ProjectedFieldPaths.Count > 0;
        var hasStructureIds = context.ProjectedStructureIds != null && context.ProjectedStructureIds.Count > 0;
        
        // SQL function selection logic:
        // 1. Projection with paths (PRIORITY) → search_objects_with_projection_by_paths
        // 2. Projection with structure_ids (legacy) → search_objects_with_projection_by_ids
        // 3. ALL OTHER CASES → search_objects_with_facets_base (fast, Props loaded separately)
        string functionName;
        bool useProjectionByPaths = false;
        bool useProjectionByIds = false;
        
        if (hasFieldPaths)
        {
            functionName = _sql.Query_SearchObjectsProjectionByPathsFunction();
            useProjectionByPaths = true;
        }
        else if (hasStructureIds)
        {
            functionName = _sql.Query_SearchObjectsProjectionByIdsFunction();
            useProjectionByIds = true;
        }
        else
        {
            // OPTIMIZATION: ALWAYS use _base function for fast search (without Props JSON)
            // Props will be loaded separately via LoadPropsForManyAsync (with cache check)
            functionName = _sql.Query_SearchObjectsBaseFunction();
        }

        _logger?.LogDebug("LINQ ToList Query: SchemeId={SchemeId}, Filters={Filters}, Limit={Limit}, Offset={Offset}, OrderBy={OrderBy}, LazyLoading={LazyLoading}, ProjectionByPaths={ProjectionByPaths}, ProjectionByIds={ProjectionByIds}", 
            context.SchemeId, facetFilters, parameters.Limit?.ToString() ?? "NULL (all records)", parameters.Offset ?? 0, orderByJson, useLazyLoading, useProjectionByPaths, useProjectionByIds);

        // ⏱️ MEASURING SQL QUERY TIME
        var sqlTimer = System.Diagnostics.Stopwatch.StartNew();
        
        SearchJsonResult? result;
        
        if (useProjectionByPaths)
        {
            // Projection by text paths
            var sqlWithParams = _sql.Query_ProjectionByPathsTemplate();
            result = await _context.QueryFirstOrDefaultAsync<SearchJsonResult>(sqlWithParams, 
                context.SchemeId, 
                facetFilters, 
                context.ProjectedFieldPaths!.ToArray(),
                parameters.Limit ?? int.MaxValue,
                parameters.Offset ?? 0,
                orderByJson ?? "null",
                context.MaxRecursionDepth ?? 10);
        }
        else if (useProjectionByIds)
        {
            // Projection by structure IDs (legacy)
            var structureIdsArray = string.Join(",", context.ProjectedStructureIds!);
            var sqlWithParams2 = _sql.Query_ProjectionByIdsTemplate(structureIdsArray);
            result = await _context.QueryFirstOrDefaultAsync<SearchJsonResult>(sqlWithParams2, 
                context.SchemeId, 
                facetFilters, 
                parameters.Limit ?? int.MaxValue,
                parameters.Offset ?? 0,
                orderByJson ?? "null",
                context.MaxRecursionDepth ?? 10);
        }
        else
        {
            // OPTIMIZATION: ALWAYS use _base template (fast search without Props)
            // Props will be loaded separately via LoadPropsForManyAsync (with cache check)
            var sqlBase = string.Format(_sql.Query_SearchWithDistinctTemplate(), functionName);
            result = await _context.QueryFirstOrDefaultAsync<SearchJsonResult>(sqlBase, 
                context.SchemeId, 
                facetFilters, 
                parameters.Limit ?? int.MaxValue,
                parameters.Offset ?? 0,
                orderByJson ?? "null",
                context.MaxRecursionDepth ?? 10,
                context.IsDistinct);
        }
        
        sqlTimer.Stop();
        _logger?.LogInformation("⏱️  SQL query ({FunctionName}) executed in {ElapsedMs} ms", functionName, sqlTimer.ElapsedMilliseconds);

        if (result?.result != null)
        {
            _logger?.LogDebug("🔍 SQL RESPONSE: Received JSON with length {Length} characters", result.result.Length);
            _logger?.LogDebug("🔍 SQL JSON: {JsonContent}", result.result);
            
            var jsonDoc = System.Text.Json.JsonDocument.Parse(result.result);
            if (jsonDoc.RootElement.TryGetProperty("objects", out var objectsElement))
            {
                var objectsJson = objectsElement.GetRawText();
                _logger?.LogDebug("🔍 OBJECTS JSON: {ObjectsJson}", objectsJson);
                
                var objects = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement[]>(objectsJson);
                
                _logger?.LogDebug("📊 SQL RESULT: {Count} objects received from the database", objects?.Length ?? 0);
                
                // ⭐ PROJECTION: Convert flat paths to hierarchy ONLY for by_ids
                // by_paths already returns hierarchical JSON from SQL (uses build_hierarchical_properties_optimized)
                if (useProjectionByIds && !useProjectionByPaths && objects != null)
                {
                    objects = ConvertFlatPropertiesToHierarchy(objects);
                }
                
                // Results materialization from JSON objects
                var materializationResult = await MaterializeResultsFromJson<TProps>(objects, context);
                
                _logger?.LogInformation("📊 TOTAL ToListAsync: SQL ({SqlMs} ms) + materialization + Props = full cycle", sqlTimer.ElapsedMilliseconds);
                
                return materializationResult;
            }
        }
        else
        {
            _logger?.LogWarning("⚠️ SQL RESULT IS EMPTY: result?.result == null");
        }

        _logger?.LogDebug("LINQ ToList Result: No objects returned, returning empty list");
        return new List<RedbObject<TProps>>();
    }

    private async Task<List<RedbObject<TProps>>> MaterializeResultsFromJson<TProps>(System.Text.Json.JsonElement[] objects, QueryContext<TProps> context) 
        where TProps : class, new()
    {
        _logger?.LogDebug("🔍 MATERIALIZATION: Received {Count} JSON objects for deserialization", objects?.Length ?? 0);
        
        // ⏱️ MEASURING MATERIALIZATION TIME
        var materializationTimer = System.Diagnostics.Stopwatch.StartNew();
        
        var materializedResults = new List<RedbObject<TProps>>();
        var successCount = 0;
        var errorCount = 0;

        if (objects == null || objects.Length == 0)
        {
            _logger?.LogDebug("⚠️ MATERIALIZATION: JSON array is empty or null");
            return materializedResults;
        }

        // Calculate lazy loading mode ONCE before the loop
        // UseLazyLoading overrides config if explicitly set (via .WithLazyLoading())
        // useLazyOnDemand = true means Props loaded on demand when accessing obj.Props
        // useLazyOnDemand = false means Props loaded via LoadPropsForManyAsync after materialization
        var useLazyOnDemand = context.UseLazyLoading ?? _configuration.EnableLazyLoadingForProps;

        foreach (var objElement in objects)
        {
            try
            {
                // Objects are already in JSON format from get_object_json
                var objectJson = objElement.GetRawText();
                
                // Filter by access rights if necessary
                if (context.CheckPermissions && context.UserId.HasValue)
                {
                    // Extract object ID for rights check
                    if (objElement.TryGetProperty("id", out var idElement))
                    {
                        var objectId = idElement.GetInt64();
                        var hasPermission = await CheckUserPermission(objectId, context.UserId.Value);
                        if (!hasPermission)
                        {
                            continue; // Skip object without access rights
                        }
                    }
                }
                
                // Deserialize object JSON data
                var redbObject = _serializer.Deserialize<TProps>(objectJson);
                
                // Set lazy loader ONLY if useLazyOnDemand=true
                // (for all other cases, Props will be loaded via LoadPropsForManyAsync after materialization)
                if (useLazyOnDemand && _lazyPropsLoader != null && redbObject.id > 0)
                {
                    redbObject._lazyLoader = _lazyPropsLoader;
                    redbObject._propsLoaded = false;
                    
                    _logger?.LogDebug("🔄 Lazy loader set for object {ObjectId} (individual on-demand mode)", redbObject.id);
                }
                
                materializedResults.Add(redbObject);
                successCount++;
            }
            catch (Exception ex)
            {
                // Fail fast on deserialization error - indicates schema/data mismatch
                var innerMsg = ex.InnerException?.Message ?? "";
                var jsonPreview = objElement.GetRawText();
                if (jsonPreview.Length > 500) jsonPreview = jsonPreview[..500] + "...";
                
                throw new InvalidOperationException(
                    $"Deserialization failed: {ex.Message} | Inner: {innerMsg} | JSON: {jsonPreview}", ex);
            }
        }

        materializationTimer.Stop();
        
        _logger?.LogDebug("📊 MATERIALIZATION COMPLETED: Success={Success}, Errors={Errors}, Total objects={Total}", 
            successCount, errorCount, materializedResults.Count);
        _logger?.LogInformation("⏱️  Materialization JSON → C# objects completed in {ElapsedMs} ms", materializationTimer.ElapsedMilliseconds);

        // ⭐ PROJECTION: If SkipPropsLoading = true — skip Props loading completely
        if (context.SkipPropsLoading)
        {
            return materializedResults;
        }
        
        // ✅ OPTIMIZATION: Two-phase loading strategy
        // Phase 1: Fast search via search_objects_with_facets_base (already done above)
        // Phase 2: Load Props via LoadPropsForManyAsync (with cache check)
        //
        // Decision logic (useLazyOnDemand already calculated above):
        // - useLazyOnDemand = false → LoadPropsForManyAsync (two-phase eager or batch)
        // - useLazyOnDemand = true → lazy on demand (Props loaded when accessing obj.Props)
        
        _logger?.LogInformation("🔍 Props loading: EnableLazyForProps={EnableLazy}, UseLazyLoading={UseLazy}, UseLazyOnDemand={OnDemand}, ResultsCount={Count}", 
            _configuration.EnableLazyLoadingForProps, context.UseLazyLoading, useLazyOnDemand, materializedResults.Count);
        
        if (useLazyOnDemand)
        {
            // INDIVIDUAL lazy loading - Props will be loaded when accessing obj.Props
            _logger?.LogInformation("✅ Individual lazy loading enabled. Props will be loaded when accessing a specific object.");
        }
        else if (materializedResults.Count > 0 && _lazyPropsLoader != null)
        {
            // BULK load Props via LoadPropsForManyAsync (two-phase loading)
            // This method checks cache first, then loads only missing objects via get_object_json batch
            var projectedIds = context.ProjectedStructureIds;
            var propsLoadTimer = System.Diagnostics.Stopwatch.StartNew();
            
            if (projectedIds != null && projectedIds.Count > 0)
            {
                await _lazyPropsLoader.LoadPropsForManyAsync(materializedResults, projectedIds, context.PropsDepth);
            }
            else
            {
                await _lazyPropsLoader.LoadPropsForManyAsync(materializedResults, context.PropsDepth);
            }
            
            propsLoadTimer.Stop();
            _logger?.LogInformation("⏱️  Props loaded via batch (with cache check) in {ElapsedMs} ms", propsLoadTimer.ElapsedMilliseconds);
        }

        // ✅ DISTINCT is now performed at the SQL level via search_objects_with_facets_base(..., distinct_hash=true)
        // No need to filter in memory!

        return materializedResults;
    }

    private async Task<bool> CheckUserPermission(long objectId, long userId)
    {
        var sql = _sql.Query_CheckPermissionSql();
        var result = await _context.QueryFirstOrDefaultAsync<PermissionCheckResult>(sql, objectId, userId);
        
        return result?.HasPermission ?? false;
    }

    /// <summary>
    /// Forms JSON for the order_by parameter based on sortings from the context
    /// 🆕 FIXED: Uses _facetBuilder.BuildOrderBy to support 0$: prefix for base fields
    /// </summary>
    private string? BuildOrderByJson<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        if (!context.Orderings.Any())
            return null;

        // 🆕 Using _facetBuilder.BuildOrderBy which correctly handles IsBaseField
        return _facetBuilder.BuildOrderBy(context.Orderings);
    }
    
    // ============================================================
    // === v2-pvt PIPELINE (free Postgres path) ===
    // Two-step search: ask the DB to BUILD the inner _id-list SQL via
    // pvt_build_query_sql(), then wrap on the client side. Dialect that
    // does not support v2-pvt (Query_BuildPvtSqlFunction() == null)
    // routes back to the legacy search_objects_with_facets pipeline.
    // ============================================================

    /// <summary>
    /// Returns true when the active dialect exposes the v2-pvt module.
    /// Projection is applied client-side (see RedbProjectedQueryable.ToListAsync),
    /// so the PVT pipeline serves projection queries by returning full objects via
    /// get_object_json — the compiled selector then extracts the requested fields.
    /// </summary>
    protected virtual bool CanUsePvtPipeline<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        return _sql.Query_BuildPvtSqlFunction() is not null;
    }

    /// <summary>
    /// Phase 1: ask the database to build the inner _id-list SQL string.
    /// When <paramref name="ignoreLimitOffset"/> is true the call passes
    /// p_limit=NULL,p_offset=0 (used for COUNT/EXISTS wrappers).
    /// </summary>
    protected async Task<string> BuildPvtInnerSqlAsync<TProps>(
        QueryContext<TProps> context,
        bool ignoreLimitOffset) where TProps : class, new()
    {
        var facetFilters = _facetBuilder.BuildFacetFilters(context.Filter);
        var orderByJson  = BuildOrderByJson(context);
        var parameters   = _facetBuilder.BuildQueryParameters(context.Limit, context.Offset);

        var limit  = ignoreLimitOffset ? (int?)null : parameters.Limit;
        var offset = ignoreLimitOffset ? 0          : (parameters.Offset ?? 0);

        // DistinctBy → p_distinct_on jsonb array (PVT engine handles SELECT DISTINCT ON parity)
        string? distinctOnJson = null;
        if (context.DistinctByField != null)
        {
            var name = context.DistinctByField.Property.Name;
            distinctOnJson = context.DistinctByIsBaseField
                ? "[{\"field\":\"0$:" + name + "\"}]"
                : "[{\"field\":\"" + name + "\"}]";
        }

        var invocation = _sql.Query_BuildPvtSqlInvocation(
            schemeId     : context.SchemeId,
            limit        : limit,
            offset       : offset,
            maxDepth     : context.MaxRecursionDepth ?? 10,
            distinct     : context.IsDistinct,
            sourceMode   : "flat",
            treeIds      : null,
            hasDistinctOn: distinctOnJson != null);

        if (invocation is null)
            throw new InvalidOperationException(
                "v2-pvt is enabled by dialect but Query_BuildPvtSqlInvocation returned null.");

        object filterParam = string.IsNullOrEmpty(facetFilters) || facetFilters == "{}"
            ? (object)DBNull.Value
            : facetFilters;
        object orderParam = string.IsNullOrEmpty(orderByJson) || orderByJson == "null" || orderByJson == "[]"
            ? (object)DBNull.Value
            : orderByJson;

        string? inner;
        if (distinctOnJson != null)
            inner = await _context.ExecuteScalarAsync<string>(invocation, filterParam, orderParam, distinctOnJson);
        else
            inner = await _context.ExecuteScalarAsync<string>(invocation, filterParam, orderParam);

        if (string.IsNullOrWhiteSpace(inner))
            throw new InvalidOperationException(
                "pvt_build_query_sql returned an empty SQL string for scheme " + context.SchemeId + ".");
        return inner;
    }

    /// <summary>v2-pvt Count path: COUNT(*) wrapper over the inner _id-list SQL.</summary>
    protected virtual async Task<int> ExecuteCountAsyncPvt<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        var inner   = await BuildPvtInnerSqlAsync(context, ignoreLimitOffset: true);
        var wrapped = _sql.Query_WrapPvtWithCount(inner)
            ?? throw new InvalidOperationException("Query_WrapPvtWithCount returned null for a PVT-enabled dialect.");
        var count = await _context.ExecuteScalarAsync<long>(wrapped);
        return checked((int)count);
    }

    /// <summary>v2-pvt SQL preview path: returns the inner _id-list SQL as-is.</summary>
    protected virtual Task<string> GetSqlPreviewAsyncPvt<TProps>(QueryContext<TProps> context) where TProps : class, new()
        => BuildPvtInnerSqlAsync(context, ignoreLimitOffset: false);

    /// <summary>
    /// v2-pvt ToList path: wraps the inner SQL with get_object_json, executes,
    /// and reuses the existing JSON materializer.
    /// </summary>
    protected virtual async Task<object> ExecuteToListAsyncPvt<TProps>(QueryContext<TProps> context, Type propsType) where TProps : class, new()
    {
        var inner = await BuildPvtInnerSqlAsync(context, ignoreLimitOffset: false);
        var maxDepth = context.MaxRecursionDepth ?? 10;
        var wrapped = _sql.Query_WrapPvtWithObjectJson(inner, maxDepth)
            ?? throw new InvalidOperationException("Query_WrapPvtWithObjectJson returned null for a PVT-enabled dialect.");

        var sqlTimer = System.Diagnostics.Stopwatch.StartNew();
        var rows = await _context.QueryAsync<StringValue>(wrapped);
        sqlTimer.Stop();
        _logger?.LogInformation("⏱️  PVT SQL (get_object_json wrapper) executed in {ElapsedMs} ms", sqlTimer.ElapsedMilliseconds);

        if (rows is null || rows.Count == 0)
        {
            _logger?.LogDebug("PVT ToList Result: empty");
            return new List<RedbObject<TProps>>();
        }

        var objects = new System.Text.Json.JsonElement[rows.Count];
        for (var i = 0; i < rows.Count; i++)
        {
            var json = rows[i].Value;
            if (string.IsNullOrWhiteSpace(json)) continue;
            objects[i] = System.Text.Json.JsonDocument.Parse(json).RootElement.Clone();
        }

        return await MaterializeResultsFromJson<TProps>(objects, context);
    }

    /// <summary>
    /// Returns true when the active dialect exposes native PVT projection
    /// (pvt_build_projection_sql) AND the context has only safe constructs.
    /// Currently we exclude DistinctByField (uses p_distinct_on path that
    /// needs separate wiring) and ProjectedStructureIds (legacy by-ids path).
    /// </summary>
    protected virtual bool CanUsePvtProjection<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        if (_sql.Query_BuildPvtProjectionSqlFunction() is null) return false;
        if (context.ProjectedFieldPaths is null || context.ProjectedFieldPaths.Count == 0) return false;
        if (context.DistinctByField != null) return false;
        if (context.ProjectedStructureIds != null && context.ProjectedStructureIds.Count > 0) return false;
        return true;
    }

    /// <summary>
    /// v2-pvt native projection path. Builds a projection JSON from
    /// context.ProjectedFieldPaths, asks pvt_build_projection_sql for the
    /// inner SELECT yielding the requested scalar columns, wraps it into a
    /// JSON-row shape compatible with the standard materializer, and runs
    /// it through ConvertFlatPropertiesToHierarchy + MaterializeResultsFromJson.
    /// The compiled selector (RedbProjectedQueryable) then trims the resulting
    /// RedbObject&lt;TProps&gt; instances client-side.
    /// </summary>
    protected virtual async Task<object> ExecuteToListAsyncPvtProjection<TProps>(QueryContext<TProps> context) where TProps : class, new()
    {
        var facetFilters = _facetBuilder.BuildFacetFilters(context.Filter);
        var orderByJson  = BuildOrderByJson(context);
        var parameters   = _facetBuilder.BuildQueryParameters(context.Limit, context.Offset);

        // Build projection JSON: one {"field":"<path>"} entry per ProjectedFieldPath.
        // The default alias inside pvt_build_projection equals the field text, so the
        // resulting inner-SQL columns are named exactly "FirstName", "Address.City", etc.
        // When IsDistinct is false we add an extra "_id" entry so we can attach the
        // object id to each materialized RedbObject<TProps>. Under DISTINCT we drop
        // "_id" to avoid making every row trivially unique.
        var includeId = !context.IsDistinct;
        var projectionJson = BuildPvtProjectionJson(context.ProjectedFieldPaths!, includeId);

        var invocation = _sql.Query_BuildPvtProjectionSqlInvocation(
            schemeId     : context.SchemeId,
            limit        : parameters.Limit,
            offset       : parameters.Offset ?? 0,
            maxDepth     : context.MaxRecursionDepth ?? 10,
            distinct     : context.IsDistinct,
            sourceMode   : "flat",
            treeIds      : null,
            hasDistinctOn: false);

        if (invocation is null)
            throw new InvalidOperationException(
                "Query_BuildPvtProjectionSqlInvocation returned null for a projection-PVT-enabled dialect.");

        object filterParam = string.IsNullOrEmpty(facetFilters) || facetFilters == "{}"
            ? (object)DBNull.Value
            : facetFilters;
        object orderParam = string.IsNullOrEmpty(orderByJson) || orderByJson == "null" || orderByJson == "[]"
            ? (object)DBNull.Value
            : orderByJson;

        var inner = await _context.ExecuteScalarAsync<string>(invocation, projectionJson, filterParam, orderParam);
        if (string.IsNullOrWhiteSpace(inner))
            throw new InvalidOperationException(
                "pvt_build_projection_sql returned an empty SQL string for scheme " + context.SchemeId + ".");

        var wrapped = _sql.Query_WrapPvtProjectionRowsAsJson(inner, includeId)
            ?? throw new InvalidOperationException("Query_WrapPvtProjectionRowsAsJson returned null for a projection-PVT-enabled dialect.");

        var sqlTimer = System.Diagnostics.Stopwatch.StartNew();
        var rows = await _context.QueryAsync<StringValue>(wrapped);
        sqlTimer.Stop();
        _logger?.LogInformation("⏱️  PVT projection SQL executed in {ElapsedMs} ms ({RowCount} rows)",
            sqlTimer.ElapsedMilliseconds, rows?.Count ?? 0);

        if (rows is null || rows.Count == 0)
            return new List<RedbObject<TProps>>();

        var objects = new System.Text.Json.JsonElement[rows.Count];
        for (var i = 0; i < rows.Count; i++)
        {
            var json = rows[i].Value;
            if (string.IsNullOrWhiteSpace(json)) continue;
            objects[i] = System.Text.Json.JsonDocument.Parse(json).RootElement.Clone();
        }

        // Convert dotted-key properties ("Address.City") into nested objects so
        // the typed Props deserializer can map them. Then go through the standard
        // materializer with SkipPropsLoading=true (we already have the Props subset).
        var hierarchical = ConvertFlatPropertiesToHierarchy(objects);
        var prevSkip = context.SkipPropsLoading;
        context.SkipPropsLoading = true;
        try
        {
            return await MaterializeResultsFromJson<TProps>(hierarchical, context);
        }
        finally
        {
            context.SkipPropsLoading = prevSkip;
        }
    }

    /// <summary>
    /// Serializes the projected field paths into the JSON shape expected by
    /// pvt_build_projection_sql: <c>[{"field":"FirstName"},{"field":"Address.City"}, ...]</c>.
    /// Adds a trailing <c>{"field":"_id"}</c> entry when <paramref name="includeId"/> is true.
    /// </summary>
    private static string BuildPvtProjectionJson(List<string> fieldPaths, bool includeId)
    {
        var sb = new System.Text.StringBuilder();
        sb.Append('[');
        for (var i = 0; i < fieldPaths.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append("{\"field\":");
            sb.Append(System.Text.Json.JsonSerializer.Serialize(fieldPaths[i]));
            sb.Append('}');
        }
        if (includeId)
        {
            if (fieldPaths.Count > 0) sb.Append(',');
            sb.Append("{\"field\":\"_id\"}");
        }
        sb.Append(']');
        return sb.ToString();
    }

    /// <summary>
    /// Trivial DTO for one-column string result-sets (e.g. get_object_json output).
    /// </summary>
    private sealed class StringValue
    {
        public string Value { get; set; } = string.Empty;
    }

    /// <summary>
    /// Result of the search_objects_with_facets function (returns jsonb)
    /// </summary>
    private class SearchJsonResult
    {
        public string result { get; set; } = string.Empty; // Lowercase for PostgreSQL
    }
    
    /// <summary>
    /// ⭐ PROJECTION: Converts flat paths ("Address.City") to hierarchical JSON
    /// </summary>
    private System.Text.Json.JsonElement[] ConvertFlatPropertiesToHierarchy(System.Text.Json.JsonElement[] objects)
    {
        var result = new List<System.Text.Json.JsonElement>();
        
        foreach (var obj in objects)
        {
            try
            {
                var dict = new Dictionary<string, object?>();
                
                // Copy all properties except properties
                foreach (var prop in obj.EnumerateObject())
                {
                    if (prop.Name == "properties")
                    {
                        // Convert flat properties to hierarchy
                        var hierarchicalProps = ConvertFlatToHierarchy(prop.Value);
                        dict["properties"] = hierarchicalProps;
                    }
                    else
                    {
                        dict[prop.Name] = JsonElementToObject(prop.Value);
                    }
                }
                
                // Serialize back to JsonElement (without Unicode escaping)
                var jsonOptions = new System.Text.Json.JsonSerializerOptions 
                { 
                    Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping 
                };
                var json = System.Text.Json.JsonSerializer.Serialize(dict, jsonOptions);
                var newElement = System.Text.Json.JsonDocument.Parse(json).RootElement.Clone();
                result.Add(newElement);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "⚠️ Error converting flat properties to hierarchy");
                result.Add(obj); // Return original on error
            }
        }
        
        return result.ToArray();
    }
    
    /// <summary>
    /// Converts flat paths to a hierarchical dictionary
    /// "Address.City": "Moscow" → {"Address": {"City": "Moscow"}}
    /// </summary>
    private Dictionary<string, object?> ConvertFlatToHierarchy(System.Text.Json.JsonElement flatProperties)
    {
        var result = new Dictionary<string, object?>();
        
        // ⭐ STEP 1: Collect all paths that are "classes" (have nested fields)
        // If there is "Address1.Details.Floor", then "Address1" and "Address1.Details" are classes
        var classPathPrefixes = new HashSet<string>();
        foreach (var prop in flatProperties.EnumerateObject())
        {
            var path = prop.Name;
            var dotIndex = path.LastIndexOf('.');
            while (dotIndex > 0)
            {
                var prefix = path.Substring(0, dotIndex);
                classPathPrefixes.Add(prefix);
                dotIndex = prefix.LastIndexOf('.');
            }
        }
        
        // ⭐ STEP 2: Sort - first nested paths (with dots), then simple ones
        var props = flatProperties.EnumerateObject()
            .OrderByDescending(p => p.Name.Contains('.') || p.Name.Contains('['))
            .ThenBy(p => p.Name)
            .ToList();
        
        foreach (var prop in props)
        {
            var path = prop.Name;
            
            // ⭐ Skip class GUIDs at ANY nesting level
            // If the path is in classPathPrefixes — it's a class GUID, skip it
            if (classPathPrefixes.Contains(path))
            {
                continue; // "Address1", "Address1.Details" are class GUIDs
            }
            
            var value = JsonElementToObject(prop.Value);
            var segments = ParsePathSegments(path);
            
            SetNestedValue(result, segments, value);
        }
        
        return result;
    }
    
    /// <summary>
    /// Parses path into segments taking arrays into account
    /// "Address.City" → ["Address", "City"]
    /// "Items[0].Price" → ["Items", "[0]", "Price"]
    /// </summary>
    private List<string> ParsePathSegments(string path)
    {
        var segments = new List<string>();
        var current = new System.Text.StringBuilder();
        
        for (int i = 0; i < path.Length; i++)
        {
            char c = path[i];
            
            if (c == '.')
            {
                if (current.Length > 0)
                {
                    segments.Add(current.ToString());
                    current.Clear();
                }
            }
            else if (c == '[')
            {
                if (current.Length > 0)
                {
                    segments.Add(current.ToString());
                    current.Clear();
                }
                // Look for closing bracket
                int closeIndex = path.IndexOf(']', i);
                if (closeIndex > i)
                {
                    segments.Add(path.Substring(i, closeIndex - i + 1)); // "[0]"
                    i = closeIndex;
                }
            }
            else
            {
                current.Append(c);
            }
        }
        
        if (current.Length > 0)
        {
            segments.Add(current.ToString());
        }
        
        return segments;
    }
    
    /// <summary>
    /// Sets value by nested path
    /// </summary>
    private void SetNestedValue(Dictionary<string, object?> root, List<string> segments, object? value)
    {
        if (segments.Count == 0) return;
        
        var current = root;
        
        for (int i = 0; i < segments.Count - 1; i++)
        {
            var segment = segments[i];
            var nextSegment = segments[i + 1];
            
            // Check if next segment is an array index?
            bool nextIsArrayIndex = nextSegment.StartsWith("[") && nextSegment.EndsWith("]");
            
            if (segment.StartsWith("[") && segment.EndsWith("]"))
            {
                // Current segment is an array index, skip (processed at previous step)
                continue;
            }
            
            if (!current.ContainsKey(segment))
            {
                // If next segment is an array index, create a list
                if (nextIsArrayIndex)
                {
                    current[segment] = new List<object?>();
                }
                else
                {
                    current[segment] = new Dictionary<string, object?>();
                }
            }
            
            var next = current[segment];
            
            if (nextIsArrayIndex)
            {
                // Extract index/key: "[0]" → "0", "[home]" → "home"
                var indexStr = nextSegment.Substring(1, nextSegment.Length - 2);
                
                if (int.TryParse(indexStr, out int index))
                {
                    // ⭐ ARRAY: numeric index
                    if (next is not List<object?> list)
                    {
                        list = new List<object?>();
                        current[segment] = list;
                    }
                    
                    // Expand list if necessary
                    while (list.Count <= index)
                    {
                        list.Add(new Dictionary<string, object?>());
                    }
                    
                    // Next element after index
                    if (i + 2 < segments.Count)
                    {
                        if (list[index] is Dictionary<string, object?> itemDict)
                        {
                            current = itemDict;
                        }
                        else
                        {
                            var newDict = new Dictionary<string, object?>();
                            list[index] = newDict;
                            current = newDict;
                        }
                    }
                    else
                    {
                        // Last segment - just a value in the array
                        list[index] = value;
                        return;
                    }
                }
                else
                {
                    // ⭐ DICTIONARY: string key (e.g. "home")
                    if (next is not Dictionary<string, object?> dict)
                    {
                        dict = new Dictionary<string, object?>();
                        current[segment] = dict;
                    }
                    
                    // Next element after key
                    if (i + 2 < segments.Count)
                    {
                        if (!dict.ContainsKey(indexStr))
                        {
                            dict[indexStr] = new Dictionary<string, object?>();
                        }
                        
                        if (dict[indexStr] is Dictionary<string, object?> keyDict)
                        {
                            current = keyDict;
                        }
                        else
                        {
                            var newDict = new Dictionary<string, object?>();
                            dict[indexStr] = newDict;
                            current = newDict;
                        }
                    }
                    else
                    {
                        // Last segment - just a value in the dictionary
                        dict[indexStr] = value;
                        return;
                    }
                }
                i++; // Skip index segment
            }
            else if (next is Dictionary<string, object?> dict)
            {
                current = dict;
            }
        }
        
        // Set final value
        var lastSegment = segments[segments.Count - 1];
        if (!lastSegment.StartsWith("["))
        {
            current[lastSegment] = value;
        }
    }
    
    /// <summary>
    /// Converts JsonElement to object
    /// </summary>
    private object? JsonElementToObject(System.Text.Json.JsonElement element)
    {
        return element.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Null => null,
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            System.Text.Json.JsonValueKind.String => element.GetString(),
            System.Text.Json.JsonValueKind.Array => element.EnumerateArray().Select(JsonElementToObject).ToList(),
            System.Text.Json.JsonValueKind.Object => element.EnumerateObject()
                .ToDictionary(p => p.Name, p => JsonElementToObject(p.Value)),
            _ => element.GetRawText()
        };
    }

    /// <summary>
    /// Returns the SQL query that will be executed (for debugging)
    /// </summary>
    public virtual async Task<string> GetSqlPreviewAsync<TProps>(QueryContext<TProps> context) 
        where TProps : class, new()
    {
        // v2-pvt path: return the inner _id-list SQL produced by pvt_build_query_sql.
        // The caller can run it directly or wrap with COUNT/EXISTS/get_object_json on the client side.
        if (CanUsePvtPipeline(context))
        {
            _logger?.LogDebug("PVT SQL Preview: SchemeId={SchemeId}", context.SchemeId);
            return await GetSqlPreviewAsyncPvt(context);
        }

        var facetFilters = _facetBuilder.BuildFacetFilters(context.Filter);
        var parameters = _facetBuilder.BuildQueryParameters(context.Limit, context.Offset);
        var orderByJson = BuildOrderByJson(context);
        
        // Choose preview function based on lazy loading settings (via dialect)
        var useLazyLoading = ShouldUseLazyLoading(context);
        var functionName = useLazyLoading
            ? _sql.Query_SqlPreviewBaseFunction() 
            : _sql.Query_SqlPreviewFunction();
        
        _logger?.LogDebug("Getting SQL Preview: Function={FunctionName}, SchemeId={SchemeId}, LazyLoading={LazyLoading}, Distinct={Distinct}", 
            functionName, context.SchemeId, useLazyLoading, context.IsDistinct);
        
        var sqlQuery = string.Format(_sql.Query_SqlPreviewTemplate(), functionName);
        var result = await _context.QueryFirstOrDefaultAsync<SqlPreviewResult>(sqlQuery, 
            context.SchemeId, 
            facetFilters, 
            parameters.Limit ?? int.MaxValue,
            parameters.Offset ?? 0,
            orderByJson ?? "null",
            context.MaxRecursionDepth ?? 10,
            context.IsDistinct);  // ✅ DISTINCT ON (_hash) in preview
        
        return result?.sql_preview ?? "-- SQL preview not available";
    }
    
    /// <summary>
    /// Returns the JSON filter that will be sent to SQL function (for diagnostics)
    /// </summary>
    public virtual Task<string> GetFilterJsonAsync<TProps>(QueryContext<TProps> context) 
        where TProps : class, new()
    {
        var facetFilters = _facetBuilder.BuildFacetFilters(context.Filter);
        return Task.FromResult(facetFilters);
    }
    
    /// <summary>
    /// 🔍 Returns SQL query for aggregation (for debugging)
    /// Similar to ToQueryString() from EF Core
    /// </summary>
    public virtual async Task<string> GetAggregateSqlPreviewAsync<TProps, TResult>(
        QueryContext<TProps> context,
        System.Linq.Expressions.Expression<Func<RedbObject<TProps>, TResult>> selector)
        where TProps : class, new()
    {
        // Parse expression using the same method as in RedbQueryable
        var aggregations = ParseAggregateExpressionForPreview(selector);
        
        // Form JSON for SQL function
        var aggregationsJson = System.Text.Json.JsonSerializer.Serialize(
            aggregations.Select(a => new { field = a.FieldPath, func = a.Function, alias = a.Alias }));
        
        // Get filter
        var filterJson = _facetBuilder.BuildFacetFilters(context.Filter);
        
        _logger?.LogDebug("Getting Aggregate SQL Preview: SchemeId={SchemeId}, Aggregations={Aggs}", 
            context.SchemeId, aggregationsJson);
        
        // If filter is empty — pass NULL (not string "null")
        object? filterParam = (filterJson == "{}" || string.IsNullOrEmpty(filterJson)) 
            ? DBNull.Value 
            : filterJson;
        
        var result = await _context.QueryFirstOrDefaultAsync<SqlPreviewResult>(
            _sql.Query_AggregateBatchPreviewSql(), 
            context.SchemeId, 
            aggregationsJson,
            filterParam);
        
        return result?.sql_preview ?? "-- SQL preview not available";
    }
    
    /// <summary>
    /// Parses aggregate expression for preview (simplified version from RedbQueryable)
    /// </summary>
    protected List<(string Alias, string FieldPath, string Function)> ParseAggregateExpressionForPreview<TProps, TResult>(
        System.Linq.Expressions.Expression<Func<RedbObject<TProps>, TResult>> selector)
        where TProps : class, new()
    {
        var result = new List<(string Alias, string FieldPath, string Function)>();
        
        if (selector.Body is System.Linq.Expressions.NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var arg = newExpr.Arguments[i];
                var propName = newExpr.Members?[i].Name ?? $"Item{i}";
                
                if (arg is System.Linq.Expressions.MethodCallExpression methodCall &&
                    methodCall.Method.DeclaringType?.Name == "Agg")
                {
                    var funcName = methodCall.Method.Name switch
                    {
                        "Sum" => "SUM",
                        "Average" => "AVG",
                        "Min" => "MIN",
                        "Max" => "MAX",
                        "Count" => "COUNT",
                        _ => methodCall.Method.Name.ToUpper()
                    };
                    
                    string fieldPath = "*";
                    if (methodCall.Arguments.Count > 0)
                    {
                        fieldPath = ExtractFieldPathForPreview(methodCall.Arguments[0]);
                    }
                    
                    result.Add((propName, fieldPath, funcName));
                }
            }
        }
        
        return result;
    }
    
    /// <summary>
    /// Extracts field path from expression (for preview)
    /// </summary>
    private string ExtractFieldPathForPreview(System.Linq.Expressions.Expression expr)
    {
        var parts = new List<string>();
        ExtractFieldPathRecursiveForPreview(expr, parts);
        return string.Join(".", parts);
    }
    
    private void ExtractFieldPathRecursiveForPreview(System.Linq.Expressions.Expression? expr, List<string> parts)
    {
        if (expr == null) return;
        
        switch (expr)
        {
            case System.Linq.Expressions.MemberExpression member:
                if (member.Member.Name != "Props")
                    parts.Insert(0, member.Member.Name);
                ExtractFieldPathRecursiveForPreview(member.Expression, parts);
                break;
                
            case System.Linq.Expressions.BinaryExpression { NodeType: System.Linq.Expressions.ExpressionType.ArrayIndex } arrayIndex:
                ExtractFieldPathRecursiveForPreview(arrayIndex.Left, parts);
                if (arrayIndex.Right is System.Linq.Expressions.ConstantExpression indexConst && parts.Count > 0)
                {
                    var index = Convert.ToInt32(indexConst.Value);
                    parts[^1] = $"{parts[^1]}[{index}]";
                }
                break;
                
            case System.Linq.Expressions.MethodCallExpression { Method.Name: "Select" } selectCall:
                ExtractFieldPathRecursiveForPreview(selectCall.Arguments[0], parts);
                if (parts.Count > 0) parts[^1] = $"{parts[^1]}[]";
                if (selectCall.Arguments.Count > 1 && 
                    selectCall.Arguments[1] is System.Linq.Expressions.LambdaExpression lambda)
                {
                    var innerParts = new List<string>();
                    ExtractFieldPathRecursiveForPreview(lambda.Body, innerParts);
                    parts.AddRange(innerParts);
                }
                break;
                
            case System.Linq.Expressions.MethodCallExpression { Method.Name: "Sum" or "Average" or "Min" or "Max" } linqAggCall:
                ExtractFieldPathRecursiveForPreview(linqAggCall.Arguments[0], parts);
                if (parts.Count > 0) parts[^1] = $"{parts[^1]}[]";
                if (linqAggCall.Arguments.Count > 1 && 
                    linqAggCall.Arguments[1] is System.Linq.Expressions.LambdaExpression aggLambda)
                {
                    var innerParts = new List<string>();
                    ExtractFieldPathRecursiveForPreview(aggLambda.Body, innerParts);
                    parts.AddRange(innerParts);
                }
                break;
        }
    }

    /// <summary>
    /// Access rights check result
    /// </summary>
    private class PermissionCheckResult
    {
        public bool HasPermission { get; set; }
    }
    
    // ===== AGGREGATIONS (EAV) =====
    
    /// <summary>
    /// Perform aggregation on EAV field (SQL strategy)
    /// </summary>
    public virtual async Task<decimal?> ExecuteAggregateAsync(
        long schemeId, 
        string fieldPath, 
        AggregateFunction function,
        string? filterJson = null)
    {
        _logger?.LogDebug("🔢 AGGREGATION: SchemeId={SchemeId}, Field={Field}, Function={Function}, Filter={Filter}", 
            schemeId, fieldPath, function, filterJson ?? "null");
        
        var timer = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // SQL function aggregate_field(scheme_id, field_path, function_name, filter_json)
            var functionName = function switch
            {
                AggregateFunction.Sum => "SUM",
                AggregateFunction.Average => "AVG",
                AggregateFunction.Min => "MIN",
                AggregateFunction.Max => "MAX",
                AggregateFunction.Count => "COUNT",
                _ => throw new NotSupportedException($"Aggregation function {function} is not supported")
            };
            
            var result = await _context.QueryFirstOrDefaultAsync<AggregateDecimalResult>(
                _sql.Query_AggregateFieldSql(),
                schemeId,
                fieldPath,
                functionName,
                filterJson ?? "null");
            
            timer.Stop();
            _logger?.LogInformation("⏱️ AGGREGATION {Function}({Field}) = {Result} in {ElapsedMs} ms", 
                function, fieldPath, result?.result, timer.ElapsedMilliseconds);
            
            return result?.result;
        }
        catch (Exception ex)
        {
            timer.Stop();
            _logger?.LogError(ex, "❌ AGGREGATION ERROR: {Function}({Field}) in {ElapsedMs} ms", 
                function, fieldPath, timer.ElapsedMilliseconds);
            throw;
        }
    }
    
    /// <summary>
    /// Perform batch aggregation — ONE SQL query for all aggregations!
    /// </summary>
    public virtual async Task<AggregateResult> ExecuteAggregateBatchAsync(
        long schemeId,
        IEnumerable<AggregateRequest> requests,
        string? filterJson = null)
    {
        var result = new AggregateResult();
        var requestList = requests.ToList();
        
        _logger?.LogDebug("🔢 BATCH AGGREGATION (1 SQL): SchemeId={SchemeId}, Requests={Count}", schemeId, requestList.Count);
        
        // Form JSON array of aggregations
        var aggregationsJson = System.Text.Json.JsonSerializer.Serialize(
            requestList.Select(r => new 
            {
                field = r.FieldPath,
                func = r.Function switch
                {
                    AggregateFunction.Sum => "SUM",
                    AggregateFunction.Average => "AVG",  // PostgreSQL uses AVG, not AVERAGE!
                    AggregateFunction.Min => "MIN",
                    AggregateFunction.Max => "MAX",
                    AggregateFunction.Count => "COUNT",
                    _ => r.Function.ToString().ToUpper()
                },
                alias = r.Alias ?? $"{r.Function}_{r.FieldPath}"
            })
        );
        
        // ONE SQL query!
        var jsonResult = await _context.ExecuteJsonAsync(
            _sql.Query_AggregateBatchSql(),
            schemeId,
            aggregationsJson,
            (object?)filterJson ?? DBNull.Value);
        
        if (!string.IsNullOrEmpty(jsonResult))
        {
            var dict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, System.Text.Json.JsonElement>>(jsonResult);
            if (dict != null)
            {
                foreach (var kv in dict)
                {
                    result.Values[kv.Key] = kv.Value.ValueKind == System.Text.Json.JsonValueKind.Number 
                        ? kv.Value.GetDecimal() 
                        : null;
                }
            }
        }
        
        _logger?.LogDebug("🔢 BATCH RESULT: {Result}", System.Text.Json.JsonSerializer.Serialize(result.Values));
        
        return result;
    }
    
    /// <summary>
    /// Execute single-field aggregation with FilterExpression (Pro version).
    /// Free fallback: converts FilterExpression to facet-JSON.
    /// </summary>
    public virtual async Task<decimal?> ExecuteAggregateAsync(
        long schemeId,
        string fieldPath,
        AggregateFunction function,
        QueryExpressions.FilterExpression? filter)
    {
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return await ExecuteAggregateAsync(schemeId, fieldPath, function, filterJson);
    }
    
    /// <summary>
    /// Execute batch aggregation with FilterExpression (Pro version).
    /// Free fallback: converts FilterExpression to facet-JSON.
    /// </summary>
    public virtual async Task<AggregateResult> ExecuteAggregateBatchAsync(
        long schemeId,
        IEnumerable<AggregateRequest> requests,
        QueryExpressions.FilterExpression? filter)
    {
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return await ExecuteAggregateBatchAsync(schemeId, requests, filterJson);
    }
    
    /// <summary>
    /// Aggregation result (decimal)
    /// </summary>
    private class AggregateDecimalResult
    {
        public decimal? result { get; set; }
    }
    
    // ===== DELETE =====
    
    /// <summary>
    /// Delete objects by filter.
    /// </summary>
    public virtual async Task<int> ExecuteDeleteAsync(long schemeId, string? filterJson = null)
    {
        // 1. Get IDs of objects by filter via search_objects
        var jsonResult = await _context.ExecuteJsonAsync(
            _sql.Query_SearchObjectsSimpleSql(),
            schemeId,
            (object?)filterJson ?? DBNull.Value);
        
        if (string.IsNullOrEmpty(jsonResult))
            return 0;
        
        // 2. Parse JSON result to get IDs
        var jsonDoc = JsonDocument.Parse(jsonResult);
        var objectIds = new List<long>();
        
        if (jsonDoc.RootElement.TryGetProperty("objects", out var objectsArray))
        {
            foreach (var obj in objectsArray.EnumerateArray())
            {
                if (obj.TryGetProperty("id", out var idProp))
                {
                    objectIds.Add(idProp.GetInt64());
                }
            }
        }
        
        if (objectIds.Count == 0)
            return 0;
        
        // 3. Delete via dialect-specific SQL (MSSQL uses delete_objects_cascade, Postgres uses ANY)
        var deletedCount = await _context.ExecuteAsync(
            _sql.ObjectStorage_DeleteByIds(),
            objectIds.ToArray());
        
        _logger?.LogDebug("DELETE: Deleted {Count} objects by filter", deletedCount);
        
        return deletedCount;
    }
    
    /// <summary>
    /// Delete objects by FilterExpression (Pro version uses PVT, OpenSource falls back to facet JSON).
    /// </summary>
    public virtual async Task<int> ExecuteDeleteAsync(long schemeId, QueryExpressions.FilterExpression? filter)
    {
        // OpenSource: convert FilterExpression to facet JSON and use existing method
        var filterJson = filter != null ? _facetBuilder.BuildFacetFilters(filter) : null;
        return await ExecuteDeleteAsync(schemeId, filterJson);
    }
}
