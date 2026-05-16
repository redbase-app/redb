using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using redb.Core.Models.Entities;
using redb.Core.Query.Projection;

namespace redb.Core.Query;

/// <summary>
/// Implementation of LINQ query projections in REDB with filtering and sorting support
/// ⭐ OPTIMIZED: Uses ProjectionFieldExtractor to load only required fields
/// </summary>
public class RedbProjectedQueryable<TProps, TResult> : IRedbProjectedQueryable<TResult>
    where TProps : class, new()
{
    private readonly IRedbQueryable<TProps> _sourceQuery;
    private readonly Expression<Func<RedbObject<TProps>, TResult>> _projection;
    
    // ⭐ Provider and SchemeId for optimized loading
    private readonly IRedbQueryProvider? _provider;
    private readonly long _schemeId;
    
    // Chain of operations to execute after projection
    private readonly List<Expression<Func<TResult, bool>>> _wherePredicates = new();
    private readonly List<(Expression KeySelector, bool IsDescending)> _orderByExpressions = new();
    private readonly bool _isDistinct;  // Distinct flag for projection
    
    // ⭐ Extracted structure_ids for optimized loading
    private HashSet<long>? _projectedStructureIds;
    // ⭐ Text field paths for SQL function search_objects_with_projection_by_paths
    private List<string>? _projectedFieldPaths;
    private readonly ProjectionFieldExtractor _fieldExtractor = new();

    public RedbProjectedQueryable(
        IRedbQueryable<TProps> sourceQuery,
        Expression<Func<RedbObject<TProps>, TResult>> projection)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
    }
    
    /// <summary>
    /// ⭐ Constructor with provider for optimized loading
    /// </summary>
    public RedbProjectedQueryable(
        IRedbQueryable<TProps> sourceQuery,
        Expression<Func<RedbObject<TProps>, TResult>> projection,
        IRedbQueryProvider provider,
        long schemeId)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
        _provider = provider;
        _schemeId = schemeId;
    }
    
    // Private constructor for creating copies with additional operations
    private RedbProjectedQueryable(
        IRedbQueryable<TProps> sourceQuery,
        Expression<Func<RedbObject<TProps>, TResult>> projection,
        List<Expression<Func<TResult, bool>>> wherePredicates,
        List<(Expression KeySelector, bool IsDescending)> orderByExpressions,
        HashSet<long>? projectedStructureIds,
        List<string>? projectedFieldPaths,
        IRedbQueryProvider? provider,
        long schemeId,
        bool isDistinct = false)
    {
        _sourceQuery = sourceQuery;
        _projection = projection;
        _wherePredicates = new List<Expression<Func<TResult, bool>>>(wherePredicates);
        _orderByExpressions = new List<(Expression, bool)>(orderByExpressions);
        _projectedStructureIds = projectedStructureIds;
        _projectedFieldPaths = projectedFieldPaths;
        _provider = provider;
        _schemeId = schemeId;
        _isDistinct = isDistinct;
    }
    
    /// <summary>
    /// ⭐ Extracted structure_ids for optimization (for provider access)
    /// </summary>
    public HashSet<long>? ProjectedStructureIds => _projectedStructureIds;
    
    /// <summary>
    /// ⭐ Text field paths for SQL function search_objects_with_projection_by_paths
    /// </summary>
    public List<string>? ProjectedFieldPaths => _projectedFieldPaths;
    
    /// <summary>
    /// ⭐ Projection expression (for provider access)
    /// </summary>
    public Expression<Func<RedbObject<TProps>, TResult>> Projection => _projection;

    public IRedbProjectedQueryable<TResult> Where(Expression<Func<TResult, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));
        
        var newWherePredicates = new List<Expression<Func<TResult, bool>>>(_wherePredicates) { predicate };
        
        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery, 
            _projection, 
            newWherePredicates, 
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct);
    }

    public IRedbProjectedQueryable<TResult> OrderBy<TKey>(Expression<Func<TResult, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));
        
        var newOrderByExpressions = new List<(Expression, bool)> { (keySelector, false) };
        
        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery, 
            _projection, 
            _wherePredicates, 
            newOrderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct);
    }

    public IRedbProjectedQueryable<TResult> OrderByDescending<TKey>(Expression<Func<TResult, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));
        
        var newOrderByExpressions = new List<(Expression, bool)> { (keySelector, true) };
        
        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery, 
            _projection, 
            _wherePredicates, 
            newOrderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct);
    }

    public IRedbProjectedQueryable<TResult> Take(int count)
    {
        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery.Take(count), 
            _projection, 
            _wherePredicates, 
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct);
    }

    public IRedbProjectedQueryable<TResult> Skip(int count)
    {
        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery.Skip(count), 
            _projection, 
            _wherePredicates, 
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            _isDistinct);
    }

    public IRedbProjectedQueryable<TResult> Distinct()
    {
        // Distinct for projection is applied in memory after projection
        return new RedbProjectedQueryable<TProps, TResult>(
            _sourceQuery, 
            _projection, 
            _wherePredicates, 
            _orderByExpressions,
            _projectedStructureIds,
            _projectedFieldPaths,
            _provider,
            _schemeId,
            isDistinct: true);
    }

    public async Task<List<TResult>> ToListAsync()
    {
        // ⭐ OPTIMIZATION: Load only required structure_ids if possible
        List<RedbObject<TProps>> fullObjects;
        
        // 📊 LOG: Projection information
       
        // ⭐ Extract text paths and structure_ids from projection if not yet extracted
        if (_projectedFieldPaths == null && _provider != null && _schemeId > 0)
        {
            // Extract text paths for SQL projection function (search_objects_with_projection_by_paths)
            _projectedFieldPaths = _fieldExtractor.ExtractFieldPathStrings(_projection);
            
            // Also extract structure_ids (used by Pro's ProLazyPropsLoader for filtered _values loading)
            var scheme = await _provider.GetSchemeAsync(_schemeId);
            if (scheme != null)
            {
                _projectedStructureIds = _fieldExtractor.ExtractStructureIds(scheme, _projection);
            }
        }
        
        // ⭐ FIX: ALWAYS skip Props re-loading during projection.
        //
        // Projection flow is two-phase:
        //   Phase 1 (SQL):  search_objects_with_projection_by_paths returns JSON with
        //                   "properties" containing ONLY the projected fields.
        //   Phase 2 (C#):   Deserialization maps "properties" → Props via setter,
        //                   which sets _propsLoaded = true. Props is PARTIAL but sufficient.
        //   Phase 3 (C#):   compiledProjection(redbObj) extracts needed fields → TResult.
        //                   RedbObject<TProps> is then discarded.
        //
        // Without skipProps=true, MaterializeResultsFromJson would:
        //   - In EAGER mode: call LazyPropsLoader.LoadPropsForManyAsync which re-loads
        //     the FULL object via get_object_json, OVERWRITING the partial Props from SQL
        //     projection. This causes double DB round-trip and defeats the projection.
        //   - In LAZY mode: set _lazyLoader + _propsLoaded=false, causing the lazy loader
        //     to load full Props on first .Props access, again defeating projection.
        //
        // In both cases the final TResult is correct (projection extracts the right fields),
        // but the optimization is wasted — we load full object data anyway.
        //
        // The fix: tell MaterializeResultsFromJson to skip ALL Props post-processing.
        // Props data is already in the deserialized object from the SQL projection JSON.
        bool skipProps = true;
        
       
        // Try to use optimized loading via internal method
        if (_sourceQuery is RedbQueryable<TProps> redbQueryable)
        {
            // ⭐ Set text paths in context BEFORE call
            // Context is read in PostgresQueryProvider to select SQL function
            redbQueryable.SetProjectedFieldPaths(_projectedFieldPaths);
            
            fullObjects = await redbQueryable.ToListWithProjectionAsync(_projectedStructureIds, skipProps);
        }
        else
        {
            fullObjects = await _sourceQuery.ToListAsync();
        }


        // ⭐ CRITICAL: Disable lazy loading BEFORE applying projection!
        // Data already loaded from SQL (search_objects_with_projection_by_paths),
        // result will be anonymous type — lazy loading not needed and dangerous.
        foreach (var obj in fullObjects)
        {
            obj._lazyLoader = null;
        }

        var compiledProjection = _projection.Compile();
        
        // Apply projection to each object
        var projectedResults = fullObjects.Select(redbObj => compiledProjection(redbObj));
        
        // Apply Where filters after projection
        foreach (var wherePredicate in _wherePredicates)
        {
            var compiledPredicate = wherePredicate.Compile();
            projectedResults = projectedResults.Where(compiledPredicate);
        }
        
        // Apply OrderBy sorting after projection
        if (_orderByExpressions.Count > 0)
        {
            IOrderedEnumerable<TResult>? orderedResults = null;
            
            for (int i = 0; i < _orderByExpressions.Count; i++)
            {
                var (keySelector, isDescending) = _orderByExpressions[i];
                
                // Compile expression dynamically
                var compiledDelegate = ((LambdaExpression)keySelector).Compile();
                
                if (i == 0)
                {
                    // First sort - use Func&lt;TResult, object&gt; for universality
                    Func<TResult, object> universalKeySelector = item => compiledDelegate.DynamicInvoke(item) ?? new object();
                    
                    orderedResults = isDescending 
                        ? projectedResults.OrderByDescending(universalKeySelector)
                        : projectedResults.OrderBy(universalKeySelector);
                }
                else
                {
                    // Subsequent sorts (ThenBy)
                    Func<TResult, object> universalKeySelector = item => compiledDelegate.DynamicInvoke(item) ?? new object();
                    
                    orderedResults = isDescending
                        ? orderedResults!.ThenByDescending(universalKeySelector)
                        : orderedResults!.ThenBy(universalKeySelector);
                }
            }
            
            if (orderedResults != null)
                projectedResults = orderedResults;
        }
        
        // Apply Distinct if flag is set
        if (_isDistinct)
        {
            projectedResults = projectedResults.Distinct();
        }
        
        return projectedResults.ToList();
    }

    public async Task<int> CountAsync()
    {
        // If there are Where operations after projection, need to consider them
        if (_wherePredicates.Count > 0)
        {
            var results = await ToListAsync();
            return results.Count;
        }
        
        // Otherwise count doesn't depend on projection
        return await _sourceQuery.CountAsync();
    }

    public async Task<TResult?> FirstOrDefaultAsync()
    {
        // For FirstOrDefault apply all operations and take first element
        var results = await ToListAsync();
        return results.FirstOrDefault();
    }
    
    /// <summary>
    /// Get projection info: SQL function and structure_ids that will be loaded.
    /// </summary>
    public async Task<string> GetProjectionInfoAsync()
    {
        HashSet<long>? structureIds = _projectedStructureIds;
        
        if (structureIds == null && _provider != null && _schemeId > 0)
        {
            var scheme = await _provider.GetSchemeAsync(_schemeId);
            if (scheme != null)
            {
                structureIds = _fieldExtractor.ExtractStructureIds(scheme, _projection);
                _projectedStructureIds = structureIds;
            }
        }
        
        var count = structureIds?.Count ?? 0;
        
        return count > 0 
            ? $"Projection: {count} structure_ids" 
            : "Full load";
    }
}
