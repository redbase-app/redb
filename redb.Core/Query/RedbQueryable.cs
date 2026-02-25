using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;
using System.Threading.Tasks;
using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.FacetFilters;

namespace redb.Core.Query;

/// <summary>
/// Basic implementation of IRedbQueryable
/// </summary>
public class RedbQueryable<TProps> : IRedbQueryable<TProps>, IOrderedRedbQueryable<TProps>
    where TProps : class, new()
{
    protected readonly IRedbQueryProvider _provider;
    protected readonly QueryContext<TProps> _context;
    protected readonly IFilterExpressionParser _filterParser;
    protected readonly IOrderingExpressionParser _orderingParser;
    protected readonly IFacetFilterBuilder? _facetBuilder;

    public RedbQueryable(
        IRedbQueryProvider provider,
        QueryContext<TProps> context,
        IFilterExpressionParser filterParser,
        IOrderingExpressionParser orderingParser,
        IFacetFilterBuilder? facetBuilder = null)
    {
        _provider = provider;
        _context = context;
        _filterParser = filterParser;
        _orderingParser = orderingParser;
        _facetBuilder = facetBuilder;
    }

    public virtual IRedbQueryable<TProps> Where(Expression<Func<TProps, bool>> predicate)
    {
        var newContext = _context.Clone();
        var filterExpression = _filterParser.ParseFilter(predicate);
        
        // ✅ FIX: Check for empty filter (Where(x => false))
        if (IsEmptyFilter(filterExpression))
        {
            newContext.IsEmpty = true;
        }
        
        // If filter already exists, combine via AND
        if (newContext.Filter != null)
        {
            newContext.Filter = new LogicalExpression(
                LogicalOperator.And,
                new[] { newContext.Filter, filterExpression }
            );
        }
        else
        {
            newContext.Filter = filterExpression;
        }

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IRedbQueryable<TProps> WhereRedb(Expression<Func<IRedbObject, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        var newContext = _context.Clone();
        
        // Parse expression for base IRedbObject fields
        var filterExpression = _filterParser.ParseRedbFilter(predicate);
        
        // ✅ Check for empty filter
        if (IsEmptyFilter(filterExpression))
        {
            newContext.IsEmpty = true;
        }
        
        // If filter already exists, combine via AND
        if (newContext.Filter != null)
        {
            newContext.Filter = new LogicalExpression(
                LogicalOperator.And,
                new[] { newContext.Filter, filterExpression }
            );
        }
        else
        {
            newContext.Filter = filterExpression;
        }

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IOrderedRedbQueryable<TProps> OrderBy<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        var newContext = _context.Clone();
        newContext.Orderings.Clear(); // OrderBy replaces previous sorting
        
        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Ascending);
        newContext.Orderings.Add(ordering);
        
        // ✅ FIX: Preserve IsEmpty flag after OrderBy
        // Even if ordering is added, query can remain empty (Where(x => false))

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IOrderedRedbQueryable<TProps> OrderByDescending<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        var newContext = _context.Clone();
        newContext.Orderings.Clear(); // OrderByDescending replaces previous sorting
        
        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Descending);
        newContext.Orderings.Add(ordering);
        
        // ✅ FIX: Preserve IsEmpty flag after OrderByDescending
        // IsEmpty is already copied in Clone(), no additional actions required

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IOrderedRedbQueryable<TProps> ThenBy<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        var newContext = _context.Clone();
        
        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Ascending);
        newContext.Orderings.Add(ordering);
        
        // ✅ FIX: Preserve IsEmpty flag after ThenBy  
        // IsEmpty is already copied in Clone(), no additional actions required

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IOrderedRedbQueryable<TProps> ThenByDescending<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        var newContext = _context.Clone();
        
        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Descending);
        newContext.Orderings.Add(ordering);
        
        // ✅ FIX: Preserve IsEmpty flag after ThenByDescending
        // IsEmpty is already copied in Clone(), no additional actions required

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // SORTING BY BASE RedbObject FIELDS (OrderByRedb, ThenByRedb)
    // ═══════════════════════════════════════════════════════════════════════════

    public virtual IOrderedRedbQueryable<TProps> OrderByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        var newContext = _context.Clone();
        newContext.Orderings.Clear(); // OrderByRedb replaces previous sorting
        
        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Ascending);
        newContext.Orderings.Add(ordering);

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IOrderedRedbQueryable<TProps> OrderByDescendingRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        var newContext = _context.Clone();
        newContext.Orderings.Clear(); // OrderByDescendingRedb replaces previous sorting
        
        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Descending);
        newContext.Orderings.Add(ordering);

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IOrderedRedbQueryable<TProps> ThenByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        var newContext = _context.Clone();
        
        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Ascending);
        newContext.Orderings.Add(ordering); // ThenByRedb adds to existing sorting

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IOrderedRedbQueryable<TProps> ThenByDescendingRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        var newContext = _context.Clone();
        
        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Descending);
        newContext.Orderings.Add(ordering); // ThenByDescendingRedb adds to existing sorting

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IRedbQueryable<TProps> Take(int count)
    {
        if (count <= 0)
            throw new ArgumentException("Take count must be positive", nameof(count));
            
        var newContext = _context.Clone();
        newContext.Limit = count;

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IRedbQueryable<TProps> Skip(int count)
    {
        if (count < 0)
            throw new ArgumentException("Skip count must be non-negative", nameof(count));
            
        var newContext = _context.Clone();
        newContext.Offset = count;

        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual async Task<List<RedbObject<TProps>>> ToListAsync()
    {
        var result = await _provider.ExecuteAsync(BuildExpression(), typeof(List<RedbObject<TProps>>));
        return (List<RedbObject<TProps>>)result;
    }

    public virtual async Task<int> CountAsync()
    {
        var result = await _provider.ExecuteAsync(BuildCountExpression(), typeof(int));
        return (int)result;
    }

    public virtual async Task<RedbObject<TProps>?> FirstOrDefaultAsync()
    {
        // For FirstOrDefault limit to 1 record
        var limitedContext = _context.Clone();
        limitedContext.Limit = 1;
        
        var tempQueryable = new RedbQueryable<TProps>(_provider, limitedContext, _filterParser, _orderingParser);
        var result = await tempQueryable.ToListAsync();
        
        return result.FirstOrDefault();
    }

    public virtual async Task<RedbObject<TProps>?> FirstOrDefaultAsync(Expression<Func<TProps, bool>> predicate)
    {
        return await Where(predicate).FirstOrDefaultAsync();
    }

    public virtual async Task<bool> AnyAsync()
    {
        var count = await CountAsync();
        return count > 0;
    }

    public virtual async Task<bool> AnyAsync(Expression<Func<TProps, bool>> predicate)
    {
        // Create new query with additional filter
        var filteredQuery = Where(predicate);
        return await filteredQuery.AnyAsync();
    }

    public virtual IRedbQueryable<TProps> WhereIn<TValue>(Expression<Func<TProps, TValue>> selector, IEnumerable<TValue> values)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));
        if (values == null)
            throw new ArgumentNullException(nameof(values));

        var valuesList = values.ToList();
        if (!valuesList.Any())
        {
            // If list is empty, return query that finds nothing
            return Where(_ => false);
        }

        // Create expression: x => values.Contains(selector(x))
        var parameter = selector.Parameters[0];
        var selectorBody = selector.Body;
        
        // Create constant with list of values
        var valuesConstant = Expression.Constant(valuesList);
        
        // Create Contains call
        var containsMethod = typeof(List<TValue>).GetMethod("Contains", new[] { typeof(TValue) });
        var containsCall = Expression.Call(valuesConstant, containsMethod!, selectorBody);
        
        // Create lambda expression
        var lambda = Expression.Lambda<Func<TProps, bool>>(containsCall, parameter);
        
        return Where(lambda);
    }

    public virtual IRedbQueryable<TProps> WhereInRedb<TValue>(Expression<Func<IRedbObject, TValue>> selector, IEnumerable<TValue> values)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));
        if (values == null)
            throw new ArgumentNullException(nameof(values));

        var valuesList = values.ToList();
        if (!valuesList.Any())
        {
            // Empty list = finds nothing
            return WhereRedb(_ => false);
        }

        // Create expression: x => values.Contains(selector(x))
        var parameter = selector.Parameters[0];
        var selectorBody = selector.Body;
        
        // Create constant with list of values
        var valuesConstant = Expression.Constant(valuesList);
        
        // Create Contains call
        var containsMethod = typeof(List<TValue>).GetMethod("Contains", new[] { typeof(TValue) });
        var containsCall = Expression.Call(valuesConstant, containsMethod!, selectorBody);
        
        // Create lambda expression for IRedbObject
        var lambda = Expression.Lambda<Func<IRedbObject, bool>>(containsCall, parameter);
        
        return WhereRedb(lambda);
    }

    public virtual async Task<bool> AllAsync(Expression<Func<TProps, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        // All() == true if all records satisfy the condition
        var totalCount = await CountAsync();
        if (totalCount == 0)
            return true; // All elements of empty set satisfy any condition

        var matchingCount = await Where(predicate).CountAsync();
        return totalCount == matchingCount;
    }

    public virtual IRedbProjectedQueryable<TResult> Select<TResult>(Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        if (selector == null)
            throw new ArgumentNullException(nameof(selector));

        return new RedbProjectedQueryable<TProps, TResult>(this, selector, _provider, _context.SchemeId);
    }
    
    /// <summary>
    /// Internal method for optimized loading with structure_ids filter.
    /// Virtual for overriding in PostgresTreeQueryable (List TreeRedbObject → List RedbObject).
    /// Text paths are taken from context (_context.ProjectedFieldPaths).
    /// </summary>
    /// <param name="projectedStructureIds">
    /// Structure IDs extracted from projection expression. Used by Pro's ProLazyPropsLoader
    /// to load only matching _values rows. Null = no projection filter.
    /// </param>
    /// <param name="skipPropsLoading">
    /// When true, tells MaterializeResultsFromJson to skip ALL Props post-processing
    /// (both eager LoadPropsForManyAsync and lazy loader setup). This is required for
    /// projections because Props data already comes from the SQL projection function
    /// (search_objects_with_projection_by_paths) embedded in the JSON "properties" field.
    /// Re-loading would overwrite partial Props with full object data, wasting the projection.
    /// </param>
    protected internal virtual async Task<List<RedbObject<TProps>>> ToListWithProjectionAsync(
        HashSet<long>? projectedStructureIds, 
        bool skipPropsLoading = false)
    {
        // Set structure_ids in context for passing to provider
        if (projectedStructureIds != null && projectedStructureIds.Count > 0)
        {
            _context.ProjectedStructureIds = projectedStructureIds;
        }
        
        if (skipPropsLoading)
        {
            // ⭐ FIX: Skip BOTH Props loading AND lazy loader setup.
            // Projection SQL already returns "properties" with only the needed fields.
            // Deserializer maps them to Props via setter (_propsLoaded = true).
            // Any post-processing (eager or lazy) would overwrite partial Props with full data.
            _context.SkipPropsLoading = true;
            
            // Explicitly disable lazy loading so MaterializeResultsFromJson does NOT set
            // _lazyLoader on the objects. Without this, if global config has lazy=true,
            // the materializer would set _lazyLoader + _propsLoaded=false, and on first
            // .Props access the lazy loader would load the FULL object, defeating projection.
            _context.UseLazyLoading = false;
        }
        
        // Delegate to provider using the same BuildExpression() as ToListAsync()
        var result = await _provider.ExecuteAsync(BuildExpression(), typeof(List<RedbObject<TProps>>));
        return (List<RedbObject<TProps>>)result;
    }
    
    /// <summary>
    /// Scheme ID for access from projections
    /// </summary>
    internal long SchemeId => _context.SchemeId;
    
    /// <summary>
    /// Provider for access from projections
    /// </summary>
    internal IRedbQueryProvider Provider => _provider;
    
    /// <summary>
    /// Sets text paths for SQL function search_objects_with_projection_by_paths.
    /// Called from RedbProjectedQueryable before ToListWithProjectionAsync.
    /// </summary>
    internal void SetProjectedFieldPaths(List<string>? paths)
    {
        if (paths != null && paths.Count > 0)
        {
            _context.ProjectedFieldPaths = paths;
        }
    }

    public virtual IRedbQueryable<TProps> Distinct()
    {
        var newContext = _context.Clone();
        newContext.IsDistinct = true;
        
        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IRedbQueryable<TProps> DistinctRedb()
    {
        var newContext = _context.Clone();
        newContext.IsDistinctRedb = true;
        
        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IRedbQueryable<TProps> DistinctBy<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));
            
        var newContext = _context.Clone();
        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Ascending);
        newContext.DistinctByField = ordering;
        newContext.DistinctByIsBaseField = false;
        
        // CRITICAL: PostgreSQL requires ORDER BY starting with DISTINCT ON fields
        newContext.Orderings.Insert(0, ordering);
        
        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IRedbQueryable<TProps> DistinctByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));
            
        var newContext = _context.Clone();
        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Ascending);
        newContext.DistinctByField = ordering;
        newContext.DistinctByIsBaseField = true;
        
        // CRITICAL: PostgreSQL requires ORDER BY starting with DISTINCT ON fields
        newContext.Orderings.Insert(0, ordering);
        
        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IRedbQueryable<TProps> WithMaxRecursionDepth(int depth)
    {
        if (depth < 1)
            throw new ArgumentException("Max recursion depth must be positive", nameof(depth));
            
        var newContext = _context.Clone();
        newContext.MaxRecursionDepth = depth;
        
        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    public virtual IRedbQueryable<TProps> WithLazyLoading(bool enabled = true)
    {
        var newContext = _context.Clone();
        newContext.UseLazyLoading = enabled;
        
        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    /// <summary>
    /// Builds expression for query execution
    /// </summary>
    protected virtual Expression BuildExpression()
    {
        // Expression representing the entire query will be created here
        // Return stub for now - concrete implementation will be in provider
        return Expression.Constant(_context);
    }

    /// <summary>
    /// Builds expression for record count
    /// </summary>
    protected virtual Expression BuildCountExpression()
    {
        // Similarly for Count
        return Expression.Constant(_context);
    }
    
    /// <summary>
    /// NEW METHOD: Determines if filter is empty (Where(x =&gt; false))
    /// </summary>
    private bool IsEmptyFilter(FilterExpression filter)
    {
        // Check for constant false filter (created for Where(x => false))
        if (filter is ComparisonExpression comparison && 
            comparison.Property.Name == "__constant" &&
            comparison.Property.Type == typeof(bool) &&
            comparison.Operator == ComparisonOperator.Equal &&
            comparison.Value is bool boolValue && 
            boolValue == false)
        {
            return true;
        }
        
        return false;
    }

    // ===== TREE METHODS =====
    // For base RedbQueryable these methods throw NotSupportedException
    // PostgresTreeQueryable overrides them with real implementation

    public virtual IRedbQueryable<TProps> WhereHasAncestor<TTarget>(
        Expression<Func<TTarget, bool>> ancestorCondition, 
        int? maxDepth = null) 
        where TTarget : class
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereHasDescendant<TTarget>(
        Expression<Func<TTarget, bool>> descendantCondition, 
        int? maxDepth = null) 
        where TTarget : class
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereLevel(int level)
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereLevel(Expression<Func<int, bool>> levelCondition)
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereRoots()
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereLeaves()
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereChildrenOf(long parentId)
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereChildrenOf(IRedbObject parentObject)
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereDescendantsOf(long ancestorId, int? maxDepth = null)
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WhereDescendantsOf(IRedbObject ancestorObject, int? maxDepth = null)
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WithMaxDepth(int depth)
    {
        throw new NotSupportedException(
            "Tree methods are not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual IRedbQueryable<TProps> WithPropsDepth(int depth)
    {
        var newContext = _context.Clone();
        newContext.PropsDepth = depth;
        return new RedbQueryable<TProps>(_provider, newContext, _filterParser, _orderingParser, _facetBuilder);
    }

    // ===== MATERIALIZATION METHODS =====

    public virtual async Task<List<TreeRedbObject<TProps>>> ToTreeListAsync()
    {
        throw new NotSupportedException(
            "ToTreeListAsync is not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual async Task<List<TreeRedbObject<TProps>>> ToFlatListAsync()
    {
        throw new NotSupportedException(
            "ToFlatListAsync is not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual async Task<List<ITreeRedbObject>> ToRootListAsync()
    {
        throw new NotSupportedException(
            "ToRootListAsync is not supported in non-tree queries. " +
            "Use ITreeQueryProvider.CreateTreeQuery() to create queries with tree support.");
    }

    public virtual async Task<int> DeleteAsync()
    {
        // Pass FilterExpression directly (Pro uses it for PVT-based deletion, OpenSource falls back to facet-JSON)
        return await _provider.ExecuteDeleteAsync(_context.SchemeId, _context.Filter);
    }
    
    // ===== AGGREGATIONS (EAV) =====
    
    public virtual async Task<decimal> SumAsync<TField>(Expression<Func<TProps, TField>> selector) 
        where TField : struct
    {
        var fieldPath = ExtractFieldPath(selector);
        
        // Pass FilterExpression directly (Pro uses it, Free falls back to facet-JSON)
        var result = await _provider.ExecuteAggregateAsync(
            _context.SchemeId, 
            fieldPath, 
            Aggregation.AggregateFunction.Sum,
            _context.Filter);
        
        return result ?? 0m;
    }
    
    public virtual async Task<decimal> AverageAsync<TField>(Expression<Func<TProps, TField>> selector) 
        where TField : struct
    {
        var fieldPath = ExtractFieldPath(selector);
        
        // Pass FilterExpression directly (Pro uses it, Free falls back to facet-JSON)
        var result = await _provider.ExecuteAggregateAsync(
            _context.SchemeId, 
            fieldPath, 
            Aggregation.AggregateFunction.Average,
            _context.Filter);
        
        return result ?? 0m;
    }
    
    public virtual async Task<TField?> MinAsync<TField>(Expression<Func<TProps, TField>> selector) 
where TField : struct
    {
        var fieldPath = ExtractFieldPath(selector);
        
        // Pass FilterExpression directly (Pro uses it, Free falls back to facet-JSON)
        var result = await _provider.ExecuteAggregateAsync(
            _context.SchemeId, 
            fieldPath, 
            Aggregation.AggregateFunction.Min,
            _context.Filter);
        
        if (result == null) return null;
        return (TField)Convert.ChangeType(result.Value, typeof(TField));
    }
    
    public virtual async Task<TField?> MaxAsync<TField>(Expression<Func<TProps, TField>> selector) 
        where TField : struct
    {
        var fieldPath = ExtractFieldPath(selector);
        
        // Pass FilterExpression directly (Pro uses it, Free falls back to facet-JSON)
        var result = await _provider.ExecuteAggregateAsync(
            _context.SchemeId, 
            fieldPath, 
            Aggregation.AggregateFunction.Max,
            _context.Filter);
        
        if (result == null) return null;
        return (TField)Convert.ChangeType(result.Value, typeof(TField));
    }
    
    // ===== AGGREGATIONS FOR BASE FIELDS =====
    
    /// <summary>
    /// Sum of base IRedbObject field values (ValueLong, Key, etc.)
    /// SQL: uses aggregate_grouped with empty grouping for base fields
    /// </summary>
    public virtual async Task<decimal> SumRedbAsync<TField>(Expression<Func<IRedbObject, TField>> selector)
        where TField : struct
    {
        var fieldPath = ExtractFieldPathRedb(selector);
        var filterJson = BuildFilterJson();
        
        // Use aggregate_grouped with empty group_fields
        var groupFields = Array.Empty<Grouping.GroupFieldRequest>();
        var aggregations = new[] { new Aggregation.AggregateRequest 
        { 
            FieldPath = $"0$:{fieldPath}", 
            Function = Aggregation.AggregateFunction.Sum, 
            Alias = "result" 
        }};
        
        var jsonResult = await _provider.ExecuteGroupedAggregateAsync(
            _context.SchemeId, groupFields, aggregations, filterJson);
        
        if (jsonResult != null && jsonResult.RootElement.GetArrayLength() > 0)
        {
            var firstRow = jsonResult.RootElement[0];
            if (firstRow.TryGetProperty("result", out var value))
            {
                return value.GetDecimal();
            }
        }
        
        return 0m;
    }
    
    /// <summary>
    /// Average value of base IRedbObject field
    /// SQL: uses aggregate_grouped with empty grouping
    /// </summary>
    public virtual async Task<decimal> AverageRedbAsync<TField>(Expression<Func<IRedbObject, TField>> selector)
        where TField : struct
    {
        var fieldPath = ExtractFieldPathRedb(selector);
        var filterJson = BuildFilterJson();
        
        var groupFields = Array.Empty<Grouping.GroupFieldRequest>();
        var aggregations = new[] { new Aggregation.AggregateRequest 
        { 
            FieldPath = $"0$:{fieldPath}", 
            Function = Aggregation.AggregateFunction.Average, 
            Alias = "result" 
        }};
        
        var jsonResult = await _provider.ExecuteGroupedAggregateAsync(
            _context.SchemeId, groupFields, aggregations, filterJson);
        
        if (jsonResult != null && jsonResult.RootElement.GetArrayLength() > 0)
        {
            var firstRow = jsonResult.RootElement[0];
            if (firstRow.TryGetProperty("result", out var value))
                return value.GetDecimal();
        }
        
        return 0m;
    }
    
    /// <summary>
    /// Minimum value of base IRedbObject field (ValueLong, Key, DateCreate, etc.)
    /// SQL: uses aggregate_grouped with empty grouping
    /// </summary>
    public virtual async Task<TField?> MinRedbAsync<TField>(Expression<Func<IRedbObject, TField>> selector)
        where TField : struct
    {
        var fieldPath = ExtractFieldPathRedb(selector);
        var filterJson = BuildFilterJson();
        
        var groupFields = Array.Empty<Grouping.GroupFieldRequest>();
        var aggregations = new[] { new Aggregation.AggregateRequest 
        { 
            FieldPath = $"0$:{fieldPath}", 
            Function = Aggregation.AggregateFunction.Min, 
            Alias = "result" 
        }};
        
        var jsonResult = await _provider.ExecuteGroupedAggregateAsync(
            _context.SchemeId, groupFields, aggregations, filterJson);
        
        if (jsonResult != null && jsonResult.RootElement.GetArrayLength() > 0)
        {
            var firstRow = jsonResult.RootElement[0];
            if (firstRow.TryGetProperty("result", out var value) && value.ValueKind != JsonValueKind.Null)
                return (TField?)Utils.JsonValueConverter.Convert(value, typeof(TField));
        }
        
        return null;
    }
    
    /// <summary>
    /// Maximum value of base IRedbObject field (ValueLong, Key, DateCreate, etc.)
    /// SQL: uses aggregate_grouped with empty grouping
    /// </summary>
    public virtual async Task<TField?> MaxRedbAsync<TField>(Expression<Func<IRedbObject, TField>> selector)
        where TField : struct
    {
        var fieldPath = ExtractFieldPathRedb(selector);
        var filterJson = BuildFilterJson();
        
        var groupFields = Array.Empty<Grouping.GroupFieldRequest>();
        var aggregations = new[] { new Aggregation.AggregateRequest 
        { 
            FieldPath = $"0$:{fieldPath}", 
            Function = Aggregation.AggregateFunction.Max, 
            Alias = "result" 
        }};
        
        var jsonResult = await _provider.ExecuteGroupedAggregateAsync(
            _context.SchemeId, groupFields, aggregations, filterJson);
        
        if (jsonResult != null && jsonResult.RootElement.GetArrayLength() > 0)
        {
            var firstRow = jsonResult.RootElement[0];
            if (firstRow.TryGetProperty("result", out var value) && value.ValueKind != JsonValueKind.Null)
                return (TField?)Utils.JsonValueConverter.Convert(value, typeof(TField));
        }
        
        return null;
    }
    
    /// <summary>
    /// Get field statistics (Sum, Avg, Min, Max, Count) in one call
    /// Executed as ONE SQL query!
    /// </summary>
    public virtual async Task<Aggregation.FieldStatistics<TField>> GetStatisticsAsync<TField>(
        Expression<Func<TProps, TField>> selector) where TField : struct
    {
        var fieldPath = ExtractFieldPath(selector);
        var filterJson = BuildFilterJson();
        var schemeId = _context.SchemeId;
        
        // ONE SQL query for all aggregations!
        var requests = new[]
        {
            new Aggregation.AggregateRequest { FieldPath = fieldPath, Function = Aggregation.AggregateFunction.Sum, Alias = "Sum" },
            new Aggregation.AggregateRequest { FieldPath = fieldPath, Function = Aggregation.AggregateFunction.Average, Alias = "Average" },
            new Aggregation.AggregateRequest { FieldPath = fieldPath, Function = Aggregation.AggregateFunction.Min, Alias = "Min" },
            new Aggregation.AggregateRequest { FieldPath = fieldPath, Function = Aggregation.AggregateFunction.Max, Alias = "Max" },
            new Aggregation.AggregateRequest { FieldPath = fieldPath, Function = Aggregation.AggregateFunction.Count, Alias = "Count" }
        };
        
        var batchResult = await _provider.ExecuteAggregateBatchAsync(schemeId, requests, filterJson);
        
        return new Aggregation.FieldStatistics<TField>
        {
            Sum = batchResult.Get<decimal>("Sum") ?? 0m,
            Average = batchResult.Get<decimal>("Average") ?? 0m,
            Min = batchResult.Get<TField>("Min"),
            Max = batchResult.Get<TField>("Max"),
            Count = batchResult.Get<int>("Count") ?? 0
        };
    }
    
    /// <summary>
    /// Flexible aggregation - choose what to aggregate via Agg.Sum/Avg/Min/Max/Count
    /// Executed as ONE SQL query (aggregate_batch)!
    /// </summary>
    public virtual async Task<TResult> AggregateAsync<TResult>(Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        var filterJson = BuildFilterJson();
        var schemeId = _context.SchemeId;
        
        // Parse expression and find all Agg.* calls
        var aggregations = ParseAggregateExpression(selector);
        
        // ONE SQL query for all aggregations!
        var requests = aggregations.Select(agg => new Aggregation.AggregateRequest
        {
            FieldPath = agg.FieldPath,
            Function = agg.Function,
            Alias = agg.PropertyName
        });
        
        var batchResult = await _provider.ExecuteAggregateBatchAsync(schemeId, requests, filterJson);
        
        // Build result
        return BuildAggregateResult<TResult>(selector, batchResult.Values);
    }
    
    /// <summary>
    /// Parses AggregateAsync expression and extracts aggregations
    /// </summary>
    private List<(string PropertyName, string FieldPath, Aggregation.AggregateFunction Function)> ParseAggregateExpression<TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector)
    {
        var result = new List<(string PropertyName, string FieldPath, Aggregation.AggregateFunction Function)>();
        
        if (selector.Body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var propName = newExpr.Members?[i].Name ?? $"Item{i}";
                var arg = newExpr.Arguments[i];
                
                if (arg is MethodCallExpression methodCall && methodCall.Method.DeclaringType == typeof(Aggregation.Agg))
                {
                    var funcName = methodCall.Method.Name;
                    var function = funcName switch
                    {
                        "Sum" => Aggregation.AggregateFunction.Sum,
                        "Average" => Aggregation.AggregateFunction.Average,
                        "Min" => Aggregation.AggregateFunction.Min,
                        "Max" => Aggregation.AggregateFunction.Max,
                        "Count" => Aggregation.AggregateFunction.Count,
                        _ => throw new NotSupportedException($"Unknown aggregation: {funcName}")
                    };
                    
                    // Extract field path
                    string fieldPath = "*"; // for Count()
                    if (methodCall.Arguments.Count > 0)
                    {
                        fieldPath = ExtractFieldPathFromExpression(methodCall.Arguments[0]);
                    }
                    
                    result.Add((propName, fieldPath, function));
                }
            }
        }
        
        return result;
    }
    
    /// <summary>
    /// Flexible aggregation ONLY for base IRedbObject fields
    /// Executed as ONE SQL query (aggregate_grouped without grouping)
    /// </summary>
    public virtual async Task<TResult> AggregateRedbAsync<TResult>(Expression<Func<IRedbObject, TResult>> selector)
    {
        var filterJson = BuildFilterJson();
        var schemeId = _context.SchemeId;
        
        // Parse expression and find all Agg.* calls
        var aggregations = ParseAggregateRedbExpression(selector);
        
        // Use aggregate_grouped with EMPTY grouping
        var groupFields = Array.Empty<Grouping.GroupFieldRequest>();
        var requests = aggregations.Select(agg => new Aggregation.AggregateRequest
        {
            // COUNT(*) - don't add 0$: (field is empty or "*")
            // For other base fields - add 0$:
            FieldPath = (agg.Function == Aggregation.AggregateFunction.Count && 
                        (string.IsNullOrEmpty(agg.FieldPath) || agg.FieldPath == "*"))
                ? agg.FieldPath  // COUNT(*) - leave as is
                : $"0$:{agg.FieldPath}",  // Base field - add prefix
            Function = agg.Function,
            Alias = agg.PropertyName
        });
        
        var jsonResult = await _provider.ExecuteGroupedAggregateAsync(schemeId, groupFields, requests, filterJson);
        
        // Parse result (first row, since no grouping)
        if (jsonResult != null && jsonResult.RootElement.GetArrayLength() > 0)
        {
            var firstRow = jsonResult.RootElement[0];
            return BuildAggregateRedbResultFromJson<TResult>(selector, firstRow);
        }
        
        throw new InvalidOperationException("AggregateRedbAsync returned no results");
    }
    
    /// <summary>
    /// Parses AggregateRedbAsync expression for base IRedbObject fields
    /// </summary>
    private List<(string PropertyName, string FieldPath, Aggregation.AggregateFunction Function)> ParseAggregateRedbExpression<TResult>(
        Expression<Func<IRedbObject, TResult>> selector)
    {
        var result = new List<(string PropertyName, string FieldPath, Aggregation.AggregateFunction Function)>();
        
        if (selector.Body is NewExpression newExpr)
        {
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var propName = newExpr.Members?[i].Name ?? $"Item{i}";
                var arg = newExpr.Arguments[i];
                
                if (arg is MethodCallExpression methodCall && methodCall.Method.DeclaringType == typeof(Aggregation.Agg))
                {
                    var funcName = methodCall.Method.Name;
                    var function = funcName switch
                    {
                        "Sum" => Aggregation.AggregateFunction.Sum,
                        "Average" => Aggregation.AggregateFunction.Average,
                        "Min" => Aggregation.AggregateFunction.Min,
                        "Max" => Aggregation.AggregateFunction.Max,
                        "Count" => Aggregation.AggregateFunction.Count,
                        _ => throw new NotSupportedException($"Unknown aggregation: {funcName}")
                    };
                    
                    // Extract base field path (without Props!)
                    string fieldPath = "*"; // for Count()
                    if (methodCall.Arguments.Count > 0)
                    {
                        // For IRedbObject: extract property name directly
                        if (methodCall.Arguments[0] is MemberExpression member)
                        {
                            fieldPath = member.Member.Name;  // Id, ValueLong, DateCreate, etc.
                        }
                    }
                    
                    result.Add((propName, fieldPath, function));
                }
            }
        }
        
        return result;
    }
    
    /// <summary>
    /// Builds AggregateRedbAsync result from JSON element
    /// </summary>
    private TResult BuildAggregateRedbResultFromJson<TResult>(
        Expression<Func<IRedbObject, TResult>> selector,
        JsonElement jsonRow)
    {
        if (selector.Body is NewExpression newExpr && newExpr.Constructor != null)
        {
            var args = new object?[newExpr.Arguments.Count];
            
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var propName = newExpr.Members?[i].Name ?? $"Item{i}";
                var targetType = newExpr.Constructor.GetParameters()[i].ParameterType;
                
                object? value = null;
                if (jsonRow.TryGetProperty(propName, out var jsonValue))
                {
                    value = Query.Utils.JsonValueConverter.Convert(jsonValue, targetType);
                }
                
                args[i] = ConvertAggregateValue(value, targetType);
            }
            
            return (TResult)newExpr.Constructor.Invoke(args);
        }
        
        throw new ArgumentException("AggregateRedbAsync requires anonymous type creation (new { ... })", nameof(selector));
    }
    
    /// <summary>
    /// Extracts field path from expression
    /// Supports:
    ///   x.Props.Price              → "Price"
    ///   x.Props.Customer.Name      → "Customer.Name"  
    ///   x.Props.Items[2].Price     → "Items[2].Price"   (specific index)
    ///   x.Props.Items[].Price      → "Items[].Price"    (all elements, via Select)
    ///   x.Props.PhoneBook["home"]  → "PhoneBook[home]"  (Dictionary key)
    ///   x.Props.PhoneBook.Values   → "PhoneBook[]"      (all Dictionary values)
    /// </summary>
    private string ExtractFieldPathFromExpression(Expression expr)
    {
        var parts = new List<string>();
        ExtractFieldPathRecursive(expr, parts);
        return string.Join(".", parts);
    }
    
    private void ExtractFieldPathRecursive(Expression? expr, List<string> parts)
    {
        if (expr == null) return;
        
        switch (expr)
        {
            // x.Props.Price or x.Props.Items.Price
            case MemberExpression member:
                // Dictionary.Values → add [] to get all values (like Array[])
                if (member.Member.Name == "Values" && member.Expression != null &&
                    IsDictionaryType(member.Expression.Type))
                {
                    ExtractFieldPathRecursive(member.Expression, parts);
                    if (parts.Count > 0)
                    {
                        parts[^1] = $"{parts[^1]}[]";
                    }
                    // DON'T continue recursion - already processed!
                }
                else
                {
                    if (member.Member.Name != "Props")
                    {
                        parts.Insert(0, member.Member.Name);
                    }
                    ExtractFieldPathRecursive(member.Expression, parts);
                }
                break;
                
            // x.Props.Items[2] - array indexer (int[], string[], etc.)
            case BinaryExpression { NodeType: ExpressionType.ArrayIndex } arrayIndex:
                // IMPORTANT: recursion first, then modification!
                ExtractFieldPathRecursive(arrayIndex.Left, parts);
                if (arrayIndex.Right is ConstantExpression indexConst && parts.Count > 0)
                {
                    var index = Convert.ToInt32(indexConst.Value);
                    // Add index to last element: "Scores1" → "Scores1[2]"
                    parts[^1] = $"{parts[^1]}[{index}]";
                }
                break;
                
            // x.Props.Items.get_Item(2) - indexer for List&lt;T&gt; or Dictionary&lt;K,V&gt;
            case MethodCallExpression { Method.Name: "get_Item" } indexerCall:
                if (indexerCall.Arguments.Count > 0 && 
                    indexerCall.Arguments[0] is ConstantExpression idxConst)
                {
                    // FIX: For nested paths like AddressBook["home"].City
                    // Recursion uses Insert(0, ...), so new element will be at the beginning
                    var countBefore = parts.Count;
                    ExtractFieldPathRecursive(indexerCall.Object, parts);
                    
                    // If element was added, it's at the beginning (Insert(0, ...))
                    if (parts.Count > countBefore)
                    {
                        // Dictionary&lt;K,V&gt;: key can be string or other type
                        // List&lt;T&gt;: key is always int
                        var keyValue = idxConst.Value;
                        if (keyValue is int intKey)
                        {
                            parts[0] = $"{parts[0]}[{intKey}]";
                        }
                        else
                        {
                            // Dictionary key (string, etc.)
                            parts[0] = $"{parts[0]}[{keyValue}]";
                        }
                    }
                }
                break;
                
            // x.Props.Items.ElementAt(2)
            case MethodCallExpression { Method.Name: "ElementAt" } elementAtCall:
                if (elementAtCall.Arguments.Count > 1 && 
                    elementAtCall.Arguments[1] is ConstantExpression elemConst)
                {
                    var index = Convert.ToInt32(elemConst.Value);
                    ExtractFieldPathRecursive(elementAtCall.Arguments[0], parts);
                    if (parts.Count > 0)
                    {
                        parts[^1] = $"{parts[^1]}[{index}]";
                    }
                }
                else if (elementAtCall.Arguments.Count == 1 && 
                         elementAtCall.Arguments[0] is ConstantExpression singleConst)
                {
                    // For instance method
                    var index = Convert.ToInt32(singleConst.Value);
                    ExtractFieldPathRecursive(elementAtCall.Object, parts);
                    if (parts.Count > 0)
                    {
                        parts[^1] = $"{parts[^1]}[{index}]";
                    }
                }
                break;
                
            // x.Props.Items.Select(i => i.Price) → Items[].Price (all elements)
            case MethodCallExpression { Method.Name: "Select" } selectCall:
                // Add [] to array
                ExtractFieldPathRecursive(selectCall.Arguments[0], parts);
                if (parts.Count > 0)
                {
                    parts[^1] = $"{parts[^1]}[]";
                }
                // Parse lambda inside Select
                if (selectCall.Arguments.Count > 1 && 
                    selectCall.Arguments[1] is LambdaExpression lambda)
                {
                    var innerParts = new List<string>();
                    ExtractFieldPathRecursive(lambda.Body, innerParts);
                    parts.AddRange(innerParts);
                }
                break;
                
            // x.Props.Items.Sum(i => i.Price) → Items[].Price
            case MethodCallExpression { Method.Name: "Sum" or "Average" or "Min" or "Max" } linqAggCall:
                ExtractFieldPathRecursive(linqAggCall.Arguments[0], parts);
                if (parts.Count > 0)
                {
                    parts[^1] = $"{parts[^1]}[]";
                }
                if (linqAggCall.Arguments.Count > 1 && 
                    linqAggCall.Arguments[1] is LambdaExpression aggLambda)
                {
                    var innerParts = new List<string>();
                    ExtractFieldPathRecursive(aggLambda.Body, innerParts);
                    parts.AddRange(innerParts);
                }
                break;
        }
    }
    
    /// <summary>
    /// Checks if a type is Dictionary&lt;K,V&gt; or IDictionary&lt;K,V&gt;
    /// </summary>
    private static bool IsDictionaryType(Type? type)
    {
        if (type == null) return false;
        if (type.IsGenericType)
        {
            var genericDef = type.GetGenericTypeDefinition();
            if (genericDef == typeof(Dictionary<,>) || genericDef == typeof(IDictionary<,>))
                return true;
        }
        return type.GetInterfaces().Any(i => 
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));
    }
    
    /// <summary>
    /// Creates aggregation result from dictionary of values
    /// </summary>
    private TResult BuildAggregateResult<TResult>(
        Expression<Func<RedbObject<TProps>, TResult>> selector,
        Dictionary<string, object?> results)
    {
        if (selector.Body is NewExpression newExpr && newExpr.Constructor != null)
        {
            var args = new object?[newExpr.Arguments.Count];
            
            for (int i = 0; i < newExpr.Arguments.Count; i++)
            {
                var propName = newExpr.Members?[i].Name ?? $"Item{i}";
                var targetType = newExpr.Constructor.GetParameters()[i].ParameterType;
                results.TryGetValue(propName, out var value);
                
                args[i] = ConvertAggregateValue(value, targetType);
            }
            
            return (TResult)newExpr.Constructor.Invoke(args);
        }
        
        throw new NotSupportedException("AggregateAsync supports only anonymous types");
    }
    
    private object? ConvertAggregateValue(object? value, Type targetType)
    {
        if (value == null)
        {
            return targetType.IsValueType ? Activator.CreateInstance(targetType) : null;
        }
        
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
        
        return underlying switch
        {
            Type t when t == typeof(int) => Convert.ToInt32(value),
            Type t when t == typeof(long) => Convert.ToInt64(value),
            Type t when t == typeof(decimal) => Convert.ToDecimal(value),
            Type t when t == typeof(double) => Convert.ToDouble(value),
            Type t when t == typeof(float) => Convert.ToSingle(value),
            _ => Convert.ChangeType(value, underlying)
        };
    }
    
    /// <summary>
    /// Extracts field path from lambda expression
    /// </summary>
    protected string ExtractFieldPath<TField>(Expression<Func<TProps, TField>> selector)
    {
        var body = selector.Body;
        
        // Handle Convert (for nullable types)
        if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            body = unary.Operand;
        }
        
        var segments = new List<string>();
        var current = body;
        
        while (current is MemberExpression member)
        {
            segments.Insert(0, member.Member.Name);
            current = member.Expression;
        }
        
        if (segments.Count == 0)
            throw new ArgumentException("Cannot extract field path from selector", nameof(selector));
        
        return string.Join(".", segments);
    }
    
    /// <summary>
    /// Extracts base field path from IRedbObject expression
    /// Used for *RedbAsync methods (SumRedbAsync, GroupByRedb, etc.)
    /// </summary>
    protected string ExtractFieldPathRedb<TField>(Expression<Func<IRedbObject, TField>> selector)
    {
        var body = selector.Body;
        
        // Handle Convert (for nullable types)
        if (body is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
        {
            body = unary.Operand;
        }
        
        if (body is MemberExpression member)
        {
            // For IRedbObject: just property name (Id, Name, ValueLong, etc.)
            return member.Member.Name;
        }
        
        throw new ArgumentException("Cannot extract field path from IRedbObject selector", nameof(selector));
    }
    
    /// <summary>
    /// Builds JSON filter from current query context
    /// </summary>
    protected virtual string? BuildFilterJson()
    {
        if (_context.Filter == null || _facetBuilder == null)
            return null;
        
        var json = _facetBuilder.BuildFacetFilters(_context.Filter);
        return string.IsNullOrEmpty(json) || json == "{}" ? null : json;
    }
    
    // ===== GROUPBY =====
    
    /// <summary>
    /// Grouping with aggregations.
    /// Pro version receives FilterExpression directly for proper SQL compilation.
    /// </summary>
    public virtual Grouping.IRedbGroupedQueryable<TKey, TProps> GroupBy<TKey>(
        Expression<Func<TProps, TKey>> keySelector)
    {
        // Pass FilterExpression directly (Pro uses it, Free falls back to facet-JSON)
        return new Grouping.RedbGroupedQueryable<TKey, TProps>(
            _provider, _context.SchemeId, _context.Filter, keySelector);
    }
    
    /// <summary>
    /// Grouping by base IRedbObject fields (id, scheme_id, parent_id, etc.)
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// Pro version receives FilterExpression directly for proper SQL compilation.
    /// </summary>
    public virtual Grouping.IRedbGroupedQueryable<TKey, TProps> GroupByRedb<TKey>(
        Expression<Func<IRedbObject, TKey>> keySelector)
    {
        // Pass FilterExpression directly (Pro uses it, Free falls back to facet-JSON)
        return new Grouping.RedbGroupedQueryable<TKey, TProps>(
            _provider, _context.SchemeId, _context.Filter, keySelector, isBaseFieldGrouping: true);
    }
    
    /// <summary>
    /// Grouping by array elements
    /// </summary>
    public virtual Grouping.IRedbGroupedQueryable<TKey, TItem> GroupByArray<TItem, TKey>(
        Expression<Func<TProps, IEnumerable<TItem>>> arraySelector,
        Expression<Func<TItem, TKey>> keySelector) where TItem : class, new()
    {
        var filterJson = BuildFilterJson();
        return new Grouping.RedbArrayGroupedQueryable<TKey, TItem, TProps>(
            _provider, _context.SchemeId, filterJson, arraySelector, keySelector);
    }
    
    // ===== WINDOW FUNCTIONS =====
    
    /// <summary>
    /// Query with window functions.
    /// Pro version receives FilterExpression directly for proper SQL compilation.
    /// </summary>
    public virtual Window.IRedbWindowedQueryable<TProps> WithWindow(
        Action<Window.IWindowSpec<TProps>> windowConfig)
    {
        var windowSpec = new Window.WindowSpec<TProps>();
        windowConfig(windowSpec);
        // Pass FilterExpression directly (Pro uses it, Free falls back to facet-JSON)
        return new Window.RedbWindowedQueryable<TProps>(
            _provider, _context.SchemeId, _context.Filter, windowSpec, _context.Limit, _context.Offset);
    }
    
    // ===== SQL PREVIEW (like EF Core) =====
    
    /// <summary>
    /// Get SQL representation of query (for debugging).
    /// Analog of EF Core ToQueryString().
    /// Pro version shows PVT SQL, Open Source - redb_json_objects.
    /// Note: Uses Task.Run to avoid SynchronizationContext deadlock in Blazor/ASP.NET.
    /// </summary>
    public virtual string ToSqlString()
    {
        // Use Task.Run to avoid Blazor SynchronizationContext deadlock
        return Task.Run(() => _provider.GetSqlPreviewAsync(_context)).GetAwaiter().GetResult();
    }
    
    /// <summary>
    /// Async version of getting SQL (recommended)
    /// </summary>
    public virtual Task<string> ToSqlStringAsync()
    {
        return _provider.GetSqlPreviewAsync(_context);
    }
    
    /// <summary>
    /// Get JSON filter that will be sent to SQL function (for diagnostics)
    /// </summary>
    public virtual Task<string> ToFilterJsonAsync()
    {
        return _provider.GetFilterJsonAsync(_context);
    }
    
    private string BuildOrderByJson()
    {
        if (!_context.Orderings.Any())
            return "[]";
        
        var orders = _context.Orderings.Select(o => new { 
            field = o.Property.Name, 
            dir = o.Direction.ToString().ToUpper() 
        });
        return System.Text.Json.JsonSerializer.Serialize(orders);
    }
}
