using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Core.Query.QueryExpressions;
using redb.Core.Query.FacetFilters;
using redb.Core.Caching;

namespace redb.Core.Query.Base;

/// <summary>
/// Base tree queryable for hierarchical LINQ queries.
/// Extends RedbQueryable with tree operation support.
/// </summary>
public class TreeQueryableBase<TProps> : RedbQueryable<TProps>
    where TProps : class, new()
{
    protected readonly ITreeQueryProvider _treeProvider;
    protected readonly TreeQueryContext<TProps> _treeContext;
    private readonly IFacetFilterBuilder _facetBuilder;

    public TreeQueryableBase(
        ITreeQueryProvider provider,
        TreeQueryContext<TProps> context,
        IFilterExpressionParser filterParser,
        IOrderingExpressionParser orderingParser,
        IFacetFilterBuilder? facetBuilder = null)
        : base(provider, context, filterParser, orderingParser)
    {
        _treeProvider = provider;
        _treeContext = context;
        _facetBuilder = facetBuilder ?? new FacetFilterBuilder();
    }
    
    /// <summary>
    /// Creates a new instance with the specified context. Override in derived classes.
    /// </summary>
    protected virtual TreeQueryableBase<TProps> CreateInstance(TreeQueryContext<TProps> context)
    {
        return new TreeQueryableBase<TProps>(_treeProvider, context, _filterParser, _orderingParser, _facetBuilder);
    }

    // ===== TREE BASE METHODS OVERRIDE =====

    public override IRedbQueryable<TProps> Where(Expression<Func<TProps, bool>> predicate)
    {
        var newContext = _treeContext.Clone();
        var filterExpression = _filterParser.ParseFilter(predicate);

        // ✅ FIX: Check for empty filter (Where(x => false))
        if (IsEmptyFilter(filterExpression))
        {
            newContext.IsEmpty = true;
        }

        // If there's already a filter, combine with AND
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

        // ✅ FIX: Return PostgresTreeQueryable, not base RedbQueryable
        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereRedb(Expression<Func<IRedbObject, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        var newContext = _treeContext.Clone();
        var filterExpression = _filterParser.ParseRedbFilter(predicate);

        // ✅ Check for empty filter
        if (IsEmptyFilter(filterExpression))
        {
            newContext.IsEmpty = true;
        }

        // If there's already a filter, combine with AND
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

        return CreateInstance(newContext);
    }

    // ===== OVERRIDE SORTING METHODS TO PRESERVE TREE CONTEXT =====

    /// <summary>
    /// ✅ FIX ISSUE #4: Override OrderBy to preserve Tree context
    /// Base OrderBy returns RedbQueryable and loses Tree context!
    /// RETURNS: IOrderedTreeQueryable (inherits from IOrderedRedbQueryable + preserves Tree methods)
    /// </summary>
    public override IOrderedRedbQueryable<TProps> OrderBy<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        var newContext = _treeContext.Clone();
        newContext.Orderings.Clear(); // OrderBy replaces previous sorting

        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Ascending);
        newContext.Orderings.Add(ordering);

        // ✅ CORRECT: PostgresTreeQueryable implements IOrderedTreeQueryable : IOrderedRedbQueryable
        // Return as IOrderedRedbQueryable, but actually it's PostgresTreeQueryable!
        return CreateInstance(newContext);
    }

    /// <summary>
    /// ✅ FIX ISSUE #4: Override OrderByDescending for Tree context
    /// </summary>
    public override IOrderedRedbQueryable<TProps> OrderByDescending<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        var newContext = _treeContext.Clone();
        newContext.Orderings.Clear(); // OrderByDescending replaces previous sorting

        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Descending);
        newContext.Orderings.Add(ordering);

        // ✅ FIX: Return PostgresTreeQueryable, preserving Tree context!
        return CreateInstance(newContext);
    }

    /// <summary>
    /// ✅ FIX ISSUE #4: Override ThenBy for Tree context
    /// </summary>
    public override IOrderedRedbQueryable<TProps> ThenBy<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        var newContext = _treeContext.Clone();

        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Ascending);
        newContext.Orderings.Add(ordering); // ThenBy adds to existing sorting

        // ✅ FIX: Return PostgresTreeQueryable, preserving Tree context!
        return CreateInstance(newContext);
    }

    /// <summary>
    /// ✅ FIX ISSUE #4: Override ThenByDescending for Tree context
    /// </summary>
    public override IOrderedRedbQueryable<TProps> ThenByDescending<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        var newContext = _treeContext.Clone();

        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Descending);
        newContext.Orderings.Add(ordering); // ThenByDescending adds to existing sorting

        // ✅ FIX: Return PostgresTreeQueryable, preserving Tree context!
        return CreateInstance(newContext);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 🆕 SORTING BY BASE RedbObject FIELDS FOR TREE CONTEXT
    // ═══════════════════════════════════════════════════════════════════════════

    public override IOrderedRedbQueryable<TProps> OrderByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        var newContext = _treeContext.Clone();
        newContext.Orderings.Clear();

        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Ascending);
        newContext.Orderings.Add(ordering);

        return CreateInstance(newContext);
    }

    public override IOrderedRedbQueryable<TProps> OrderByDescendingRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        var newContext = _treeContext.Clone();
        newContext.Orderings.Clear();

        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Descending);
        newContext.Orderings.Add(ordering);

        return CreateInstance(newContext);
    }

    public override IOrderedRedbQueryable<TProps> ThenByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        var newContext = _treeContext.Clone();

        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Ascending);
        newContext.Orderings.Add(ordering);

        return CreateInstance(newContext);
    }

    public override IOrderedRedbQueryable<TProps> ThenByDescendingRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        var newContext = _treeContext.Clone();

        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Descending);
        newContext.Orderings.Add(ordering);

        return CreateInstance(newContext);
    }

    // ===== OVERRIDE LAZY LOADING METHODS TO PRESERVE TREE CONTEXT =====

    /// <summary>
    /// ✅ FIX: Override WithLazyLoading to preserve Tree context
    /// Base WithLazyLoading returns RedbQueryable and loses Tree context!
    /// </summary>
    public override IRedbQueryable<TProps> WithLazyLoading(bool enabled = true)
    {
        var newContext = _treeContext.Clone();
        newContext.UseLazyLoading = enabled;
        
        return CreateInstance(newContext);
    }

    // ===== TREE FILTERS =====

    /// <summary>
    /// Filter by ancestors with polymorphic query support
    /// </summary>
    public override IRedbQueryable<TProps> WhereHasAncestor<TTarget>(
        Expression<Func<TTarget, bool>> ancestorCondition, 
        int? maxDepth = null)
    {
        if (ancestorCondition == null)
            throw new ArgumentNullException(nameof(ancestorCondition));

        if (maxDepth.HasValue && maxDepth.Value < 1)
            throw new ArgumentException("maxDepth must be >= 1", nameof(maxDepth));

        var newContext = _treeContext.Clone();

        // Get scheme_id of target type through Cache
        var targetSchemeId = _treeProvider.Cache.GetSchemeIdByClrType(typeof(TTarget));

        // Parse ancestor condition and add tree filter
        var filterExpression = _filterParser.ParseFilter(ancestorCondition);
        var ancestorFilter = new TreeFilter(TreeFilterOperator.HasAncestor)
        {
            FilterConditions = ConvertFilterToJson(filterExpression),
            OriginalFilter = filterExpression,  // Pro: for ProSqlBuilder
            MaxDepth = maxDepth,
            TargetSchemeId = targetSchemeId
        };

        newContext.TreeFilters.Add(ancestorFilter);

        return CreateInstance(newContext);
    }

    /// <summary>
    /// Filter by descendants with polymorphic query support
    /// </summary>
    public override IRedbQueryable<TProps> WhereHasDescendant<TTarget>(
        Expression<Func<TTarget, bool>> descendantCondition, 
        int? maxDepth = null)
    {
        if (descendantCondition == null)
            throw new ArgumentNullException(nameof(descendantCondition));

        if (maxDepth.HasValue && maxDepth.Value < 1)
            throw new ArgumentException("maxDepth must be >= 1", nameof(maxDepth));

        var newContext = _treeContext.Clone();

        // Get scheme_id of target type through Cache
        var targetSchemeId = _treeProvider.Cache.GetSchemeIdByClrType(typeof(TTarget));

        // Parse descendant condition and add tree filter
        var filterExpression = _filterParser.ParseFilter(descendantCondition);
        var descendantFilter = new TreeFilter(TreeFilterOperator.HasDescendant)
        {
            FilterConditions = ConvertFilterToJson(filterExpression),
            OriginalFilter = filterExpression,  // Pro: for ProSqlBuilder
            MaxDepth = maxDepth,
            TargetSchemeId = targetSchemeId
        };

        newContext.TreeFilters.Add(descendantFilter);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereLevel(int level)
    {
        var newContext = _treeContext.Clone();
        var levelFilter = new TreeFilter(TreeFilterOperator.Level, level);
        newContext.TreeFilters.Add(levelFilter);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereLevel(Expression<Func<int, bool>> levelCondition)
    {
        if (levelCondition == null)
            throw new ArgumentNullException(nameof(levelCondition));

        var newContext = _treeContext.Clone();
        var levelFilter = new TreeFilter(TreeFilterOperator.Level);

        // Analyze level condition (e.g.: level => level > 2)
        levelFilter.FilterConditions = ParseLevelCondition(levelCondition);
        newContext.TreeFilters.Add(levelFilter);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereRoots()
    {
        var newContext = _treeContext.Clone();
        var rootFilter = new TreeFilter(TreeFilterOperator.IsRoot);
        newContext.TreeFilters.Add(rootFilter);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereLeaves()
    {
        var newContext = _treeContext.Clone();
        var leafFilter = new TreeFilter(TreeFilterOperator.IsLeaf);
        newContext.TreeFilters.Add(leafFilter);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereChildrenOf(long parentId)
    {
        var newContext = _treeContext.Clone();
        var childrenFilter = new TreeFilter(TreeFilterOperator.ChildrenOf, parentId);
        newContext.TreeFilters.Add(childrenFilter);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereChildrenOf(IRedbObject parentObject)
    {
        if (parentObject == null)
            throw new ArgumentNullException(nameof(parentObject));

        return WhereChildrenOf(parentObject.Id);
    }

    public override IRedbQueryable<TProps> WhereDescendantsOf(long ancestorId, int? maxDepth = null)
    {
        var newContext = _treeContext.Clone();
        var descendantsFilter = new TreeFilter(TreeFilterOperator.DescendantsOf, ancestorId)
        {
            MaxDepth = maxDepth
        };
        newContext.TreeFilters.Add(descendantsFilter);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereDescendantsOf(IRedbObject ancestorObject, int? maxDepth = null)
    {
        if (ancestorObject == null)
            throw new ArgumentNullException(nameof(ancestorObject));

        return WhereDescendantsOf(ancestorObject.Id, maxDepth);
    }

    // ===== MATERIALIZATION METHODS =====
    
    /// <summary>
    /// Execute query and get list of objects (base RedbObject)
    /// TreeRedbObject inherits from RedbObject, so direct casting works
    /// </summary>
    public override async Task<List<RedbObject<TProps>>> ToListAsync()
    {
        // Get TreeRedbObject and cast to RedbObject (they are compatible)
        var treeObjects = await ToFlatListAsync();
        return treeObjects.Cast<RedbObject<TProps>>().ToList();
    }

    /// <summary>
    /// Override ToListWithProjectionAsync for correct type conversion.
    /// Tree provider returns List&lt;TreeRedbObject&gt;, need Cast to List&lt;RedbObject&gt;.
    /// </summary>
    protected internal override async Task<List<RedbObject<TProps>>> ToListWithProjectionAsync(
        HashSet<long>? projectedStructureIds, 
        bool skipPropsLoading = false)
    {
        // Set structure_ids in context for provider
        if (projectedStructureIds != null && projectedStructureIds.Count > 0)
        {
            _treeContext.ProjectedStructureIds = projectedStructureIds;
        }

        if (skipPropsLoading)
        {
            // ⭐ FIX: Same logic as RedbQueryable.ToListWithProjectionAsync.
            // Skip Props re-loading and disable lazy loader — projection SQL already
            // returned partial Props in the JSON, deserialized via Props setter.
            _treeContext.SkipPropsLoading = true;
            _treeContext.UseLazyLoading = false;
        }

        // Get TreeRedbObject and cast to RedbObject via Cast
        var treeObjects = await ToFlatListAsync();
        return treeObjects.Cast<RedbObject<TProps>>().ToList();
    }

    public override async Task<RedbObject<TProps>?> FirstOrDefaultAsync()
    {
        // Limit to 1 record
        var limitedQuery = Take(1);
        var results = await limitedQuery.ToListAsync();
        return results.FirstOrDefault();
    }

    public override IRedbQueryable<TProps> Take(int count)
    {
        if (count <= 0)
            throw new ArgumentException("Take count must be positive", nameof(count));

        var newContext = _treeContext.Clone();
        newContext.Limit = count;

        // ✅ FIX: Return PostgresTreeQueryable, preserving Tree context!
        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> Skip(int count)
    {
        if (count < 0)
            throw new ArgumentException("Skip count must be non-negative", nameof(count));

        var newContext = _treeContext.Clone();
        newContext.Offset = count;

        // ✅ FIX: Return PostgresTreeQueryable, preserving Tree context!
        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WhereIn<TValue>(Expression<Func<TProps, TValue>> selector, IEnumerable<TValue> values)
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

    public override IRedbQueryable<TProps> WhereInRedb<TValue>(Expression<Func<IRedbObject, TValue>> selector, IEnumerable<TValue> values)
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

    public override IRedbQueryable<TProps> Distinct()
    {
        var newContext = _treeContext.Clone();
        newContext.IsDistinct = true;

        // ✅ FIX: Return PostgresTreeQueryable, preserving Tree context!
        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> DistinctRedb()
    {
        var newContext = _treeContext.Clone();
        newContext.IsDistinctRedb = true;

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> DistinctBy<TKey>(Expression<Func<TProps, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        var newContext = _treeContext.Clone();
        var ordering = _orderingParser.ParseOrdering(keySelector, SortDirection.Ascending);
        newContext.DistinctByField = ordering;
        newContext.DistinctByIsBaseField = false;

        // CRITICAL: PostgreSQL requires ORDER BY starting with DISTINCT ON fields
        newContext.Orderings.Insert(0, ordering);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> DistinctByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        var newContext = _treeContext.Clone();
        var ordering = _orderingParser.ParseRedbOrdering(keySelector, SortDirection.Ascending);
        newContext.DistinctByField = ordering;
        newContext.DistinctByIsBaseField = true;

        // CRITICAL: PostgreSQL requires ORDER BY starting with DISTINCT ON fields
        newContext.Orderings.Insert(0, ordering);

        return CreateInstance(newContext);
    }

    public override IRedbQueryable<TProps> WithMaxRecursionDepth(int depth)
    {
        if (depth < 1)
            throw new ArgumentException("Max recursion depth must be positive", nameof(depth));

        var newContext = _treeContext.Clone();
        newContext.MaxRecursionDepth = depth;

        // ✅ FIX: Return PostgresTreeQueryable, preserving Tree context!
        return CreateInstance(newContext);
    }

    public override async Task<int> CountAsync()
    {
        var result = await _treeProvider.ExecuteAsync(BuildCountExpression(), typeof(int));
        return (int)result;
    }

    public override async Task<bool> AnyAsync()
    {
        var count = await CountAsync();
        return count > 0;
    }

    public override async Task<bool> AnyAsync(Expression<Func<TProps, bool>> predicate)
    {
        // Create new query with additional filter
        var filteredQuery = Where(predicate);
        return await filteredQuery.AnyAsync();
    }

    public override async Task<bool> AllAsync(Expression<Func<TProps, bool>> predicate)
    {
        if (predicate == null)
            throw new ArgumentNullException(nameof(predicate));

        // All() == true if all records satisfy condition
        var totalCount = await CountAsync();
        if (totalCount == 0)
            return true; // All elements of empty set satisfy any condition

        var matchingCount = await Where(predicate).CountAsync();
        return totalCount == matchingCount;
    }

    // ===== TREE-SPECIFIC METHODS =====

    /// <summary>
    /// Execute query and return filtered objects with populated Parent/Children chains
    /// Loads objects + all parents to root and establishes links
    /// Children contain only objects from the loaded chain
    /// </summary>
    public override async Task<List<TreeRedbObject<TProps>>> ToTreeListAsync()
    {
        // 1. Get filtered objects (with Props via lazy loading)
        var filteredObjects = await ToFlatListAsync();
        if (!filteredObjects.Any())
            return new List<TreeRedbObject<TProps>>();

        var filteredIds = filteredObjects.Select(o => o.id).ToList();

        // 2. Get IDs of all parents to root
        var allIds = await _treeProvider.GetIdsWithAncestorsAsync<TProps>(filteredIds);

        // 3. FIX: Calculate IDs of ONLY parents (that aren't already loaded)
        var parentIds = allIds.Except(filteredIds).ToList();
        
        List<ITreeRedbObject> allObjectsUntyped;
        
        if (parentIds.Any())
        {
            // 4. Load ONLY parents (don't reload children!)
            var parents = await _treeProvider.LoadObjectsByIdsAsync(parentIds, _treeContext.PropsDepth);

            // 5. Combine parents with already loaded children
            allObjectsUntyped = parents
                .Concat(filteredObjects.Cast<ITreeRedbObject>())
                .ToList();
        }
        else
        {
            // No parents (all objects are root) - use only filteredObjects
            allObjectsUntyped = filteredObjects.Cast<ITreeRedbObject>().ToList();
        }

        // 6. Establish Parent/Children relationships
        BuildRelationshipsUntyped(allObjectsUntyped);

        // 7. Return only filtered objects (filter by TProps type)
        var matchingByIdAndType = allObjectsUntyped
            .Where(o => filteredIds.Contains(o.Id) && o is TreeRedbObject<TProps>)
            .Cast<TreeRedbObject<TProps>>()
            .ToList();
        
        return matchingByIdAndType;
    }

    /// <summary>
    /// Execute query and get list of tree root nodes
    /// Loads objects + all parents to root, establishes links and returns roots
    /// Children contain only objects from the loaded chain
    /// </summary>
    public override async Task<List<ITreeRedbObject>> ToRootListAsync()
    {
        // 1. Get filtered objects (with Props via lazy loading)
        var filteredObjects = await ToFlatListAsync();
        if (!filteredObjects.Any())
            return new List<ITreeRedbObject>();

        var filteredIds = filteredObjects.Select(o => o.id).ToList();

        // 2. Get IDs of all parents to root
        var allIds = await _treeProvider.GetIdsWithAncestorsAsync<TProps>(filteredIds);

        // 3. FIX: Calculate IDs of ONLY parents (that aren't already loaded)
        var parentIds = allIds.Except(filteredIds).ToList();
        
        List<ITreeRedbObject> allObjects;
        
        if (parentIds.Any())
        {
            // 4. Load ONLY parents
            var parents = await _treeProvider.LoadObjectsByIdsAsync(parentIds, _treeContext.PropsDepth);

            // 5. Combine parents with already loaded children
            allObjects = parents
                .Concat(filteredObjects.Cast<ITreeRedbObject>())
                .ToList();
        }
        else
        {
            // No parents - use only filteredObjects
            allObjects = filteredObjects.Cast<ITreeRedbObject>().ToList();
        }

        // 6. Establish Parent/Children relationships
        BuildRelationshipsUntyped(allObjects);

        // 7. Return roots (ParentId == null)
        return allObjects.Where(o => o.ParentId == null).ToList();
    }

    /// <summary>
    /// Execute query and get flat list of tree objects
    /// </summary>
    public override async Task<List<TreeRedbObject<TProps>>> ToFlatListAsync()
    {
        // Execute query through provider
        var expression = BuildTreeExpression();
        var result = await _treeProvider.ExecuteAsync(expression, typeof(List<TreeRedbObject<TProps>>));
        var list = (List<TreeRedbObject<TProps>>)result;
        return list;
    }

    /// <summary>
    /// ✅ NEW METHOD: Determines if filter is empty (Where(x => false))
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

    // ===== LINQ METHODS ARE INHERITED FROM BASE CLASS =====
    // 💀 REMOVED all duplicated Where/OrderBy/Take/Skip/Distinct methods!
    // ✅ Using logic from RedbQueryable - WITHOUT DUPLICATION!

    /// <summary>
    /// Configure maximum search depth in tree
    /// </summary>
    public override IRedbQueryable<TProps> WithMaxDepth(int depth)
    {
        if (depth < 1)
            throw new ArgumentException("Max depth must be positive", nameof(depth));

        // Create new context with required MaxDepth (init-only property)
        var newContext = new TreeQueryContext<TProps>(_treeContext.SchemeId, _treeContext.UserId, _treeContext.CheckPermissions, _treeContext.RootObjectId, depth)
        {
            ParentIds = _treeContext.ParentIds,
            Filter = _treeContext.Filter,
            Orderings = new List<OrderingExpression>(_treeContext.Orderings),
            Limit = _treeContext.Limit,
            Offset = _treeContext.Offset,
            IsDistinct = _treeContext.IsDistinct,
            MaxRecursionDepth = _treeContext.MaxRecursionDepth,
            IsEmpty = _treeContext.IsEmpty
        };

        // Copy tree filters
        newContext.TreeFilters = new List<TreeFilter>(_treeContext.TreeFilters);

        return CreateInstance(newContext);
    }

    /// <summary>
    /// Configure maximum depth for loading nested RedbObject in Props
    /// </summary>
    public override IRedbQueryable<TProps> WithPropsDepth(int depth)
    {
        if (depth < 1)
            throw new ArgumentException("Props depth must be positive", nameof(depth));

        var newContext = _treeContext.Clone();
        newContext.PropsDepth = depth;
        return CreateInstance(newContext);
    }

    public IRedbProjectedQueryable<TResult> Select<TResult>(Expression<Func<TreeRedbObject<TProps>, TResult>> selector)
    {
        return new TreeProjectedQueryable<TProps, TResult>(this, selector);
    }

    // ===== HELPER METHODS =====

    /// <summary>
    /// Build expression for tree query
    /// </summary>
    protected virtual Expression BuildTreeExpression()
    {
        return Expression.Constant(_treeContext);
    }

    /// <summary>
    /// Converts FilterExpression to JSON dictionary.
    /// Uses FacetFilterBuilder for full operator support.
    /// </summary>
    private Dictionary<string, object> ConvertFilterToJson(FilterExpression filter)
    {
        // Uses FacetFilterBuilder for full operator support
        var facetFiltersJson = _facetBuilder.BuildFacetFilters(filter);

        if (facetFiltersJson == "{}")
            return new Dictionary<string, object>();

        // Parse JSON back to Dictionary
        var result = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(facetFiltersJson);
        return result ?? new Dictionary<string, object>();
    }

    /// <summary>
    /// Parse level condition (e.g.: level => level &gt; 2)
    /// </summary>
    private Dictionary<string, object> ParseLevelCondition(Expression<Func<int, bool>> levelCondition)
    {
        // Simplified parsing - analyze lambda body
        if (levelCondition.Body is BinaryExpression binary)
        {
            switch (binary.NodeType)
            {
                case ExpressionType.GreaterThan:
                    if (binary.Right is ConstantExpression gtConstant)
                        return new Dictionary<string, object> { ["$gt"] = gtConstant.Value! };
                    break;
                case ExpressionType.LessThan:
                    if (binary.Right is ConstantExpression ltConstant)
                        return new Dictionary<string, object> { ["$lt"] = ltConstant.Value! };
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    if (binary.Right is ConstantExpression gteConstant)
                        return new Dictionary<string, object> { ["$gte"] = gteConstant.Value! };
                    break;
                case ExpressionType.LessThanOrEqual:
                    if (binary.Right is ConstantExpression lteConstant)
                        return new Dictionary<string, object> { ["$lte"] = lteConstant.Value! };
                    break;
                case ExpressionType.Equal:
                    if (binary.Right is ConstantExpression eqConstant)
                        return new Dictionary<string, object> { ["$eq"] = eqConstant.Value! };
                    break;
            }
        }

        // Fallback - return equality 0 (root level)
        return new Dictionary<string, object> { ["$eq"] = 0 };
    }

    /// <summary>
    /// Establish Parent/Children relationships between loaded objects
    /// Does not filter objects - only establishes bidirectional links
    /// </summary>
    private void BuildRelationships(List<TreeRedbObject<TProps>> objects)
    {
        if (!objects.Any()) return;

        // Create dictionary for fast lookup by ID
        var objectDict = objects.ToDictionary(obj => obj.id, obj => obj);

        // Establish relationships for all objects
        foreach (var obj in objects)
        {
            // Clear Children in case of repeated call
            obj.Children.Clear();
            obj.Parent = null;

            // If parent exists and is in list - establish relationship
            if (obj.parent_id.HasValue && objectDict.TryGetValue(obj.parent_id.Value, out var parent))
            {
                obj.Parent = parent;
                parent.Children.Add(obj);
            }
        }
    }

    /// <summary>
    /// Build Parent/Children relationships for untyped objects
    /// Used for polymorphic trees in ToRootListAsync
    /// </summary>
    private void BuildRelationshipsUntyped(List<ITreeRedbObject> objects)
    {
        if (!objects.Any()) return;

        // Create dictionary for fast lookup by ID
        var objectDict = objects.ToDictionary(obj => obj.Id, obj => obj);

        // Establish relationships for all objects
        foreach (var obj in objects)
        {
            // Clear Children in case of repeated call
            obj.Children.Clear();
            obj.Parent = null;

            // If parent exists and is in list - establish relationship
            if (obj.ParentId.HasValue && objectDict.TryGetValue(obj.ParentId.Value, out var parent))
            {
                obj.Parent = parent;
                parent.Children.Add(obj);
            }
        }
    }

    /// <summary>
    /// Delete objects without loading data
    /// Executes DELETE with filters from query
    /// </summary>
    public override async Task<int> DeleteAsync()
    {
        // 1. Get list of objects by filter
        var objects = await ToListAsync();

        if (objects.Count == 0)
            return 0;

        // 2. Extract object IDs
        var objectIds = objects.Select(o => o.Id).ToArray();

        // 3. Delete through provider
        return await _treeProvider.ExecuteTreeDeleteAsync(objectIds);
    }
    
    /// <summary>
    /// Override GroupBy to respect tree context (rootObjectId, maxDepth, ParentIds).
    /// Returns TreeGroupedQueryable that uses tree-aware execution.
    /// </summary>
    public override Grouping.IRedbGroupedQueryable<TKey, TProps> GroupBy<TKey>(
        Expression<Func<TProps, TKey>> keySelector)
    {
        // Return tree-aware grouped queryable that has access to TreeQueryContext
        return new Grouping.TreeGroupedQueryable<TKey, TProps>(
            _treeProvider, _treeContext, keySelector, BuildFilterJson());
    }
    
    /// <summary>
    /// Override GroupByRedb to respect tree context.
    /// </summary>
    public override Grouping.IRedbGroupedQueryable<TKey, TProps> GroupByRedb<TKey>(
        Expression<Func<IRedbObject, TKey>> keySelector)
    {
        return new Grouping.TreeGroupedQueryable<TKey, TProps>(
            _treeProvider, _treeContext, keySelector, BuildFilterJson(), isBaseFieldGrouping: true);
    }
    
    /// <summary>
    /// Override WithWindow to use tree-aware execution.
    /// Returns TreeWindowedQueryable that uses tree-aware execution with CTE.
    /// </summary>
    public override Window.IRedbWindowedQueryable<TProps> WithWindow(
        Action<Window.IWindowSpec<TProps>> windowConfig)
    {
        var filterJson = BuildFilterJson();
        var windowSpec = new Window.WindowSpec<TProps>();
        windowConfig(windowSpec);
        return new Window.TreeWindowedQueryable<TProps>(
            _treeProvider, _treeContext, filterJson, windowSpec);
    }
}
