using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Core.Query.Aggregation;

namespace redb.Core.Query;

/// <summary>
/// Main interface for type-safe LINQ queries to REDB.
/// </summary>
public interface IRedbQueryable<TProps> where TProps : class, new()
{
    /// <summary>
    /// Filter by Props fields.
    /// </summary>
    IRedbQueryable<TProps> Where(Expression<Func<TProps, bool>> predicate);
    
    /// <summary>
    /// Filter by base object fields (Id, Name, ParentId, etc.).
    /// Allows LINQ queries on IRedbObject fields: Id, ParentId, SchemeId, 
    /// Name, Note, Key, Hash, OwnerId, WhoChangeId,
    /// DateCreate, DateModify, DateBegin, DateComplete,
    /// ValueLong, ValueString, ValueGuid, ValueBool, ValueDouble, ValueNumeric, ValueDatetime, ValueBytes.
    /// Uses IRedbObject for compile-time safety - Props not visible in IntelliSense!
    /// </summary>
    /// <example>
    /// .WhereRedb(x => x.Id == 123)
    /// .WhereRedb(x => x.ValueLong > 100)
    /// .WhereRedb(x => x.Name.Contains("test"))
    /// .WhereRedb(x => x.ParentId == null)  // root objects
    /// </example>
    IRedbQueryable<TProps> WhereRedb(Expression<Func<IRedbObject, bool>> predicate);
    
    /// <summary>
    /// Sort ascending by Props field.
    /// </summary>
    IOrderedRedbQueryable<TProps> OrderBy<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// Sort descending by Props field.
    /// </summary>
    IOrderedRedbQueryable<TProps> OrderByDescending<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// Sort ascending by IRedbObject base fields (id, name, date_create, etc.).
    /// Uses IRedbObject for compile-time safety - Props not visible in IntelliSense!
    /// </summary>
    /// <example>
    /// .OrderByRedb(x => x.DateCreate)
    /// .OrderByRedb(x => x.Name)
    /// .OrderByRedb(x => x.Id)
    /// </example>
    IOrderedRedbQueryable<TProps> OrderByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);
    
    /// <summary>
    /// Sort descending by IRedbObject base fields.
    /// Uses IRedbObject for compile-time safety - Props not visible in IntelliSense!
    /// </summary>
    IOrderedRedbQueryable<TProps> OrderByDescendingRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);
    
    /// <summary>
    /// Limit the number of records.
    /// </summary>
    IRedbQueryable<TProps> Take(int count);
    
    /// <summary>
    /// Skip records.
    /// </summary>
    IRedbQueryable<TProps> Skip(int count);
    
    /// <summary>
    /// Execute query and return list of objects.
    /// </summary>
    Task<List<RedbObject<TProps>>> ToListAsync();
    
    /// <summary>
    /// Count records without loading data.
    /// </summary>
    Task<int> CountAsync();
    
    /// <summary>
    /// Get first object or null.
    /// </summary>
    Task<RedbObject<TProps>?> FirstOrDefaultAsync();
    
    /// <summary>
    /// Get first object matching condition, or null.
    /// </summary>
    Task<RedbObject<TProps>?> FirstOrDefaultAsync(Expression<Func<TProps, bool>> predicate);
    
    /// <summary>
    /// Check if any records exist.
    /// </summary>
    Task<bool> AnyAsync();
    
    /// <summary>
    /// Check if any records match condition.
    /// </summary>
    Task<bool> AnyAsync(Expression<Func<TProps, bool>> predicate);
    
    /// <summary>
    /// Filter by value in list (WHERE field IN (...)).
    /// </summary>
    IRedbQueryable<TProps> WhereIn<TValue>(Expression<Func<TProps, TValue>> selector, IEnumerable<TValue> values);
    
    /// <summary>
    /// Filter by IRedbObject base field in list (WHERE _field IN (...)).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    /// <example>
    /// .WhereInRedb(x => x.Id, new[] { 1L, 2L, 3L })
    /// .WhereInRedb(x => x.Name, new[] { "Alice", "Bob" })
    /// </example>
    IRedbQueryable<TProps> WhereInRedb<TValue>(Expression<Func<IRedbObject, TValue>> selector, IEnumerable<TValue> values);
    
    /// <summary>
    /// Check if ALL records match condition.
    /// </summary>
    Task<bool> AllAsync(Expression<Func<TProps, bool>> predicate);
    
    /// <summary>
    /// Project fields - return only selected properties.
    /// </summary>
    IRedbProjectedQueryable<TResult> Select<TResult>(Expression<Func<RedbObject<TProps>, TResult>> selector);
    
    /// <summary>
    /// Get distinct values (by all object fields).
    /// </summary>
    IRedbQueryable<TProps> Distinct();
    
    /// <summary>
    /// DISTINCT by IRedbObject base fields (Name, ValueLong, ParentId, etc.) excluding Id.
    /// Used for finding duplicates by base fields.
    /// </summary>
    IRedbQueryable<TProps> DistinctRedb();
    
    /// <summary>
    /// DISTINCT ON (field) - one object per unique Props field value.
    /// Automatically adds field to beginning of ORDER BY (PostgreSQL requirement).
    /// </summary>
    /// <example>
    /// .DistinctBy(x => x.Category)  // one object per Category
    /// </example>
    IRedbQueryable<TProps> DistinctBy<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// DISTINCT ON (base_field) - one object per unique IRedbObject base field value.
    /// Automatically adds field to beginning of ORDER BY (PostgreSQL requirement).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    /// <example>
    /// .DistinctByRedb(x => x.Name)  // one object per unique Name
    /// </example>
    IRedbQueryable<TProps> DistinctByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);
    
    /// <summary>
    /// Configure maximum recursion depth for complex queries ($and/$or/$not).
    /// Default: 10 levels.
    /// </summary>
    IRedbQueryable<TProps> WithMaxRecursionDepth(int depth);
    
    /// <summary>
    /// Enable/disable lazy loading of Props for this query.
    /// Overrides global EnableLazyLoadingForProps configuration setting.
    /// With lazy loading: Props loaded in BULK for ALL objects via LoadPropsForManyAsync after ToListAsync.
    /// Without lazy loading: Props loaded immediately via get_object_json (legacy behavior).
    /// </summary>
    /// <param name="enabled">true = lazy loading, false = eager loading</param>
    /// <returns>Query with configured Props loading mode</returns>
    IRedbQueryable<TProps> WithLazyLoading(bool enabled = true);
    
    // ===== TREE FILTERS =====
    
    /// <summary>
    /// Filter by ancestors: find objects that have an ancestor of specified type matching condition.
    /// Uses SQL operator $hasAncestor.
    /// Supports polymorphic trees - can search for ancestors of different type.
    /// </summary>
    /// <typeparam name="TTarget">Ancestor type to search for (may differ from TProps)</typeparam>
    /// <param name="ancestorCondition">Condition for ancestor search</param>
    /// <param name="maxDepth">Maximum depth to search up the tree (null = to root)</param>
    /// <returns>Query with ancestor filter</returns>
    IRedbQueryable<TProps> WhereHasAncestor<TTarget>(
        Expression<Func<TTarget, bool>> ancestorCondition, 
        int? maxDepth = null) 
        where TTarget : class;
    
    /// <summary>
    /// Filter by descendants: find objects that have a descendant of specified type matching condition.
    /// Uses SQL operator $hasDescendant.
    /// Supports polymorphic trees - can search for descendants of different type.
    /// </summary>
    /// <typeparam name="TTarget">Descendant type to search for (may differ from TProps)</typeparam>
    /// <param name="descendantCondition">Condition for descendant search</param>
    /// <param name="maxDepth">Maximum depth to search down the tree (null = to leaves)</param>
    /// <returns>Query with descendant filter</returns>
    IRedbQueryable<TProps> WhereHasDescendant<TTarget>(
        Expression<Func<TTarget, bool>> descendantCondition, 
        int? maxDepth = null) 
        where TTarget : class;
    
    /// <summary>
    /// Filter by tree level.
    /// Uses SQL operator $level.
    /// </summary>
    /// <param name="level">Tree level (0 = root)</param>
    /// <returns>Query with level filter</returns>
    IRedbQueryable<TProps> WhereLevel(int level);
    
    /// <summary>
    /// Filter by tree level with comparison operator.
    /// Uses SQL operators $level: {$gt: N}, {$lt: N}, etc.
    /// </summary>
    /// <param name="levelCondition">Condition for level (e.g.: level => level > 2)</param>
    /// <returns>Query with level condition filter</returns>
    IRedbQueryable<TProps> WhereLevel(Expression<Func<int, bool>> levelCondition);
    
    /// <summary>
    /// Root elements only (parent_id IS NULL).
    /// Uses SQL operator $isRoot.
    /// </summary>
    /// <returns>Query for root objects only</returns>
    IRedbQueryable<TProps> WhereRoots();
    
    /// <summary>
    /// Leaf nodes only (objects without children).
    /// Uses SQL operator $isLeaf.
    /// </summary>
    /// <returns>Query for leaf objects only</returns>
    IRedbQueryable<TProps> WhereLeaves();
    
    /// <summary>
    /// Direct children of specified object.
    /// Uses SQL operator $childrenOf.
    /// </summary>
    /// <param name="parentId">Parent object ID</param>
    /// <returns>Query for object children</returns>
    IRedbQueryable<TProps> WhereChildrenOf(long parentId);
    
    /// <summary>
    /// Direct children of specified object.
    /// Uses SQL operator $childrenOf.
    /// </summary>
    /// <param name="parentObject">Parent object</param>
    /// <returns>Query for object children</returns>
    IRedbQueryable<TProps> WhereChildrenOf(IRedbObject parentObject);
    
    /// <summary>
    /// All descendants of specified object (recursive).
    /// Uses SQL operator $descendantsOf.
    /// </summary>
    /// <param name="ancestorId">Ancestor ID</param>
    /// <param name="maxDepth">Maximum search depth (null = unlimited)</param>
    /// <returns>Query for all object descendants</returns>
    IRedbQueryable<TProps> WhereDescendantsOf(long ancestorId, int? maxDepth = null);
    
    /// <summary>
    /// All descendants of specified object (recursive).
    /// Uses SQL operator $descendantsOf.
    /// </summary>
    /// <param name="ancestorObject">Ancestor object</param>
    /// <param name="maxDepth">Maximum search depth (null = unlimited)</param>
    /// <returns>Query for all object descendants</returns>
    IRedbQueryable<TProps> WhereDescendantsOf(IRedbObject ancestorObject, int? maxDepth = null);
    
    /// <summary>
    /// Configure maximum tree search depth.
    /// Default: 50 levels (descendant search), 1 (children search).
    /// </summary>
    IRedbQueryable<TProps> WithMaxDepth(int depth);
    
    /// <summary>
    /// Configure maximum depth for loading nested RedbObject in Props.
    /// Controls recursion when Props contain references to other RedbObjects via _values._Object.
    /// Default: uses global DefaultMaxTreeDepth from config.
    /// </summary>
    /// <param name="depth">Maximum depth for nested RedbObject loading (1 = only direct references)</param>
    /// <returns>Query with configured Props depth</returns>
    IRedbQueryable<TProps> WithPropsDepth(int depth);
    
    // ===== MATERIALIZATION METHODS =====
    
    /// <summary>
    /// Execute query and return filtered objects with populated Parent/Children chains.
    /// Loads objects + all their parents to root and establishes relationships.
    /// Children contain only objects from loaded chain (not all children from DB).
    /// </summary>
    /// <returns>List of filtered TreeRedbObject with populated relationships</returns>
    Task<List<TreeRedbObject<TProps>>> ToTreeListAsync();
    
    /// <summary>
    /// Execute query and get list of root tree nodes.
    /// Loads objects + all their parents to root, establishes relationships and returns roots.
    /// Children contain only objects from loaded chain (not all children from DB).
    /// Supports polymorphic trees - each object of its own type via ITreeRedbObject.
    /// </summary>
    /// <returns>List of root ITreeRedbObject (polymorphic - each of its own TProps type)</returns>
    Task<List<ITreeRedbObject>> ToRootListAsync();
    
    /// <summary>
    /// Execute query and get flat list of tree objects.
    /// Without loading Parent/Children relationships (for performance).
    /// </summary>
    /// <returns>Flat list of TreeRedbObject</returns>
    Task<List<TreeRedbObject<TProps>>> ToFlatListAsync();
    
    /// <summary>
    /// Delete objects without loading data.
    /// Executes DELETE with filters from query.
    /// </summary>
    /// <returns>Number of deleted records</returns>
    Task<int> DeleteAsync();
    
    // ===== AGGREGATIONS (EAV) =====
    
    /// <summary>
    /// Sum of field values.
    /// Strategy: SQL for simple fields, C# for complex (Class, arrays).
    /// </summary>
    /// <example>
    /// var total = await query.Where(x => x.Status == "Active").SumAsync(x => x.Price);
    /// </example>
    Task<decimal> SumAsync<TField>(Expression<Func<TProps, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Average of field values.
    /// </summary>
    Task<decimal> AverageAsync<TField>(Expression<Func<TProps, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Minimum field value.
    /// </summary>
    Task<TField?> MinAsync<TField>(Expression<Func<TProps, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Maximum field value.
    /// </summary>
    Task<TField?> MaxAsync<TField>(Expression<Func<TProps, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Get field statistics (Sum, Avg, Min, Max, Count) in single call.
    /// All aggregations execute in parallel for maximum performance.
    /// </summary>
    /// <example>
    /// var stats = await query.GetStatisticsAsync(x => x.Price);
    /// stats.Sum, stats.Average, stats.Min, stats.Max, stats.Count
    /// </example>
    Task<FieldStatistics<TField>> GetStatisticsAsync<TField>(Expression<Func<TProps, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Flexible aggregation - choose what to aggregate via Agg.Sum/Avg/Min/Max/Count.
    /// </summary>
    /// <example>
    /// var result = await query
    ///     .Where(x => x.Status == "Active")
    ///     .AggregateAsync(x => new {
    ///         TotalAmount = Agg.Sum(x.Props.Amount),
    ///         AvgPrice = Agg.Avg(x.Props.Price),
    ///         MinStock = Agg.Min(x.Props.Stock),
    ///         OrderCount = Agg.Count()
    ///     });
    /// </example>
    Task<TResult> AggregateAsync<TResult>(Expression<Func<RedbObject<TProps>, TResult>> selector);
    
    // ===== AGGREGATIONS FOR BASE FIELDS =====
    
    /// <summary>
    /// Sum of IRedbObject base field values (ValueLong, Key, etc.).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    /// <example>
    /// var totalValue = await query.SumRedbAsync(x => x.ValueLong);
    /// </example>
    Task<decimal> SumRedbAsync<TField>(Expression<Func<IRedbObject, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Average of IRedbObject base field values.
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    Task<decimal> AverageRedbAsync<TField>(Expression<Func<IRedbObject, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Minimum IRedbObject base field value (ValueLong, Key, DateCreate, etc.).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    Task<TField?> MinRedbAsync<TField>(Expression<Func<IRedbObject, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Maximum IRedbObject base field value (ValueLong, Key, DateCreate, etc.).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    Task<TField?> MaxRedbAsync<TField>(Expression<Func<IRedbObject, TField>> selector) where TField : struct;
    
    /// <summary>
    /// Flexible aggregation for IRedbObject base fields ONLY.
    /// For aggregating both base AND Props fields - use AggregateAsync.
    /// </summary>
    /// <example>
    /// var result = await query
    ///     .AggregateRedbAsync(x => new {
    ///         MaxId = Agg.Max(x.Id),
    ///         MinDateCreate = Agg.Min(x.DateCreate),
    ///         AvgValue = Agg.Avg(x.ValueLong),
    ///         Count = Agg.Count()
    ///     });
    /// </example>
    Task<TResult> AggregateRedbAsync<TResult>(Expression<Func<IRedbObject, TResult>> selector);
    
    // ===== GROUPBY =====
    
    /// <summary>
    /// Group by field with subsequent aggregation.
    /// </summary>
    /// <example>
    /// var result = await query
    ///     .GroupBy(x => x.Category)
    ///     .SelectAsync(g => new {
    ///         Category = g.Key,
    ///         Total = Agg.Sum(g, x => x.Stock),
    ///         Count = Agg.Count(g)
    ///     });
    /// </example>
    Grouping.IRedbGroupedQueryable<TKey, TProps> GroupBy<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// Group by IRedbObject base fields (id, scheme_id, parent_id, etc.).
    /// Uses IRedbObject for compile-time safety - Props not visible!
    /// </summary>
    /// <example>
    /// var result = await query
    ///     .GroupByRedb(x => x.SchemeId)
    ///     .SelectAsync(g => new {
    ///         SchemeId = g.Key,
    ///         Count = Agg.Count(g)
    ///     });
    /// Multiple base fields:
    /// var result = await query
    ///     .GroupByRedb(x => new { x.SchemeId, x.OwnerId })
    ///     .SelectAsync(g => new {
    ///         g.Key.SchemeId,
    ///         g.Key.OwnerId,
    ///         Count = Agg.Count(g)
    ///     });
    /// </example>
    Grouping.IRedbGroupedQueryable<TKey, TProps> GroupByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);
    
    /// <summary>
    /// Group by array elements (Items[].Property).
    /// </summary>
    /// <example>
    /// var result = await query
    ///     .GroupByArray(x => x.Items, item => item.Category)
    ///     .SelectAsync(g => new {
    ///         Category = g.Key,
    ///         TotalPrice = Agg.Sum(g, item => item.Price)
    ///     });
    /// </example>
    Grouping.IRedbGroupedQueryable<TKey, TItem> GroupByArray<TItem, TKey>(
        Expression<Func<TProps, IEnumerable<TItem>>> arraySelector,
        Expression<Func<TItem, TKey>> keySelector) where TItem : class, new();
    
    // ===== WINDOW FUNCTIONS =====
    
    /// <summary>
    /// Query with window functions.
    /// </summary>
    /// <example>
    /// var result = await query
    ///     .WithWindow(w => w.PartitionBy(x => x.Category).OrderByDesc(x => x.Stock))
    ///     .SelectAsync(x => new { x.name, Rank = Win.RowNumber() });
    /// </example>
    Window.IRedbWindowedQueryable<TProps> WithWindow(Action<Window.IWindowSpec<TProps>> windowConfig);
    
    // ===== SQL PREVIEW =====
    
    /// <summary>
    /// Get SQL representation of query (for debugging).
    /// Analogous to EF Core ToQueryString().
    /// </summary>
    /// <returns>SQL query as string with comments</returns>
    string ToSqlString();
    
    /// <summary>
    /// Async version of getting SQL (recommended).
    /// </summary>
    Task<string> ToSqlStringAsync();
    
    /// <summary>
    /// Get JSON filter that will be sent to SQL function (for diagnostics).
    /// </summary>
    Task<string> ToFilterJsonAsync();
}

/// <summary>
/// Interface for ordered queries (after OrderBy).
/// Supports cascading sorts: OrderBy().ThenByRedb().ThenBy().
/// </summary>
public interface IOrderedRedbQueryable<TProps> : IRedbQueryable<TProps> where TProps : class, new()
{
    /// <summary>
    /// Additional ascending sort by Props field.
    /// </summary>
    IOrderedRedbQueryable<TProps> ThenBy<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// Additional descending sort by Props field.
    /// </summary>
    IOrderedRedbQueryable<TProps> ThenByDescending<TKey>(Expression<Func<TProps, TKey>> keySelector);
    
    /// <summary>
    /// Additional ascending sort by IRedbObject base fields.
    /// Uses IRedbObject for compile-time safety - Props not visible in IntelliSense!
    /// </summary>
    /// <example>
    /// .OrderBy(p => p.Name).ThenByRedb(x => x.DateCreate)
    /// </example>
    IOrderedRedbQueryable<TProps> ThenByRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);

    /// <summary>
    /// Additional descending sort by IRedbObject base fields.
    /// Uses IRedbObject for compile-time safety - Props not visible in IntelliSense!
    /// </summary>
    IOrderedRedbQueryable<TProps> ThenByDescendingRedb<TKey>(Expression<Func<IRedbObject, TKey>> keySelector);
}
