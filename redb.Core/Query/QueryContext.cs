using System.Collections.Generic;
using redb.Core.Query.QueryExpressions;

namespace redb.Core.Query;

/// <summary>
/// Query context - contains all information about LINQ query
/// </summary>
public class QueryContext<TProps> where TProps : class, new()
{
    public long SchemeId { get; init; }
    public long? UserId { get; init; }
    public bool CheckPermissions { get; init; }
    
    // ✅ SYNC WITH RUSLAN VERSION: Add tree query support
    public long? ParentId { get; init; }                   // For single parent
    public long[]? ParentIds { get; set; }                 // For batch operations
    public int? MaxDepth { get; init; }                    // Maximum search depth
    
    public FilterExpression? Filter { get; set; }
    public List<OrderingExpression> Orderings { get; set; } = new();
    public int? Limit { get; set; }
    public int? Offset { get; set; }
    public bool IsDistinct { get; set; }
    
    /// <summary>
    /// DISTINCT by base IRedbObject fields (Name, ValueLong, ParentId, etc.) excluding Id.
    /// Used for finding duplicates by base fields.
    /// </summary>
    public bool IsDistinctRedb { get; set; }
    
    /// <summary>
    /// Field for DISTINCT ON (field) - one object per each unique field value
    /// </summary>
    public OrderingExpression? DistinctByField { get; set; }
    
    /// <summary>
    /// true = DistinctByField is a base IRedbObject field
    /// false = DistinctByField is a Props field
    /// </summary>
    public bool DistinctByIsBaseField { get; set; }
    
    public int? MaxRecursionDepth { get; set; }
    
    /// <summary>
    /// ✅ NEW FLAG: Indicates that query should return empty result
    /// Used for Where(x => false) and similar cases
    /// </summary>
    public bool IsEmpty { get; set; }
    
    /// <summary>
    /// Lazy loading flag for Props. 
    /// null = use global config
    /// true = explicitly enable lazy loading
    /// false = explicitly disable lazy loading
    /// </summary>
    public bool? UseLazyLoading { get; set; }
    
    /// <summary>
    /// ⭐ PROJECTION: Structure IDs for optimized Props loading
    /// null = load ALL Props fields
    /// HashSet = load ONLY specified structure_ids
    /// </summary>
    public HashSet<long>? ProjectedStructureIds { get; set; }
    
    /// <summary>
    /// ⭐ PROJECTION: Text field paths for SQL function search_objects_with_projection_by_paths
    /// Format: ["Name", "AddressBook[home].City", "Items[0].Price"]
    /// Used instead of ProjectedStructureIds for human-readable queries
    /// </summary>
    public List<string>? ProjectedFieldPaths { get; set; }
    
    /// <summary>
    /// PROJECTION: Skip ALL Props post-processing in MaterializeResultsFromJson.
    /// When true, neither LoadPropsForManyAsync (eager) nor lazy loader setup will run.
    /// Used by projections: SQL projection function already returns partial Props in JSON,
    /// deserialized via Props setter. Re-loading would overwrite them with full object data.
    /// </summary>
    public bool SkipPropsLoading { get; set; }
    
    /// <summary>
    /// Maximum depth for loading nested RedbObject in Props.
    /// Controls recursion when Props contain references to other RedbObjects via _values._Object.
    /// null = use global DefaultMaxTreeDepth from config.
    /// </summary>
    public int? PropsDepth { get; set; }
    
    public QueryContext(long schemeId, long? userId = null, bool checkPermissions = false, long? parentId = null, int? maxDepth = null)
    {
        SchemeId = schemeId;
        UserId = userId;
        CheckPermissions = checkPermissions;
        ParentId = parentId;      // ✅ SYNC: add initialization
        MaxDepth = maxDepth;      // ✅ SYNC: add initialization
    }
    
    /// <summary>
    /// Create a copy of context
    /// </summary>
    public QueryContext<TProps> Clone()
    {
        return new QueryContext<TProps>(SchemeId, UserId, CheckPermissions, ParentId, MaxDepth)
        {
            ParentIds = ParentIds,  // ✅ SYNC: copy batch array
            Filter = Filter,
            Orderings = new List<OrderingExpression>(Orderings),
            Limit = Limit,
            Offset = Offset,
            IsDistinct = IsDistinct,
            IsDistinctRedb = IsDistinctRedb,
            DistinctByField = DistinctByField,
            DistinctByIsBaseField = DistinctByIsBaseField,
            MaxRecursionDepth = MaxRecursionDepth,
            IsEmpty = IsEmpty,       // ✅ FIX: copy IsEmpty flag
            UseLazyLoading = UseLazyLoading,  // ✅ copy lazy loading flag
            ProjectedStructureIds = ProjectedStructureIds,
            ProjectedFieldPaths = ProjectedFieldPaths,
            SkipPropsLoading = SkipPropsLoading,
            PropsDepth = PropsDepth
        };
    }
}
