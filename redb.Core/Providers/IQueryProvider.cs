using redb.Core.Query;
using System.Collections.Generic;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for creating LINQ queries (high-level API).
    /// </summary>
    public interface IQueryableProvider
    {
        /// <summary>
        /// Create type-safe query by type (synchronous).
        /// </summary>
        IRedbQueryable<TProps> Query<TProps>() where TProps : class, new();
        
        /// <summary>
        /// Create type-safe query by type with specified user (synchronous).
        /// </summary>
        IRedbQueryable<TProps> Query<TProps>(IRedbUser user) where TProps : class, new();
        
        // ===== TREE LINQ QUERIES =====
        
        /// <summary>
        /// Create type-safe tree query by type (synchronous).
        /// Supports hierarchical operators: WhereHasAncestor, WhereHasDescendant, WhereLevel, WhereRoots, WhereLeaves.
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>() where TProps : class, new();
        
        /// <summary>
        /// Create type-safe tree query by type with specified user (synchronous).
        /// Supports hierarchical operators: WhereHasAncestor, WhereHasDescendant, WhereLevel, WhereRoots, WhereLeaves.
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(IRedbUser user) where TProps : class, new();
        
        // ===== TREE LINQ WITH SUBTREE LIMITATION =====
        // ===== SYNCHRONOUS VERSIONS =====
        
        /// <summary>
        /// Create tree query limited to subtree (synchronous, by ID).
        /// Search will be performed only among descendants of specified rootObjectId.
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(long rootObjectId, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Create tree query limited to subtree (synchronous).
        /// If rootObject = null, returns empty queryable (more convenient for client code).
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(IRedbObject? rootObject, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Create tree query limited to subtrees of object list (synchronous).
        /// If list empty, returns empty queryable (more convenient for client code).
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<IRedbObject> rootObjects, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Create tree query limited to subtrees by ID list (synchronous).
        /// If list empty, returns empty queryable.
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<long> rootObjectIds, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Create tree query limited to subtree with specified user (synchronous, by ID).
        /// Search will be performed only among descendants of specified rootObjectId.
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Create tree query limited to subtree with specified user (synchronous).
        /// If rootObject = null, returns empty queryable (more convenient for client code).
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(IRedbObject? rootObject, IRedbUser user, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Create tree query limited to subtrees of object list with specified user (synchronous).
        /// If list empty, returns empty queryable (more convenient for client code).
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<IRedbObject> rootObjects, IRedbUser user, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Create tree query limited to subtrees by ID list with specified user (synchronous).
        /// If list empty, returns empty queryable.
        /// </summary>
        IRedbQueryable<TProps> TreeQuery<TProps>(IEnumerable<long> rootObjectIds, IRedbUser user, int? maxDepth = null) where TProps : class, new();
    }
}
