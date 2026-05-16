using System;
using System.Collections.Generic;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// Interface for REDB tree objects with navigational properties
    /// Extends IRedbObject adding tree navigation capabilities in memory
    /// Supports polymorphic trees (objects of different schemes in one tree)
    /// </summary>
    public interface ITreeRedbObject : IRedbObject
    {
        /// <summary>
        /// Reference to parent object (filled when loading tree)
        /// null for root nodes
        /// </summary>
        ITreeRedbObject? Parent { get; set; }
        
        /// <summary>
        /// Collection of child objects (filled when loading tree)
        /// Empty collection for leaf nodes
        /// </summary>
        ICollection<ITreeRedbObject> Children { get; set; }
        
        /// <summary>
        /// Checks if node is leaf (has no children)
        /// </summary>
        bool IsLeaf { get; }
        
        /// <summary>
        /// Gets node level in tree (0 for root)
        /// Requires loaded hierarchy up to root through Parent references
        /// </summary>
        int Level { get; }
        
        /// <summary>
        /// Gets path from root to current node as sequence of IDs
        /// </summary>
        /// <returns>Sequence of IDs from root to current node</returns>
        IEnumerable<long> GetPathIds();
        
        /// <summary>
        /// Gets breadcrumbs for navigation
        /// </summary>
        /// <param name="separator">Separator between elements (default " > ")</param>
        /// <param name="includeIds">Whether to include IDs in parentheses (default false)</param>
        /// <returns>String like "Root > Category > Subcategory" or "Root (1) > Category (5) > Subcategory (23)"</returns>
        string GetBreadcrumbs(string separator = " > ", bool includeIds = false);
        
        /// <summary>
        /// Checks if current node is descendant of specified node
        /// </summary>
        /// <param name="ancestor">Alleged ancestor</param>
        /// <returns>true if current node is descendant of ancestor</returns>
        bool IsDescendantOf(ITreeRedbObject ancestor);
        
        /// <summary>
        /// Checks if current node is ancestor of specified node
        /// </summary>
        /// <param name="descendant">Alleged descendant</param>
        /// <returns>true if current node is ancestor of descendant</returns>
        bool IsAncestorOf(ITreeRedbObject descendant);
        
        /// <summary>
        /// Gets all subtree nodes (including current) in depth-first order
        /// </summary>
        /// <returns>Sequence of subtree nodes</returns>
        IEnumerable<ITreeRedbObject> GetSubtree();
        
        /// <summary>
        /// Gets number of nodes in subtree (including current)
        /// </summary>
        int SubtreeSize { get; }
        
        /// <summary>
        /// Gets maximum depth of subtree from current node
        /// </summary>
        int MaxDepth { get; }
        
        /// <summary>
        /// Gets all node ancestors (from parent to root)
        /// Requires loaded hierarchy up to root through Parent references
        /// </summary>
        IEnumerable<ITreeRedbObject> Ancestors { get; }
        
        /// <summary>
        /// Gets all node descendants recursively
        /// Requires loaded hierarchy downwards through Children collections
        /// </summary>
        IEnumerable<ITreeRedbObject> Descendants { get; }
    }

    /// <summary>
    /// Typed interface for tree objects with specific property type.
    /// Combines ITreeRedbObject (untyped navigation) and IRedbObject&lt;TProps&gt; (typed data).
    /// Supports polymorphic trees - parents and children can be different types.
    /// </summary>
    /// <typeparam name="TProps">Object properties type.</typeparam>
    public interface ITreeRedbObject<TProps> : ITreeRedbObject, IRedbObject<TProps>
        where TProps : class, new()
    {
        // Empty interface - combines ITreeRedbObject + IRedbObject<TProps>
        // Parent and Children inherited from ITreeRedbObject (untyped)
        // Props inherited from IRedbObject<TProps> (typed)
    }
}
