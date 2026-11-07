using System;
using System.Collections.Generic;
using System.Linq;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// Interface for tree nodes with navigational properties and traversal operations
    /// </summary>
    /// <typeparam name="T">Tree node type</typeparam>
    public interface ITreeNode<T> where T : class, ITreeNode<T>
    {
        /// <summary>
        /// Unique node identifier
        /// </summary>
        long Id { get; set; }
        
        /// <summary>
        /// Parent node identifier (null for root nodes)
        /// </summary>
        long? ParentId { get; set; }
        
        /// <summary>
        /// Reference to parent node (filled when loading tree)
        /// </summary>
        T? Parent { get; set; }
        
        /// <summary>
        /// Collection of child nodes (filled when loading tree)
        /// </summary>
        ICollection<T> Children { get; set; }
        
        /// <summary>
        /// Checks if node is root (has no parent)
        /// </summary>
        bool IsRoot => ParentId == null;
        
        /// <summary>
        /// Checks if node is leaf (has no children)
        /// </summary>
        bool IsLeaf => !Children.Any();
        
        /// <summary>
        /// Gets node level in tree (0 for root)
        /// Requires loaded hierarchy up to root
        /// </summary>
        int Level
        {
            get
            {
                int level = 0;
                var current = Parent;
                while (current != null)
                {
                    level++;
                    current = current.Parent;
                }
                return level;
            }
        }
        
        /// <summary>
        /// Gets all node ancestors (from parent to root)
        /// Requires loaded hierarchy up to root
        /// </summary>
        IEnumerable<T> Ancestors
        {
            get
            {
                var current = Parent;
                while (current != null)
                {
                    yield return current;
                    current = current.Parent;
                }
            }
        }
        
        /// <summary>
        /// Gets all node descendants recursively
        /// Requires loaded hierarchy downwards
        /// </summary>
        IEnumerable<T> Descendants
        {
            get
            {
                foreach (var child in Children)
                {
                    yield return child;
                    foreach (var descendant in child.Descendants)
                        yield return descendant;
                }
            }
        }
    }
}
