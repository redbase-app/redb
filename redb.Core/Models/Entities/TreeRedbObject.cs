using redb.Core.Models.Contracts;
using redb.Core.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace redb.Core.Models.Entities
{
    /// <summary>
    /// Base class for REDB tree objects with hierarchy navigation
    /// Supports polymorphic trees - objects of different schemes in one tree
    /// Extends RedbObject by adding navigational properties and traversal methods
    /// </summary>
    public class TreeRedbObject : RedbObject, ITreeRedbObject
    {
        /// <summary>
        /// Reference to parent object (filled when loading tree)
        /// </summary>
        public ITreeRedbObject? Parent { get; set; }
        
        /// <summary>
        /// Collection of child objects (filled when loading tree)
        /// </summary>
        public ICollection<ITreeRedbObject> Children { get; set; } = new List<ITreeRedbObject>();

        /// <summary>
        /// Checks if node is leaf (has no children)
        /// </summary>
        public bool IsLeaf => !Children.Any();
        
        /// <summary>
        /// Gets node level in tree (0 for root)
        /// </summary>
        public int Level
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
        /// </summary>
        public IEnumerable<ITreeRedbObject> Ancestors
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
        /// </summary>
        public IEnumerable<ITreeRedbObject> Descendants
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

        /// <summary>
        /// Gets path from root to current node as IDs
        /// </summary>
        public IEnumerable<long> GetPathIds()
        {
            var path = new List<long>();
            var current = this;
            
            while (current != null)
            {
                path.Insert(0, current.Id);
                current = current.Parent as TreeRedbObject;
            }
            
            return path;
        }
        
        /// <summary>
        /// Gets breadcrumbs for navigation
        /// </summary>
        /// <param name="separator">Separator between elements</param>
        /// <param name="includeIds">Whether to include IDs in parentheses</param>
        /// <returns>String like "Root > Category > Subcategory"</returns>
        public string GetBreadcrumbs(string separator = " > ", bool includeIds = false)
        {
            var ancestors = Ancestors.Reverse().ToList();
            var path = ancestors.Append(this);
            
            var names = path.Select(node => 
            {
                var displayName = node.Name ?? $"Object {node.Id}";
                return includeIds ? $"{displayName} ({node.Id})" : displayName;
            });
            
            return string.Join(separator, names);
        }
        
        /// <summary>
        /// Checks if current node is descendant of specified node
        /// </summary>
        /// <param name="ancestor">Alleged ancestor</param>
        /// <returns>true if current is descendant of ancestor</returns>
        public bool IsDescendantOf(ITreeRedbObject ancestor)
        {
            var current = Parent;
            while (current != null)
            {
                if (current.Id == ancestor.Id)
                    return true;
                current = current.Parent;
            }
            return false;
        }
        
        /// <summary>
        /// Checks if current node is ancestor of specified node
        /// </summary>
        /// <param name="descendant">Alleged descendant</param>
        /// <returns>true if current is ancestor of descendant</returns>
        public bool IsAncestorOf(ITreeRedbObject descendant)
        {
            return descendant.IsDescendantOf(this);
        }
        
        /// <summary>
        /// Gets all subtree nodes (including current) in depth-first order
        /// </summary>
        /// <returns>Sequence of subtree nodes</returns>
        public IEnumerable<ITreeRedbObject> GetSubtree()
        {
            yield return this;
            
            foreach (var child in Children)
            {
                foreach (var node in child.GetSubtree())
                    yield return node;
            }
        }
        
        /// <summary>
        /// Gets number of nodes in subtree (including current)
        /// </summary>
        public int SubtreeSize => GetSubtree().Count();
        
        /// <summary>
        /// Gets maximum depth of subtree from current node
        /// </summary>
        public int MaxDepth
        {
            get
            {
                if (!Children.Any())
                    return 0;
                
                return 1 + Children.Max(child => child.MaxDepth);
            }
        }
        
        /// <summary>
        /// Recompute MD5 hash from object values and store in hash field
        /// </summary>
        public override void RecomputeHash()
        {
            hash = RedbHash.ComputeFor((IRedbObject)this);
        }

        /// <summary>
        /// Get MD5 hash from object values without modifying hash field
        /// </summary>
        public override Guid ComputeHash() => RedbHash.ComputeFor((IRedbObject)this) ?? Guid.Empty;
    }

    /// <summary>
    /// ARCHITECTURAL FIX: Typed version of REDB tree object
    /// NEW INHERITANCE: RedbObject&lt;TProps&gt; instead of TreeRedbObject
    /// ADVANTAGES: Direct type casting, Props duplication elimination, no conversion
    /// POLYMORPHISM: Implements ITreeRedbObject for untyped operations support
    /// </summary>
    /// <typeparam name="TProps">Object properties type</typeparam>
    public class TreeRedbObject<TProps> : RedbObject<TProps>, ITreeRedbObject, ITreeRedbObject<TProps>
        where TProps : class, new()
    {
        // FIX: Props inherited from RedbObject<TProps> - duplication eliminated!

        /// <summary>
        /// TREE-SPECIFIC PROPERTIES - untyped navigation
        /// Supports polymorphic trees - parents and children can be different types
        /// </summary>
        
        /// <summary>
        /// Reference to parent object (untyped to support polymorphic trees)
        /// </summary>
        public ITreeRedbObject? Parent { get; set; }
        
        /// <summary>
        /// Collection of child objects (untyped to support polymorphic trees)
        /// </summary>
        public ICollection<ITreeRedbObject> Children { get; set; } = new List<ITreeRedbObject>();

        /// <summary>
        /// TREE NAVIGATION PROPERTIES (moved from base TreeRedbObject)
        /// </summary>
        
        /// <summary>
        /// Checks if node is leaf (has no children)
        /// </summary>
        public bool IsLeaf => !Children.Any();
        
        /// <summary>
        /// Gets node level in tree (0 for root)
        /// </summary>
        public int Level
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
        /// Gets all subtree nodes (including current) in depth-first order
        /// </summary>
        public IEnumerable<ITreeRedbObject> GetSubtree()
        {
            yield return this;
            
            foreach (var child in Children)
            {
                foreach (var node in child.GetSubtree())
                    yield return node;
            }
        }
        
        /// <summary>
        /// Gets all node ancestors (from parent to root)
        /// </summary>
        public IEnumerable<ITreeRedbObject> Ancestors
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
        /// </summary>
        public IEnumerable<ITreeRedbObject> Descendants
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

        /// <summary>
        /// NAVIGATION METHODS (moved from base TreeRedbObject)
        /// </summary>
        
        /// <summary>
        /// Gets path from root to current node as IDs
        /// </summary>
        public IEnumerable<long> GetPathIds()
        {
            var path = new List<long>();
            var current = (ITreeRedbObject?)this;
            
            while (current != null)
            {
                path.Insert(0, current.Id);
                current = current.Parent;
            }
            
            return path;
        }
        
        /// <summary>
        /// Gets breadcrumbs for navigation
        /// </summary>
        public string GetBreadcrumbs(string separator = " > ", bool includeIds = false)
        {
            var ancestors = Ancestors.Reverse().ToList();
            var path = ancestors.Append(this);
            
            var names = path.Select(node => 
            {
                var displayName = node.Name ?? $"Object {node.Id}";
                return includeIds ? $"{displayName} ({node.Id})" : displayName;
            });
            
            return string.Join(separator, names);
        }
        
        /// <summary>
        /// Checks if current node is descendant of specified node
        /// </summary>
        public bool IsDescendantOf(ITreeRedbObject ancestor)
        {
            var current = Parent;
            while (current != null)
            {
                if (current.Id == ancestor.Id)
                    return true;
                current = current.Parent;
            }
            return false;
        }
        
        /// <summary>
        /// Checks if current node is ancestor of specified node
        /// </summary>
        public bool IsAncestorOf(ITreeRedbObject descendant)
        {
            return descendant.IsDescendantOf(this);
        }
        
        /// <summary>
        /// Gets number of nodes in subtree (including current)
        /// </summary>
        public int SubtreeSize => GetSubtree().Count();
        
        /// <summary>
        /// Gets maximum depth of subtree from current node
        /// </summary>
        public int MaxDepth
        {
            get
            {
                if (!Children.Any())
                    return 0;
                
                return 1 + Children.Max(child => child.MaxDepth);
            }
        }
        
        // Cache and metadata methods inherited from RedbObject<TProps>
        // Navigation methods now directly implement ITreeRedbObject (untyped)
        // Explicit interface implementations no longer needed!
    }
}
