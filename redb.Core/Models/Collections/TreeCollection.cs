using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using redb.Core.Utils;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Models.Collections
{
    /// <summary>
    /// Specialized collection for working with tree-structured objects
    /// Automatically builds hierarchy when adding nodes
    /// Supports polymorphic trees (objects of different schemes in one tree)
    /// </summary>
    public class TreeCollection : IEnumerable<ITreeRedbObject>
    {
        private readonly Dictionary<long, ITreeRedbObject> _nodesById = new();
        private readonly List<ITreeRedbObject> _roots = new();
        private readonly List<ITreeRedbObject> _orphans = new(); // Nodes whose parents haven't been added yet

        /// <summary>
        /// Gets count of nodes in collection
        /// </summary>
        public int Count => _nodesById.Count;

        /// <summary>
        /// Gets all root nodes (without parents)
        /// </summary>
        public IEnumerable<ITreeRedbObject> Roots => _roots.AsReadOnly();

        /// <summary>
        /// Gets all leaf nodes (without children)
        /// </summary>
        public IEnumerable<ITreeRedbObject> Leaves => _nodesById.Values.Where(n => n.IsLeaf);

        /// <summary>
        /// Gets orphan nodes (whose parents haven't been added to collection)
        /// </summary>
        public IEnumerable<ITreeRedbObject> Orphans => _orphans.AsReadOnly();

        /// <summary>
        /// Adds node to collection and automatically builds hierarchy
        /// </summary>
        /// <param name="node">Node to add</param>
        public void Add(ITreeRedbObject node)
        {
            if (node == null) throw new ArgumentNullException(nameof(node));
            
            // Check if this node hasn't been added already
            if (_nodesById.ContainsKey(node.Id))
            {
                throw new InvalidOperationException($"Node with ID {node.Id} already added to collection");
            }

            _nodesById[node.Id] = node;

            // If this is a root node
            if (node.IsRoot)
            {
                _roots.Add(node);
            }
            else
            {
                // Look for parent
                if (_nodesById.TryGetValue(node.ParentId!.Value, out var parent))
                {
                    // Parent found - establish connections
                    ConnectChild(parent, node);
                    
                    // Remove from orphans list if it was there
                    _orphans.Remove(node);
                }
                else
                {
                    // Parent not found - add to orphans list
                    _orphans.Add(node);
                }
            }

            // Check if this node became parent for any orphans
            CheckForOrphansToAdopt(node);
        }

        /// <summary>
        /// Adds range of nodes
        /// </summary>
        /// <param name="nodes">Nodes to add</param>
        public void AddRange(IEnumerable<ITreeRedbObject> nodes)
        {
            foreach (var node in nodes)
            {
                Add(node);
            }
        }

        /// <summary>
        /// Finds node by ID
        /// </summary>
        /// <param name="id">Node ID</param>
        /// <returns>Found node or null</returns>
        public ITreeRedbObject? FindById(long id)
        {
            return _nodesById.GetValueOrDefault(id);
        }

        /// <summary>
        /// Finds nodes by predicate
        /// </summary>
        /// <param name="predicate">Search condition</param>
        /// <returns>Collection of found nodes</returns>
        public IEnumerable<ITreeRedbObject> FindNodes(Func<ITreeRedbObject, bool> predicate)
        {
            return _nodesById.Values.Where(predicate);
        }

        /// <summary>
        /// Removes node from collection
        /// </summary>
        /// <param name="id">ID of node to remove</param>
        /// <returns>true if node was removed</returns>
        public bool Remove(long id)
        {
            if (!_nodesById.TryGetValue(id, out var node))
                return false;

            // Remove node from structure
            if (node.IsRoot)
            {
                _roots.Remove(node);
            }
            else if (node.Parent != null)
            {
                node.Parent.Children.Remove(node);
            }

            // Make children orphans
            foreach (var child in node.Children.ToList())
            {
                child.Parent = null;
                _orphans.Add(child);
            }

            // Remove from all collections
            _nodesById.Remove(id);
            _orphans.Remove(node);

            return true;
        }

        /// <summary>
        /// Clears collection
        /// </summary>
        public void Clear()
        {
            _nodesById.Clear();
            _roots.Clear();
            _orphans.Clear();
        }

        /// <summary>
        /// Checks if node with specified ID is contained
        /// </summary>
        /// <param name="id">Node ID</param>
        /// <returns>true if node is contained in collection</returns>
        public bool Contains(long id)
        {
            return _nodesById.ContainsKey(id);
        }

        /// <summary>
        /// Gets collection statistics
        /// </summary>
        /// <returns>Statistics object</returns>
        public TreeCollectionStats GetStats()
        {
            var allNodes = _nodesById.Values;
            var maxDepth = _roots.Any() ? _roots.Max(root => root.MaxDepth) : 0;
            var totalNodes = allNodes.Count();

            return new TreeCollectionStats
            {
                TotalNodes = totalNodes,
                RootNodes = _roots.Count,
                LeafNodes = Leaves.Count(),
                OrphanNodes = _orphans.Count,
                MaxDepth = maxDepth,
                AverageChildrenPerNode = totalNodes > 0 ? allNodes.Average(n => n.Children.Count) : 0
            };
        }

        /// <summary>
        /// Gets flattened list of all nodes with level indication
        /// </summary>
        /// <returns>List of (node, level) pairs</returns>
        public IEnumerable<(ITreeRedbObject Node, int Level)> GetFlattenedWithLevels()
        {
            var result = new List<(ITreeRedbObject, int)>();

            foreach (var root in _roots)
            {
                foreach (var node in root.GetSubtree())
                {
                    var level = node.Level;
                    result.Add((node, level));
                }
            }

            // Add orphans with level -1 (undefined)
            foreach (var orphan in _orphans)
            {
                result.Add((orphan, -1));
            }

            return result;
        }

        /// <summary>
        /// Validates tree integrity
        /// </summary>
        /// <returns>List of found issues</returns>
        public IEnumerable<string> ValidateIntegrity()
        {
            var issues = new List<string>();

            foreach (var node in _nodesById.Values)
            {
                // Check that ParentId matches Parent
                if (node.ParentId.HasValue)
                {
                    if (node.Parent == null)
                    {
                        issues.Add($"Node {node.Id} has ParentId={node.ParentId}, but Parent=null");
                    }
                    else if (node.Parent.Id != node.ParentId.Value)
                    {
                        issues.Add($"Node {node.Id}: ParentId={node.ParentId} doesn't match Parent.Id={node.Parent.Id}");
                    }
                }

                // Check that all children have correct back reference
                foreach (var child in node.Children)
                {
                    if (child.Parent != node)
                    {
                        issues.Add($"Child node {child.Id} doesn't reference parent {node.Id}");
                    }
                    if (child.ParentId != node.Id)
                    {
                        issues.Add($"Child node {child.Id}: ParentId={child.ParentId} doesn't match parent {node.Id}");
                    }
                }
            }

            return issues;
        }

        #region Private Methods

        /// <summary>
        /// Establishes parent-child relationship
        /// </summary>
        private void ConnectChild(ITreeRedbObject parent, ITreeRedbObject child)
        {
            child.Parent = parent;
            if (!parent.Children.Contains(child))
            {
                parent.Children.Add(child);
            }
        }

        /// <summary>
        /// Checks if node became parent for any orphans
        /// </summary>
        private void CheckForOrphansToAdopt(ITreeRedbObject potentialParent)
        {
            var childrenToAdopt = _orphans.Where(orphan => orphan.ParentId == potentialParent.Id).ToList();
            
            foreach (var child in childrenToAdopt)
            {
                ConnectChild(potentialParent, child);
                _orphans.Remove(child);
            }
        }

        #endregion

        #region IEnumerable Implementation

        public IEnumerator<ITreeRedbObject> GetEnumerator()
        {
            return _nodesById.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }

    /// <summary>
    /// Typed version of collection for backward compatibility
    /// Wrapper over polymorphic TreeCollection with type safety
    /// </summary>
    /// <typeparam name="TProps">Type of object properties</typeparam>
    public class TreeCollection<TProps> : IEnumerable<ITreeRedbObject<TProps>> where TProps : class, new()
    {
        private readonly TreeCollection _baseCollection = new();

        /// <summary>
        /// Gets count of nodes in collection
        /// </summary>
        public int Count => _baseCollection.Count;

        /// <summary>
        /// Gets all root nodes (without parents)
        /// </summary>
        public IEnumerable<ITreeRedbObject<TProps>> Roots => _baseCollection.Roots.Cast<ITreeRedbObject<TProps>>();

        /// <summary>
        /// Gets all leaf nodes (without children)
        /// </summary>
        public IEnumerable<ITreeRedbObject<TProps>> Leaves => _baseCollection.Leaves.Cast<ITreeRedbObject<TProps>>();

        /// <summary>
        /// Gets orphan nodes (whose parents haven't been added to collection)
        /// </summary>
        public IEnumerable<ITreeRedbObject<TProps>> Orphans => _baseCollection.Orphans.Cast<ITreeRedbObject<TProps>>();

        /// <summary>
        /// Adds typed node to collection
        /// </summary>
        public void Add(ITreeRedbObject<TProps> node)
        {
            _baseCollection.Add(node);
        }

        /// <summary>
        /// Adds range of typed nodes
        /// </summary>
        public void AddRange(IEnumerable<ITreeRedbObject<TProps>> nodes)
        {
            _baseCollection.AddRange(nodes.Cast<ITreeRedbObject>());
        }

        /// <summary>
        /// Finds typed node by ID
        /// </summary>
        public ITreeRedbObject<TProps>? FindById(long id)
        {
            return _baseCollection.FindById(id) as ITreeRedbObject<TProps>;
        }

        /// <summary>
        /// Finds typed nodes by predicate
        /// </summary>
        public IEnumerable<ITreeRedbObject<TProps>> FindNodes(Func<ITreeRedbObject<TProps>, bool> predicate)
        {
            return _baseCollection.FindNodes(node => node is ITreeRedbObject<TProps> typed && predicate(typed))
                .Cast<ITreeRedbObject<TProps>>();
        }

        /// <summary>
        /// Removes node from collection
        /// </summary>
        public bool Remove(long id) => _baseCollection.Remove(id);

        /// <summary>
        /// Clears collection
        /// </summary>
        public void Clear() => _baseCollection.Clear();

        /// <summary>
        /// Checks if node with specified ID is contained
        /// </summary>
        public bool Contains(long id) => _baseCollection.Contains(id);

        /// <summary>
        /// Gets collection statistics
        /// </summary>
        public TreeCollectionStats GetStats() => _baseCollection.GetStats();

        /// <summary>
        /// Gets flattened list of all typed nodes with level indication
        /// </summary>
        public IEnumerable<(ITreeRedbObject<TProps> Node, int Level)> GetFlattenedWithLevels()
        {
            return _baseCollection.GetFlattenedWithLevels()
                .Where(item => item.Node is ITreeRedbObject<TProps>)
                .Select(item => ((ITreeRedbObject<TProps>)item.Node, item.Level));
        }

        /// <summary>
        /// Validates tree integrity
        /// </summary>
        public IEnumerable<string> ValidateIntegrity() => _baseCollection.ValidateIntegrity();

        public IEnumerator<ITreeRedbObject<TProps>> GetEnumerator()
        {
            return _baseCollection.Cast<ITreeRedbObject<TProps>>().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    /// <summary>
    /// Tree collection statistics
    /// </summary>
    public class TreeCollectionStats
    {
        public int TotalNodes { get; set; }
        public int RootNodes { get; set; }
        public int LeafNodes { get; set; }
        public int OrphanNodes { get; set; }
        public int MaxDepth { get; set; }
        public double AverageChildrenPerNode { get; set; }

        public override string ToString()
        {
            return $"Nodes: {TotalNodes}, Roots: {RootNodes}, Leaves: {LeafNodes}, Orphans: {OrphanNodes}, MaxDepth: {MaxDepth}, AvgChildren: {AverageChildrenPerNode:F1}";
        }
    }
}
