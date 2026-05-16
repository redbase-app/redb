using System;
using System.Collections.Generic;
using System.Linq;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Core.Utils
{
    /// <summary>
    /// Extensions for working with tree structures.
    /// </summary>
    public static class TreeExtensions
    {
        /// <summary>
        /// Breadth-First Search tree traversal for polymorphic trees.
        /// </summary>
        /// <param name="root">Root node for traversal</param>
        /// <returns>Node sequence in BFS order</returns>
        public static IEnumerable<ITreeRedbObject> BreadthFirstTraversal(this ITreeRedbObject root)
        {
            var queue = new Queue<ITreeRedbObject>();
            queue.Enqueue(root);
            
            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                yield return current;
                
                foreach (var child in current.Children)
                    queue.Enqueue(child);
            }
        }

        /// <summary>
        /// Breadth-First Search tree traversal for typed trees.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node for traversal</param>
        /// <returns>Node sequence in BFS order</returns>
        public static IEnumerable<ITreeRedbObject<TProps>> BreadthFirstTraversal<TProps>(this ITreeRedbObject<TProps> root)
            where TProps : class, new()
        {
            var queue = new Queue<ITreeRedbObject<TProps>>();
            queue.Enqueue(root);
            
            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                yield return current;
                
                // Children is untyped, cast to typed version
                foreach (var child in current.Children.OfType<ITreeRedbObject<TProps>>())
                    queue.Enqueue(child);
            }
        }
        
        /// <summary>
        /// Depth-First Search tree traversal - pre-order for polymorphic trees.
        /// </summary>
        /// <param name="root">Root node for traversal</param>
        /// <returns>Node sequence in DFS pre-order</returns>
        public static IEnumerable<ITreeRedbObject> DepthFirstTraversal(this ITreeRedbObject root)
        {
            yield return root;
            
            foreach (var child in root.Children)
            {
                foreach (var descendant in child.DepthFirstTraversal())
                    yield return descendant;
            }
        }

        /// <summary>
        /// Depth-First Search tree traversal - pre-order for typed trees.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node for traversal</param>
        /// <returns>Node sequence in DFS pre-order</returns>
        public static IEnumerable<ITreeRedbObject<TProps>> DepthFirstTraversal<TProps>(this ITreeRedbObject<TProps> root)
            where TProps : class, new()
        {
            yield return root;
            
            // Children is untyped, cast to typed version
            foreach (var child in root.Children.OfType<ITreeRedbObject<TProps>>())
            {
                foreach (var descendant in child.DepthFirstTraversal())
                    yield return descendant;
            }
        }
        
        /// <summary>
        /// Depth-First Search tree traversal - post-order for polymorphic trees.
        /// </summary>
        /// <param name="root">Root node for traversal</param>
        /// <returns>Node sequence in DFS post-order</returns>
        public static IEnumerable<ITreeRedbObject> PostOrderTraversal(this ITreeRedbObject root)
        {
            foreach (var child in root.Children)
            {
                foreach (var descendant in child.PostOrderTraversal())
                    yield return descendant;
            }
            
            yield return root;
        }

        /// <summary>
        /// Depth-First Search tree traversal - post-order for typed trees.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node for traversal</param>
        /// <returns>Node sequence in DFS post-order</returns>
        public static IEnumerable<ITreeRedbObject<TProps>> PostOrderTraversal<TProps>(this ITreeRedbObject<TProps> root)
            where TProps : class, new()
        {
            // Children is untyped, cast to typed version
            foreach (var child in root.Children.OfType<ITreeRedbObject<TProps>>())
            {
                foreach (var descendant in child.PostOrderTraversal())
                    yield return descendant;
            }
            
            yield return root;
        }
        
        /// <summary>
        /// Find node by ID in polymorphic tree.
        /// </summary>
        /// <param name="root">Root node for search</param>
        /// <param name="id">ID of target node</param>
        /// <returns>Found node or null</returns>
        public static ITreeRedbObject? FindById(this ITreeRedbObject root, long id)
        {
            return root.DepthFirstTraversal().FirstOrDefault(node => node.Id == id);
        }

        /// <summary>
        /// Find node by ID in typed tree.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node for search</param>
        /// <param name="id">ID of target node</param>
        /// <returns>Found node or null</returns>
        public static ITreeRedbObject<TProps>? FindById<TProps>(this ITreeRedbObject<TProps> root, long id)
            where TProps : class, new()
        {
            return root.DepthFirstTraversal().FirstOrDefault(node => node.Id == id);
        }
        
        /// <summary>
        /// Find nodes by predicate in polymorphic tree.
        /// </summary>
        /// <param name="root">Root node for search</param>
        /// <param name="predicate">Search predicate</param>
        /// <returns>Collection of found nodes</returns>
        public static IEnumerable<ITreeRedbObject> FindNodes(this ITreeRedbObject root, 
            Func<ITreeRedbObject, bool> predicate)
        {
            return root.DepthFirstTraversal().Where(predicate);
        }

        /// <summary>
        /// Find nodes by predicate in typed tree.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node for search</param>
        /// <param name="predicate">Search predicate</param>
        /// <returns>Collection of found nodes</returns>
        public static IEnumerable<ITreeRedbObject<TProps>> FindNodes<TProps>(this ITreeRedbObject<TProps> root, 
            Func<ITreeRedbObject<TProps>, bool> predicate)
            where TProps : class, new()
        {
            return root.DepthFirstTraversal().Where(predicate);
        }
        
        /// <summary>
        /// Gets all leaf nodes of polymorphic tree.
        /// </summary>
        /// <param name="root">Root node</param>
        /// <returns>Collection of leaf nodes</returns>
        public static IEnumerable<ITreeRedbObject> GetLeaves(this ITreeRedbObject root)
        {
            return root.DepthFirstTraversal().Where(node => node.IsLeaf);
        }

        /// <summary>
        /// Gets all leaf nodes of typed tree.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node</param>
        /// <returns>Collection of leaf nodes</returns>
        public static IEnumerable<ITreeRedbObject<TProps>> GetLeaves<TProps>(this ITreeRedbObject<TProps> root)
            where TProps : class, new()
        {
            return root.DepthFirstTraversal().Where(node => node.IsLeaf);
        }
        
        /// <summary>
        /// Gets all nodes at specific level in polymorphic tree.
        /// </summary>
        /// <param name="root">Root node</param>
        /// <param name="level">Level (0 for root)</param>
        /// <returns>Collection of nodes at specified level</returns>
        public static IEnumerable<ITreeRedbObject> GetNodesAtLevel(this ITreeRedbObject root, int level)
        {
            return root.DepthFirstTraversal().Where(node => node.Level == level);
        }

        /// <summary>
        /// Gets all nodes at specific level in typed tree.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node</param>
        /// <param name="level">Level (0 for root)</param>
        /// <returns>Collection of nodes at specified level</returns>
        public static IEnumerable<ITreeRedbObject<TProps>> GetNodesAtLevel<TProps>(this ITreeRedbObject<TProps> root, int level)
            where TProps : class, new()
        {
            return root.DepthFirstTraversal().Where(node => node.Level == level);
        }
        
        /// <summary>
        /// Builds materialized path for polymorphic node.
        /// </summary>
        /// <param name="node">Node</param>
        /// <param name="separator">Path separator</param>
        /// <returns>Materialized path like "/1/5/23"</returns>
        public static string GetMaterializedPath(this ITreeRedbObject node, string separator = "/")
        {
            var pathIds = node.GetPathIds();
            return separator + string.Join(separator, pathIds);
        }

        /// <summary>
        /// Builds materialized path for typed node.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="node">Node</param>
        /// <param name="separator">Path separator</param>
        /// <returns>Materialized path like "/1/5/23"</returns>
        public static string GetMaterializedPath<TProps>(this ITreeRedbObject<TProps> node, string separator = "/")
            where TProps : class, new()
        {
            var pathIds = node.GetPathIds();
            return separator + string.Join(separator, pathIds);
        }
        
        /// <summary>
        /// Flattens polymorphic tree to list with level indicators.
        /// </summary>
        /// <param name="root">Root node</param>
        /// <returns>List of (node, level) pairs</returns>
        public static IEnumerable<(ITreeRedbObject Node, int Level)> FlattenWithLevels(this ITreeRedbObject root)
        {
            return root.DepthFirstTraversal().Select(node => (node, node.Level));
        }

        /// <summary>
        /// Flattens typed tree to list with level indicators.
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node</param>
        /// <returns>List of (node, level) pairs</returns>
        public static IEnumerable<(ITreeRedbObject<TProps> Node, int Level)> FlattenWithLevels<TProps>(this ITreeRedbObject<TProps> root)
            where TProps : class, new()
        {
            return root.DepthFirstTraversal().Select(node => (node, node.Level));
        }
        
        /// <summary>
        /// Checks if polymorphic tree is balanced (subtree depth difference does not exceed 1).
        /// </summary>
        /// <param name="root">Root node</param>
        /// <returns>true if tree is balanced</returns>
        public static bool IsBalanced(this ITreeRedbObject root)
        {
            return CheckBalance(root) != -1;
        }

        /// <summary>
        /// Checks if typed tree is balanced (subtree depth difference does not exceed 1).
        /// </summary>
        /// <typeparam name="TProps">Object properties type</typeparam>
        /// <param name="root">Root node</param>
        /// <returns>true if tree is balanced</returns>
        public static bool IsBalanced<TProps>(this ITreeRedbObject<TProps> root)
            where TProps : class, new()
        {
            return CheckBalance(root) != -1;
        }
        
        private static int CheckBalance(ITreeRedbObject node)
        {
            if (!node.Children.Any())
                return 0;
            
            var childHeights = new List<int>();
            foreach (var child in node.Children)
            {
                var height = CheckBalance(child);
                if (height == -1) return -1; // Not balanced
                childHeights.Add(height);
            }
            
            var maxHeight = childHeights.Max();
            var minHeight = childHeights.Min();
            
            return Math.Abs(maxHeight - minHeight) <= 1 ? maxHeight + 1 : -1;
        }
        
        private static int CheckBalance<TProps>(ITreeRedbObject<TProps> node)
            where TProps : class, new()
        {
            if (!node.Children.Any())
                return 0;
            
            var childHeights = new List<int>();
            foreach (var child in node.Children)
            {
                var height = CheckBalance(child);
                if (height == -1) return -1; // Not balanced
                childHeights.Add(height);
            }
            
            var maxHeight = childHeights.Max();
            var minHeight = childHeights.Min();
            
            return Math.Abs(maxHeight - minHeight) <= 1 ? maxHeight + 1 : -1;
        }
    }
}
