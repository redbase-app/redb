using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Providers;

namespace redb.Core.Extensions
{
    /// <summary>
    /// Extension methods for IRedbObject for convenient work with trees and objects
    /// </summary>
    public static class RedbObjectExtensions
    {
        // ===== TREE OPERATIONS =====

        /// <summary>
        /// OPTIMIZED version: Checks if object is descendant of specified parent
        /// Uses already loaded hierarchy from GetPathToRootAsync, but adds cycle protection
        /// </summary>
        /// <param name="obj">Object to check</param>
        /// <param name="potentialAncestor">Potential ancestor</param>
        /// <param name="treeProvider">Provider for tree operations</param>
        public static async Task<bool> IsDescendantOfAsync<T>(
            this IRedbObject obj, 
            IRedbObject potentialAncestor,
            ITreeProvider treeProvider) where T : class, new()
        {
            if (obj.Id == potentialAncestor.Id) return false; // Object cannot be descendant of itself
            
            try
            {
                // Use existing method, but with cycle protection
                var pathToRoot = await treeProvider.GetPathToRootAsync<T>(obj);
                
                // OPTIMIZATION: Use HashSet for O(1) search instead of linear scan
                var ancestorIds = new HashSet<long>(pathToRoot.Select(ancestor => ancestor.Id));
                return ancestorIds.Contains(potentialAncestor.Id);
            }
            catch
            {
                return false; // In case of error return false
            }
        }

        /// <summary>
        /// Checks if object is an ancestor of the specified descendant
        /// </summary>
        /// <param name="obj">Object to check</param>
        /// <param name="potentialDescendant">Potential descendant</param>
        /// <param name="treeProvider">Provider for tree operations</param>
        public static async Task<bool> IsAncestorOfAsync<T>(
            this IRedbObject obj, 
            IRedbObject potentialDescendant,
            ITreeProvider treeProvider) where T : class, new()
        {
            return await potentialDescendant.IsDescendantOfAsync<T>(obj, treeProvider);
        }

        /// <summary>
        /// OPTIMIZED version: Gets object level in tree (root = 0)  
        /// Uses cycle protection and more efficient approach
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="treeProvider">Provider for tree operations</param>
        public static async Task<int> GetTreeLevelAsync<T>(
            this IRedbObject obj,
            ITreeProvider treeProvider) where T : class, new()
        {
            try
            {
                var pathToRoot = await treeProvider.GetPathToRootAsync<T>(obj);
                var pathCount = pathToRoot.Count();
                
                // PROTECTION: If path is too long, there may be a cycle
                if (pathCount > 1000) 
                {
                    return -1; // Suspiciously deep tree - possible cycle
                }
                
                return Math.Max(0, pathCount - 1); // -1 because path includes the object itself
            }
            catch
            {
                return -1; // Error determining level
            }
        }

        /// <summary>
        /// Checks if object is a tree leaf (without children)
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="treeProvider">Provider for tree operations</param>
        public static async Task<bool> IsLeafAsync<T>(
            this IRedbObject obj,
            ITreeProvider treeProvider) where T : class, new()
        {
            try
            {
                var children = await treeProvider.GetChildrenAsync<T>(obj);
                return !children.Any();
            }
            catch
            {
                return true; // In case of error consider as leaf
            }
        }

        /// <summary>
        /// Gets count of object children
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="treeProvider">Provider for tree operations</param>
        public static async Task<int> GetChildrenCountAsync<T>(
            this IRedbObject obj,
            ITreeProvider treeProvider) where T : class, new()
        {
            try
            {
                var children = await treeProvider.GetChildrenAsync<T>(obj);
                return children.Count();
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>
        /// Gets count of all object descendants
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="treeProvider">Provider for tree operations</param>
        /// <param name="maxDepth">Maximum search depth</param>
        public static async Task<int> GetDescendantsCountAsync<T>(
            this IRedbObject obj,
            ITreeProvider treeProvider,
            int? maxDepth = null) where T : class, new()
        {
            try
            {
                var descendants = await treeProvider.GetDescendantsAsync<T>(obj, maxDepth);
                return descendants.Count();
            }
            catch
            {
                return 0;
            }
        }

        // ===== STATE CHECKS =====

        /// <summary>
        /// Checks if object is active by timestamps
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="checkDate">Date to check (default - current)</param>
        public static bool IsActiveAt(this IRedbObject obj, DateTime? checkDate = null)
        {
            var date = checkDate ?? DateTime.Now;
            
            // Check begin date
            if (obj.DateBegin.HasValue && date < obj.DateBegin.Value)
                return false;
                
            // Check end date
            if (obj.DateComplete.HasValue && date > obj.DateComplete.Value)
                return false;
                
            return true;
        }

        /// <summary>
        /// Checks if object validity has expired
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="checkDate">Date to check (default - current)</param>
        public static bool IsExpired(this IRedbObject obj, DateTime? checkDate = null)
        {
            var date = checkDate ?? DateTime.Now;
            return obj.DateComplete.HasValue && date > obj.DateComplete.Value;
        }

        /// <summary>
        /// Checks if object has started its validity
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="checkDate">Date to check (default - current)</param>
        public static bool HasStarted(this IRedbObject obj, DateTime? checkDate = null)
        {
            var date = checkDate ?? DateTime.Now;
            return !obj.DateBegin.HasValue || date >= obj.DateBegin.Value;
        }

        /// <summary>
        /// Gets object age (time since creation)
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="referenceDate">Reference date (default - current)</param>
        public static TimeSpan GetAge(this IRedbObject obj, DateTime? referenceDate = null)
        {
            var date = referenceDate ?? DateTime.Now;
            return date - obj.DateCreate;
        }

        /// <summary>
        /// Gets time since last object modification
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="referenceDate">Reference date (default - current)</param>
        public static TimeSpan GetTimeSinceLastModification(this IRedbObject obj, DateTime? referenceDate = null)
        {
            var date = referenceDate ?? DateTime.Now;
            return date - obj.DateModify;
        }

        // ===== UTILITIES =====

        /// <summary>
        /// Gets object display name with fallback logic
        /// </summary>
        /// <param name="obj">Object</param>
        /// <param name="includeId">Whether to include ID in display name</param>
        public static string GetDisplayName(this IRedbObject obj, bool includeId = true)
        {
            var displayName = obj.Name;
            
            // Fallback to values
            if (string.IsNullOrWhiteSpace(displayName))
            {
                if (!string.IsNullOrWhiteSpace(obj.ValueString))
                    displayName = obj.ValueString;
                else if (obj.ValueLong.HasValue)
                    displayName = $"Value_{obj.ValueLong.Value}";
                else if (obj.ValueGuid.HasValue)
                    displayName = obj.ValueGuid.Value.ToString("D").Substring(0, 8);
                else if (obj.Key.HasValue)
                    displayName = $"Key_{obj.Key.Value}";
                else
                    displayName = $"Object_{obj.Id}";
            }
            
            return includeId ? $"{displayName} (#{obj.Id})" : displayName;
        }

        /// <summary>
        /// Gets brief object information for debugging
        /// </summary>
        /// <param name="obj">Object</param>
        public static string GetDebugInfo(this IRedbObject obj)
        {
            return $"Object[Id={obj.Id}, Scheme={obj.SchemeId}, Name='{obj.Name}', Parent={obj.ParentId}, Owner={obj.OwnerId}]";
        }

        /// <summary>
        /// Creates hierarchical path string for object
        /// </summary>
        /// <param name="pathObjects">Path objects from root to object</param>
        /// <param name="separator">Separator (default "/")</param>
        public static string CreateHierarchicalPath(this IEnumerable<IRedbObject> pathObjects, string separator = "/")
        {
            return string.Join(separator, pathObjects.Select(obj => obj.Name ?? $"#{obj.Id}"));
        }
    }
}
