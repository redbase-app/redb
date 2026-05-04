using System.Collections.Generic;
using System.Linq;
using redb.Core.Data;
using redb.Core.Models.Entities;

namespace redb.Core.Utils
{
    /// <summary>
    /// Topological sorting of _values to maintain FK constraint on _array_parent_id.
    /// Parent records must be inserted BEFORE children in BulkInsert.
    /// </summary>
    public static class ValuesTopologicalSort
    {
        /// <summary>
        /// Sort values by ArrayParentId dependencies (single-threaded).
        /// BFS algorithm: first records without parent, then their children, etc.
        /// </summary>
        public static List<RedbValue> SortByFkDependency(List<RedbValue> values)
        {
            if (values.Count <= 1) return values;
            
            var valueIds = values.Select(v => v.Id).ToHashSet();
            
            // BFS sorting
            var byParent = values
                .Where(v => v.ArrayParentId.HasValue)
                .ToLookup(v => v.ArrayParentId!.Value);
            
            var result = new List<RedbValue>(values.Count);
            var added = new HashSet<long>();
            var queue = new Queue<RedbValue>();
            
            // Start with records WITHOUT parent OR with EXTERNAL parent (parent in DB, not in batch)
            foreach (var v in values.Where(v => !v.ArrayParentId.HasValue || !valueIds.Contains(v.ArrayParentId.Value)))
            {
                queue.Enqueue(v);
                added.Add(v.Id);
            }
            
            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                result.Add(current);
                
                foreach (var child in byParent[current.Id])
                {
                    if (!added.Contains(child.Id))
                    {
                        queue.Enqueue(child);
                        added.Add(child.Id);
                    }
                }
            }
            
            // If records remain (circular dependencies?), add to end
            if (result.Count < values.Count)
            {
                var remaining = values.Where(v => !added.Contains(v.Id)).ToList();
                result.AddRange(remaining);
            }
            
            return result;
        }
    }
}
