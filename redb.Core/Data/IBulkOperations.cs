using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using redb.Core.Models.Entities;

namespace redb.Core.Data
{
    /// <summary>
    /// Bulk operations abstraction for high-performance data operations.
    /// Replaces EFCore.BulkExtensions with pure ADO.NET COPY protocol.
    /// </summary>
    public interface IBulkOperations
    {
        /// <summary>
        /// Bulk insert objects using COPY protocol.
        /// Much faster than individual INSERTs for large datasets.
        /// </summary>
        /// <param name="objects">Objects to insert.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task BulkInsertObjectsAsync(IEnumerable<RedbObjectRow> objects, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk insert values using COPY protocol.
        /// </summary>
        /// <param name="values">Values to insert.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task BulkInsertValuesAsync(IEnumerable<RedbValue> values, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk update objects.
        /// Uses temp table + UPDATE FROM pattern for efficiency.
        /// </summary>
        /// <param name="objects">Objects to update.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task BulkUpdateObjectsAsync(IEnumerable<RedbObjectRow> objects, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk update values.
        /// Uses temp table + UPDATE FROM pattern for efficiency.
        /// </summary>
        /// <param name="values">Values to update.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task BulkUpdateValuesAsync(IEnumerable<RedbValue> values, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk delete objects by IDs.
        /// Uses ANY() with array for efficiency.
        /// </summary>
        /// <param name="objectIds">Object IDs to delete.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task BulkDeleteObjectsAsync(IEnumerable<long> objectIds, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk delete values by IDs.
        /// </summary>
        /// <param name="valueIds">Value IDs to delete.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task BulkDeleteValuesAsync(IEnumerable<long> valueIds, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk delete values by object IDs.
        /// Commonly used before re-inserting all values for object.
        /// </summary>
        /// <param name="objectIds">Object IDs whose values to delete.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task BulkDeleteValuesByObjectIdsAsync(IEnumerable<long> objectIds, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk delete values by ListItem IDs.
        /// Used when deleting list items that are referenced by values.
        /// </summary>
        /// <param name="listItemIds">ListItem IDs whose referencing values to delete.</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task BulkDeleteValuesByListItemIdsAsync(IEnumerable<long> listItemIds, CancellationToken cancellationToken = default);
    }
}

