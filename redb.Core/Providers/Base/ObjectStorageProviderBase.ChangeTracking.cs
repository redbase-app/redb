using redb.Core.Models.Entities;
using System.Collections.Generic;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Pending-каналы ChangeTracking: дифф (Pro) складывает сюда DELETE/INSERT/UPDATE, а
    /// SaveBatchWithChangeTrackingStrategy исполняет их одной точкой flush в живом порядке
    /// DELETE -> UPDATE -> INSERT. В OpenSource остаются пустыми (CT - Pro-фича).
    /// </summary>
    public abstract partial class ObjectStorageProviderBase
    {
        /// <summary>
        /// Values to update (from ChangeTracking diff). Pro only.
        /// </summary>
        protected List<RedbValue> _pendingValuesToUpdate = [];

        /// <summary>
        /// Values to insert (from ChangeTracking diff). Pro only.
        /// </summary>
        protected List<RedbValue> _pendingValuesToInsert = [];

        /// <summary>
        /// Value IDs to delete (from ChangeTracking diff). Pro only.
        /// </summary>
        protected List<long> _pendingValuesToDelete = [];
    }
}
