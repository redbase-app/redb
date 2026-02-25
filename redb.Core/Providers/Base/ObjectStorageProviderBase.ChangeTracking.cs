using redb.Core.Models.Entities;
using System.Collections.Generic;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Pending values fields for ChangeTracking (used in CommitAllChangesBatch).
    /// In OpenSource these remain empty. Full ChangeTracking implementation is in Pro edition.
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
