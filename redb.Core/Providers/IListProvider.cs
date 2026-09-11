using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for working with dictionaries (Lists) and their items (ListItems).
    /// </summary>
    public interface IListProvider
    {
        /// <summary>
        /// Loader attached to every <see cref="RedbListItem"/> this provider hands out, so that
        /// <see cref="RedbListItem.Object"/> resolves through the scope that materialized the item
        /// (or a fresh one once that scope is gone) instead of a process-wide delegate. Set by the
        /// owning <see cref="IRedbService"/>; <c>null</c> leaves items on the process-wide fallback.
        /// </summary>
        Func<long, Task<IRedbObject?>>? LinkedObjectLoader { get => null; set { } }

        /// <summary>
        /// Synchronous twin of <see cref="LinkedObjectLoader"/> for the thread-pool-free lazy
        /// path: the sync getter of <see cref="RedbListItem.Object"/> prefers it, running the
        /// whole load on the calling thread down to ADO.NET (no thread-pool continuations, so a
        /// saturated pool cannot slow or deadlock the getter). <c>null</c> leaves the sync getter
        /// on the blocking-over-async fallback.
        /// </summary>
        Func<long, IRedbObject?>? LinkedObjectSyncLoader { get => null; set { } }

        /// <summary>
        /// Batch form of <see cref="LinkedObjectLoader"/>: resolves many linked objects in one
        /// round-trip for the hand-out preload (<c>PreloadListItemLinkedObjects</c>). Returns
        /// only the objects that exist; a missing id simply stays lazy on its item.
        /// </summary>
        Func<IReadOnlyCollection<long>, CancellationToken, Task<IReadOnlyDictionary<long, IRedbObject>>>? LinkedObjectsBatchLoader { get => null; set { } }

        // === LIST CRUD ===

        Task<RedbList?> GetListAsync(long listId,
        CancellationToken cancellationToken = default);
        Task<RedbList?> GetListByNameAsync(string name,
        CancellationToken cancellationToken = default);
        Task<List<RedbList>> GetAllListsAsync(CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get list with all its items loaded.
        /// </summary>
        Task<RedbList?> GetListWithItemsAsync(long listId,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Get list by name with all its items loaded.
        /// </summary>
        Task<RedbList?> GetListByNameWithItemsAsync(string name,
        CancellationToken cancellationToken = default);
        Task<RedbList> SaveListAsync(IRedbList list,
        CancellationToken cancellationToken = default);
        Task<bool> DeleteListAsync(long listId,
        CancellationToken cancellationToken = default);
        
        // === ITEM CRUD ===
        
        Task<RedbListItem?> GetListItemAsync(long itemId,
        CancellationToken cancellationToken = default);
        Task<List<RedbListItem>> GetListItemsAsync(long listId,
        CancellationToken cancellationToken = default);
        Task<RedbListItem?> GetListItemByValueAsync(long listId, string value,
        CancellationToken cancellationToken = default);
        Task<RedbListItem> SaveListItemAsync(IRedbListItem item,
        CancellationToken cancellationToken = default);
        Task<bool> DeleteListItemAsync(long itemId,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Add multiple items from ready objects.
        /// </summary>
        Task<List<RedbListItem>> AddItemsAsync(IRedbList list, IEnumerable<IRedbListItem> items,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Add multiple items from string values (convenience method).
        /// </summary>
        Task<List<RedbListItem>> AddItemsAsync(IRedbList list, IEnumerable<string> values, IEnumerable<string>? aliases = null,
        CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Save list with all its items (Aggregate Root).
        /// </summary>
        Task<RedbList> SaveListWithItemsAsync(IRedbList list,
        CancellationToken cancellationToken = default);
        
        // === SPECIFIC METHODS ===
        
        Task<List<RedbListItem>> GetItemsByObjectReferenceAsync(long objectId,
        CancellationToken cancellationToken = default);
        Task<bool> IsListUsedInStructuresAsync(long listId,
        CancellationToken cancellationToken = default);
        Task<RedbList> SyncListFromEnumAsync<TEnum>(string? listName = null,
        CancellationToken cancellationToken = default) where TEnum : struct, Enum;
    }
}
