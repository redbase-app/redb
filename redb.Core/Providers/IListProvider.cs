using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for working with dictionaries (Lists) and their items (ListItems).
    /// </summary>
    public interface IListProvider
    {
        // === LIST CRUD ===
        
        Task<RedbList?> GetListAsync(long listId);
        Task<RedbList?> GetListByNameAsync(string name);
        Task<List<RedbList>> GetAllListsAsync();
        
        /// <summary>
        /// Get list with all its items loaded.
        /// </summary>
        Task<RedbList?> GetListWithItemsAsync(long listId);
        
        /// <summary>
        /// Get list by name with all its items loaded.
        /// </summary>
        Task<RedbList?> GetListByNameWithItemsAsync(string name);
        Task<RedbList> SaveListAsync(IRedbList list);
        Task<bool> DeleteListAsync(long listId);
        
        // === ITEM CRUD ===
        
        Task<RedbListItem?> GetListItemAsync(long itemId);
        Task<List<RedbListItem>> GetListItemsAsync(long listId);
        Task<RedbListItem?> GetListItemByValueAsync(long listId, string value);
        Task<RedbListItem> SaveListItemAsync(IRedbListItem item);
        Task<bool> DeleteListItemAsync(long itemId);
        
        /// <summary>
        /// Add multiple items from ready objects.
        /// </summary>
        Task<List<RedbListItem>> AddItemsAsync(IRedbList list, IEnumerable<IRedbListItem> items);
        
        /// <summary>
        /// Add multiple items from string values (convenience method).
        /// </summary>
        Task<List<RedbListItem>> AddItemsAsync(IRedbList list, IEnumerable<string> values, IEnumerable<string>? aliases = null);
        
        /// <summary>
        /// Save list with all its items (Aggregate Root).
        /// </summary>
        Task<RedbList> SaveListWithItemsAsync(IRedbList list);
        
        // === SPECIFIC METHODS ===
        
        Task<List<RedbListItem>> GetItemsByObjectReferenceAsync(long objectId);
        Task<bool> IsListUsedInStructuresAsync(long listId);
        Task<RedbList> SyncListFromEnumAsync<TEnum>(string? listName = null) where TEnum : struct, Enum;
    }
}
