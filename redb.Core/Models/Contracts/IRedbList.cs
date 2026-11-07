using System;
using System.Collections.Generic;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB list interface
    /// Represents a reference list for list-type fields
    /// </summary>
    public interface IRedbList
    {
        /// <summary>
        /// Unique list identifier
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// List name
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// List alias (short name)
        /// </summary>
        string? Alias { get; }
        
        // === DDD methods ===
        
        /// <summary>
        /// List items collection (read-only)
        /// </summary>
        IReadOnlyCollection<IRedbListItem> Items { get; }
        
        /// <summary>
        /// Add item to list (Aggregate Root pattern)
        /// Automatically sets IdList
        /// </summary>
        IRedbListItem AddItem(string value, long? idObject = null, string? alias = null);
        
        /// <summary>
        /// Add item to list with linked object (Aggregate Root pattern)
        /// Automatically extracts Id from object
        /// </summary>
        IRedbListItem AddItem(string value, IRedbObject linkedObject, string? alias = null);
        
        /// <summary>
        /// Create item for list without adding to collection
        /// Useful when you need to save item separately
        /// </summary>
        IRedbListItem CreateItem(string value, long? idObject = null, string? alias = null);
        
        /// <summary>
        /// Create item for list with linked object
        /// Automatically extracts Id from object
        /// </summary>
        IRedbListItem CreateItem(string value, IRedbObject linkedObject, string? alias = null);
        
        /// <summary>
        /// Remove item from list
        /// </summary>
        bool RemoveItem(IRedbListItem item);
        
        /// <summary>
        /// Find item by value
        /// </summary>
        IRedbListItem? FindItemByValue(string value);
    }
}
