using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB list item interface
    /// Represents a reference list element
    /// </summary>
    public interface IRedbListItem
    {
        /// <summary>
        /// Unique list item identifier
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// Identifier of the list to which the item belongs
        /// </summary>
        long IdList { get; }
        
        /// <summary>
        /// List item text value
        /// </summary>
        string Value { get; }
        
        /// <summary>
        /// Object identifier (if list item references an object)
        /// </summary>
        long? IdObject { get; }
        
        /// <summary>
        /// List item alias (short description)
        /// </summary>
        string? Alias { get; }
        
        /// <summary>
        /// Fully lazy loading of linked object
        /// Automatically loaded on first access through global loader
        /// Returns null if IdObject is null or loader is not configured
        /// </summary>
        IRedbObject? Object { get; }
    }
}
