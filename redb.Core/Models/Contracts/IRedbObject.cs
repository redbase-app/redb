using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// Base interface for all REDB objects
    /// Allows working with objects as classes rather than IDs
    /// Enriched with full functionality for working with trees, timestamps and auditing
    /// </summary>
    public interface IRedbObject
    {
        // ===== BASIC IDENTIFIERS =====
        
        /// <summary>
        /// Unique object identifier
        /// Used for extracting ID from object in new methods
        /// </summary>
        long Id { get; set; }
        
        /// <summary>
        /// Object scheme identifier
        /// Used for schema permissions checking and validation
        /// </summary>
        long SchemeId { get; set; }
        
        /// <summary>
        /// Object name
        /// Used for logging, debugging and user interface
        /// </summary>
        string Name { get; set; }

        // ===== TREE STRUCTURE =====
        
        /// <summary>
        /// Parent object identifier
        /// null - if object is root
        /// </summary>
        long? ParentId { get; set; }
        
        /// <summary>
        /// Checks if object has a parent
        /// </summary>
        bool HasParent { get; }
        
        /// <summary>
        /// Checks if object is root (has no parent)
        /// </summary>
        bool IsRoot { get; }

        // ===== TIMESTAMPS =====
        
        /// <summary>
        /// Object creation date and time
        /// </summary>
        DateTimeOffset DateCreate { get; set; }
        
        /// <summary>
        /// Object last modification date and time
        /// </summary>
        DateTimeOffset DateModify { get; set; }
        
        /// <summary>
        /// Object validity start date (optional)
        /// </summary>
        DateTimeOffset? DateBegin { get; set; }
        
        /// <summary>
        /// Object validity end date (optional)
        /// </summary>
        DateTimeOffset? DateComplete { get; set; }

        // ===== OWNERSHIP AND AUDIT =====
        
        /// <summary>
        /// Object owner identifier
        /// </summary>
        long OwnerId { get; set; }
        
        /// <summary>
        /// Identifier of user who last modified the object
        /// </summary>
        long WhoChangeId { get; set; }

        // ===== ADDITIONAL IDENTIFIERS =====
        
        /// <summary>
        /// Object key field
        /// </summary>
        long? Key { get; set; }
        
        // ===== PRIMITIVE VALUES (stored directly in _objects table) =====
        
        /// <summary>
        /// Primitive long value (for primitive schemas)
        /// </summary>
        long? ValueLong { get; set; }
        
        /// <summary>
        /// Primitive string value (for primitive schemas)
        /// </summary>
        string? ValueString { get; set; }
        
        /// <summary>
        /// Primitive GUID value (for primitive schemas)
        /// </summary>
        Guid? ValueGuid { get; set; }
        
        /// <summary>
        /// Primitive boolean value (for primitive schemas)
        /// </summary>
        bool? ValueBool { get; set; }
        
        /// <summary>
        /// Primitive double value (for primitive schemas)
        /// </summary>
        double? ValueDouble { get; set; }
        
        /// <summary>
        /// Primitive decimal value (for primitive schemas)
        /// </summary>
        decimal? ValueNumeric { get; set; }
        
        /// <summary>
        /// Primitive datetime value (for primitive schemas)
        /// </summary>
        DateTimeOffset? ValueDatetime { get; set; }
        
        /// <summary>
        /// Primitive byte array value (for primitive schemas)
        /// </summary>
        byte[]? ValueBytes { get; set; }

        // ===== OBJECT STATE =====
        
        /// <summary>
        /// Object notes
        /// </summary>
        string? Note { get; set; }
        
        /// <summary>
        /// MD5 hash of object for integrity control
        /// </summary>
        Guid? Hash { get; set; }

        /// <summary>
        /// Reset object ID to 0 and optionally ParentId to null
        /// </summary>
        /// <param name="withParent">If true, also resets ParentId to null (default true)</param>
        void ResetId(bool withParent = true);
        
        /// <summary>
        /// Reset object ID and ParentId (recursively processes nested objects)
        /// </summary>
        /// <param name="recursive">If true, recursively resets ID in all nested IRedbObject in Props</param>
        void ResetIds(bool recursive = false);
    }
}
