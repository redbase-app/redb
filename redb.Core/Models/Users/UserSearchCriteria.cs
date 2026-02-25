using System;

namespace redb.Core.Models.Users
{
    /// <summary>
    /// User search criteria
    /// </summary>
    public class UserSearchCriteria
    {
        /// <summary>
        /// Search by login (partial match)
        /// </summary>
        public string? LoginPattern { get; set; }
        
        /// <summary>
        /// Search by name (partial match)
        /// </summary>
        public string? NamePattern { get; set; }
        
        /// <summary>
        /// Search by email (partial match)
        /// </summary>
        public string? EmailPattern { get; set; }
        
        /// <summary>
        /// Filter by activity status
        /// null - all users, true - only active, false - only inactive
        /// </summary>
        public bool? Enabled { get; set; }
        
        /// <summary>
        /// Filter by role (role ID)
        /// </summary>
        public long? RoleId { get; set; }
        
        /// <summary>
        /// Filter by registration date (from)
        /// </summary>
        public DateTimeOffset? RegisteredFrom { get; set; }
        
        /// <summary>
        /// Filter by registration date (to)
        /// </summary>
        public DateTimeOffset? RegisteredTo { get; set; }
        
        /// <summary>
        /// Exclude system users (ID 0, 1)
        /// </summary>
        public bool ExcludeSystemUsers { get; set; } = true;
        
        // === NEW FIELDS FOR FILTERING ===
        
        /// <summary>
        /// Filter by user key (exact match)
        /// </summary>
        public long? KeyValue { get; set; }
        
        /// <summary>
        /// Filter by integer code (exact match)
        /// </summary>
        public long? CodeIntValue { get; set; }
        
        /// <summary>
        /// Search by string code (partial match)
        /// </summary>
        public string? CodeStringPattern { get; set; }
        
        /// <summary>
        /// Search by note (partial match)
        /// </summary>
        public string? NotePattern { get; set; }
        
        /// <summary>
        /// Filter by GUID code (exact match, rarely used)
        /// </summary>
        public Guid? CodeGuidValue { get; set; }
        
        /// <summary>
        /// Maximum number of results (0 = no limit)
        /// </summary>
        public int Limit { get; set; } = 100;
        
        /// <summary>
        /// Offset for pagination
        /// </summary>
        public int Offset { get; set; } = 0;
        
        /// <summary>
        /// Field for sorting
        /// </summary>
        public UserSortField SortBy { get; set; } = UserSortField.Name;
        
        /// <summary>
        /// Sort direction
        /// </summary>
        public UserSortDirection SortDirection { get; set; } = UserSortDirection.Ascending;
    }
    
    /// <summary>
    /// Fields for sorting users
    /// </summary>
    public enum UserSortField
    {
        Id,
        Login,
        Name,
        Email,
        DateRegister,
        DateDismiss,
        Enabled,
        
        // === NEW FIELDS FOR SORTING ===
        Key,
        CodeInt,
        CodeString,
        Note
        // Hash not added - technical field
        // CodeGuid rarely used for sorting
    }
    
    /// <summary>
    /// User sort direction
    /// </summary>
    public enum UserSortDirection
    {
        Ascending,
        Descending
    }
}
