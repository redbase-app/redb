using System;
using redb.Core.Models.Contracts;

namespace redb.Core.Models.Users
{
    /// <summary>
    /// Request for updating user data
    /// </summary>
    public class UpdateUserRequest
    {
        /// <summary>
        /// New user login (if null - not changed)
        /// System users (ID 0, 1) cannot change login
        /// </summary>
        public string? Login { get; set; }
        
        /// <summary>
        /// New user name (if null - not changed)
        /// System users (ID 0, 1) cannot change name
        /// </summary>
        public string? Name { get; set; }
        
        /// <summary>
        /// New user phone (if null - not changed)
        /// </summary>
        public string? Phone { get; set; }
        
        /// <summary>
        /// New user email (if null - not changed)
        /// </summary>
        public string? Email { get; set; }
        
        /// <summary>
        /// New activity status (if null - not changed)
        /// </summary>
        public bool? Enabled { get; set; }
        
        /// <summary>
        /// Dismissal date (if null - not changed)
        /// </summary>
        public DateTimeOffset? DateDismiss { get; set; }
        
        /// <summary>
        /// New user roles as objects (if null - not changed)
        /// For backward compatibility and programmatic usage
        /// If empty array is specified - all roles are removed
        /// </summary>
        public IRedbRole[]? Roles { get; set; }
        
        /// <summary>
        /// Names of new user roles (if null - not changed)
        /// Example: ["Admin", "Manager"]
        /// If both Roles and RoleNames are specified - RoleNames are used (priority)
        /// If empty array is specified - all roles are removed
        /// </summary>
        public string[]? RoleNames { get; set; }
        
        // === NEW EXTENDED FIELDS ===
        
        /// <summary>
        /// New additional key (if null - not changed)
        /// </summary>
        public long? Key { get; set; }
        
        /// <summary>
        /// New integer code (if null - not changed)
        /// </summary>
        public long? CodeInt { get; set; }
        
        /// <summary>
        /// New string code (if null - not changed)
        /// If empty string - code is cleared
        /// </summary>
        public string? CodeString { get; set; }
        
        /// <summary>
        /// New GUID code (if null - not changed)
        /// </summary>
        public Guid? CodeGuid { get; set; }
        
        /// <summary>
        /// New note (if null - not changed)
        /// If empty string - note is cleared
        /// </summary>
        public string? Note { get; set; }
        
        // Hash is NOT added to UpdateUserRequest - it is recalculated automatically!
    }
}
