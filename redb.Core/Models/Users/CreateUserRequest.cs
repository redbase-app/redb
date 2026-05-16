using System;
using redb.Core.Models.Contracts;

namespace redb.Core.Models.Users
{
    /// <summary>
    /// Request for creating new user
    /// </summary>
    public class CreateUserRequest
    {
        /// <summary>
        /// User login (unique)
        /// </summary>
        public string Login { get; set; } = "";
        
        /// <summary>
        /// User password (in plain text, will be hashed)
        /// </summary>
        public string Password { get; set; } = "";
        
        /// <summary>
        /// User name
        /// </summary>
        public string Name { get; set; } = "";
        
        /// <summary>
        /// User phone (optional)
        /// </summary>
        public string? Phone { get; set; }
        
        /// <summary>
        /// User email (optional)
        /// </summary>
        public string? Email { get; set; }
        
        /// <summary>
        /// Is user active upon creation
        /// </summary>
        public bool Enabled { get; set; } = true;
        
        /// <summary>
        /// Roles to assign to user upon creation (objects)
        /// For backward compatibility and programmatic usage
        /// </summary>
        public IRedbRole[]? Roles { get; set; }
        
        /// <summary>
        /// Role names to assign to user upon creation
        /// Example: ["Admin", "Manager", "User"]
        /// If both Roles and RoleNames are specified - RoleNames are used (priority)
        /// </summary>
        public string[]? RoleNames { get; set; }
        
        /// <summary>
        /// Registration date (if not specified, current date is used)
        /// </summary>
        public DateTimeOffset? DateRegister { get; set; }
        
        // === NEW EXTENDED FIELDS ===
        
        /// <summary>
        /// Additional user key (optional)
        /// Can be used for external integrations or additional identification
        /// </summary>
        public long? Key { get; set; }
        
        /// <summary>
        /// Integer user code (optional)
        /// Can be used for categorization, access groups or external systems
        /// </summary>
        public long? CodeInt { get; set; }
        
        /// <summary>
        /// String user code (optional)
        /// Can be used for department codes, branches or special labels
        /// </summary>
        public string? CodeString { get; set; }
        
        /// <summary>
        /// GUID user code (optional)
        /// Can be used for unique identification in distributed systems
        /// </summary>
        public Guid? CodeGuid { get; set; }
        
        /// <summary>
        /// Note or comment for user (optional)
        /// Can contain additional information, instructions or notes
        /// </summary>
        public string? Note { get; set; }
        
        // Hash is NOT added to CreateUserRequest - it is generated automatically!
    }
}
