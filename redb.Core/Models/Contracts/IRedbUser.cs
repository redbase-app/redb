using System;

namespace redb.Core.Models.Contracts
{
    /// <summary>
    /// REDB user interface
    /// Represents system user with his basic properties
    /// </summary>
    public interface IRedbUser
    {
        /// <summary>
        /// Unique user identifier
        /// </summary>
        long Id { get; }
        
        /// <summary>
        /// User login (unique)
        /// </summary>
        string Login { get; }
        
        /// <summary>
        /// User name
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// User password (hashed)
        /// </summary>
        string Password { get; }
        
        /// <summary>
        /// Is user active
        /// </summary>
        bool Enabled { get; }
        
        /// <summary>
        /// User registration date
        /// </summary>
        DateTimeOffset DateRegister { get; }
        
        /// <summary>
        /// Dismissal date (if null - user is active)
        /// </summary>
        DateTimeOffset? DateDismiss { get; }
        
        /// <summary>
        /// User phone (optional)
        /// </summary>
        string? Phone { get; }
        
        /// <summary>
        /// User email (optional)
        /// </summary>
        string? Email { get; }
        
        /// <summary>
        /// Additional user key (optional)
        /// Can be used for external integrations or additional identification
        /// </summary>
        long? Key { get; }
        
        /// <summary>
        /// User integer code (optional)
        /// Can be used for categorization, access groups or external systems
        /// </summary>
        long? CodeInt { get; }
        
        /// <summary>
        /// User string code (optional)
        /// Can be used for department codes, branches or special labels
        /// </summary>
        string? CodeString { get; }
        
        /// <summary>
        /// User GUID code (optional)
        /// Can be used for unique identification in distributed systems
        /// </summary>
        Guid? CodeGuid { get; }
        
        /// <summary>
        /// User note or comment (optional)
        /// May contain additional information, instructions or notes
        /// </summary>
        string? Note { get; }
        
        /// <summary>
        /// User hash for data integrity check (optional)
        /// Generated automatically based on user basic properties
        /// </summary>
        Guid? Hash { get; }
    }
}
