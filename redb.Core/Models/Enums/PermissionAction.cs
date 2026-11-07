using System;

namespace redb.Core.Models.Enums
{
    /// <summary>
    /// Permission actions (flags for combining)
    /// </summary>
    [Flags]
    public enum PermissionAction
    {
        /// <summary>
        /// No permissions
        /// </summary>
        None = 0,
        
        /// <summary>
        /// Read permission
        /// </summary>
        Select = 1,
        
        /// <summary>
        /// Permission to create child objects
        /// </summary>
        Insert = 2,
        
        /// <summary>
        /// Edit permission
        /// </summary>
        Update = 4,
        
        /// <summary>
        /// Delete permission
        /// </summary>
        Delete = 8,
        
        /// <summary>
        /// All permissions
        /// </summary>
        All = Select | Insert | Update | Delete,
        
        /// <summary>
        /// Read and edit permissions
        /// </summary>
        ReadWrite = Select | Update,
        
        /// <summary>
        /// Read and create permissions
        /// </summary>
        ReadCreate = Select | Insert
    }
}
