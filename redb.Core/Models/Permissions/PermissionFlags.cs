using System;

namespace redb.Core.Models.Permissions
{
    /// <summary>
    /// Permission flags for REDB objects
    /// Correspond to _select, _insert, _update, _delete fields in _permissions table
    /// </summary>
    [Flags]
    public enum PermissionFlags
    {
        /// <summary>
        /// No permissions
        /// </summary>
        None = 0,
        
        /// <summary>
        /// Read permission (SELECT)
        /// </summary>
        Select = 1,
        
        /// <summary>
        /// Create permission (INSERT)
        /// </summary>
        Insert = 2,
        
        /// <summary>
        /// Update permission (UPDATE)
        /// </summary>
        Update = 4,
        
        /// <summary>
        /// Delete permission (DELETE)
        /// </summary>
        Delete = 8,
        
        /// <summary>
        /// All permissions (RIUD)
        /// </summary>
        All = Select | Insert | Update | Delete,
        
        /// <summary>
        /// Read and write (RI_U)
        /// </summary>
        ReadWrite = Select | Insert | Update,
        
        /// <summary>
        /// Read only (R)
        /// </summary>
        ReadOnly = Select
    }
    
    /// <summary>
    /// Extensions for working with PermissionFlags
    /// </summary>
    public static class PermissionFlagsExtensions
    {
        /// <summary>
        /// Check if has read permission
        /// </summary>
        public static bool CanSelect(this PermissionFlags flags) => flags.HasFlag(PermissionFlags.Select);
        
        /// <summary>
        /// Check if has create permission
        /// </summary>
        public static bool CanInsert(this PermissionFlags flags) => flags.HasFlag(PermissionFlags.Insert);
        
        /// <summary>
        /// Check if has update permission
        /// </summary>
        public static bool CanUpdate(this PermissionFlags flags) => flags.HasFlag(PermissionFlags.Update);
        
        /// <summary>
        /// Check if has delete permission
        /// </summary>
        public static bool CanDelete(this PermissionFlags flags) => flags.HasFlag(PermissionFlags.Delete);
        
        /// <summary>
        /// Convert to display string (e.g. "RIUD")
        /// </summary>
        public static string ToDisplayString(this PermissionFlags flags)
        {
            var result = "";
            if (flags.CanSelect()) result += "R";
            if (flags.CanInsert()) result += "I";
            if (flags.CanUpdate()) result += "U";
            if (flags.CanDelete()) result += "D";
            return string.IsNullOrEmpty(result) ? "----" : result;
        }
        
        /// <summary>
        /// Create PermissionFlags from boolean values (as in DB)
        /// </summary>
        public static PermissionFlags FromBooleans(bool select, bool insert, bool update, bool delete)
        {
            var flags = PermissionFlags.None;
            if (select) flags |= PermissionFlags.Select;
            if (insert) flags |= PermissionFlags.Insert;
            if (update) flags |= PermissionFlags.Update;
            if (delete) flags |= PermissionFlags.Delete;
            return flags;
        }
    }
}
