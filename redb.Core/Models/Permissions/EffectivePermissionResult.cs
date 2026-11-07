namespace redb.Core.Models.Permissions
{
    /// <summary>
    /// Result of getting user's effective permissions on object
    /// </summary>
    public class EffectivePermissionResult
    {
        /// <summary>
        /// Object ID
        /// </summary>
        public long ObjectId { get; set; }
        
        /// <summary>
        /// User ID
        /// </summary>
        public long UserId { get; set; }
        
        /// <summary>
        /// Permission source ID (object from which permission is inherited)
        /// </summary>
        public long PermissionSourceId { get; set; }
        
        /// <summary>
        /// Permission type (user or role-based)
        /// </summary>
        public string PermissionType { get; set; } = "";
        
        /// <summary>
        /// Role ID (if permission is role-based)
        /// </summary>
        public long? RoleId { get; set; }
        
        /// <summary>
        /// User ID in permission (if permission is user-based)
        /// </summary>
        public long? PermissionUserId { get; set; }
        
        /// <summary>
        /// Read permission
        /// </summary>
        public bool CanSelect { get; set; }
        
        /// <summary>
        /// Permission to create child objects
        /// </summary>
        public bool CanInsert { get; set; }
        
        /// <summary>
        /// Edit permission
        /// </summary>
        public bool CanUpdate { get; set; }
        
        /// <summary>
        /// Delete permission
        /// </summary>
        public bool CanDelete { get; set; }
        
        /// <summary>
        /// Permission is inherited from parent object
        /// </summary>
        public bool IsInherited => PermissionSourceId != ObjectId;
        
        /// <summary>
        /// Has any permissions
        /// </summary>
        public bool HasAnyPermission => CanSelect || CanInsert || CanUpdate || CanDelete;
        
        /// <summary>
        /// Has full permissions (all actions allowed)
        /// </summary>
        public bool HasFullPermission => CanSelect && CanInsert && CanUpdate && CanDelete;
    }
}
