namespace redb.Core.Models.Permissions
{
    /// <summary>
    /// Request for creating or updating permission
    /// </summary>
    public class PermissionRequest
    {
        /// <summary>
        /// User ID (null if permission is for role)
        /// </summary>
        public long? UserId { get; set; }
        
        /// <summary>
        /// Role ID (null if permission is for user)
        /// </summary>
        public long? RoleId { get; set; }
        
        /// <summary>
        /// Object ID (0 for global permissions)
        /// </summary>
        public long ObjectId { get; set; }
        
        /// <summary>
        /// Read permission
        /// </summary>
        public bool? CanSelect { get; set; }
        
        /// <summary>
        /// Permission to create child objects
        /// </summary>
        public bool? CanInsert { get; set; }
        
        /// <summary>
        /// Edit permission
        /// </summary>
        public bool? CanUpdate { get; set; }
        
        /// <summary>
        /// Delete permission
        /// </summary>
        public bool? CanDelete { get; set; }
        
        /// <summary>
        /// Validate request
        /// </summary>
        public bool IsValid()
        {
            // Either user or role must be specified, but not both
            if (UserId.HasValue && RoleId.HasValue)
                return false;
                
            if (!UserId.HasValue && !RoleId.HasValue)
                return false;
                
            // At least one permission must be specified
            if (!CanSelect.HasValue && !CanInsert.HasValue && !CanUpdate.HasValue && !CanDelete.HasValue)
                return false;
                
            return true;
        }
    }
}
