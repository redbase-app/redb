using System;

namespace redb.Core.Models.Permissions
{
    // Model for VIEW v_user_permissions (keyless)
    public class VUserPermission
    {
        public long ObjectId { get; set; }
        public long UserId { get; set; }
        public long PermissionId { get; set; }
        public string PermissionType { get; set; } = string.Empty; // 'user' | 'role'
        public long? IdRole { get; set; }
        public bool CanSelect { get; set; }
        public bool CanInsert { get; set; }
        public bool CanUpdate { get; set; }
        public bool CanDelete { get; set; }
    }
}
