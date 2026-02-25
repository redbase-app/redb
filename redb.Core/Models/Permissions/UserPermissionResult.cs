using System;
using System.ComponentModel.DataAnnotations.Schema;

namespace redb.Core.Models.Permissions
{
    // Result of get_user_permissions_for_object(object_id, user_id) function
    public class UserPermissionResult
    {
        [Column("object_id")]
        public long ObjectId { get; set; }
        
        [Column("user_id")]
        public long? UserId { get; set; } // Can be NULL when called with user_id = NULL
        
        [Column("permission_source_id")]
        public long PermissionSourceId { get; set; }
        
        [Column("permission_type")]
        public string PermissionType { get; set; } = string.Empty; // 'user' | 'role'
        
        [Column("_id_role")]
        public long? IdRole { get; set; }
        
        [Column("_id_user")]
        public long? IdUser { get; set; }
        
        [Column("can_select")]
        public bool CanSelect { get; set; }
        
        [Column("can_insert")]
        public bool CanInsert { get; set; }
        
        [Column("can_update")]
        public bool CanUpdate { get; set; }
        
        [Column("can_delete")]
        public bool CanDelete { get; set; }
    }
}
