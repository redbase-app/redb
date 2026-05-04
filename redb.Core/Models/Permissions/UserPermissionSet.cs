using System;
using System.Collections.Generic;

namespace redb.Core.Models.Permissions
{
    /// <summary>
    /// User permissions set for caching
    /// Contains all user permissions for quick access
    /// </summary>
    public class UserPermissionSet
    {
        /// <summary>
        /// User ID
        /// </summary>
        public long UserId { get; set; }
        
        /// <summary>
        /// Permission set creation time
        /// </summary>
        public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
        
        /// <summary>
        /// Cache expiration time
        /// </summary>
        public DateTimeOffset ExpiresAt { get; set; } = DateTimeOffset.UtcNow.AddMinutes(30);
        
        /// <summary>
        /// Permissions on specific objects
        /// Key: ObjectId, Value: PermissionFlags
        /// </summary>
        public Dictionary<long, PermissionFlags> ObjectPermissions { get; set; } = new();
        
        /// <summary>
        /// Global permissions (for all objects)
        /// </summary>
        public PermissionFlags GlobalPermissions { get; set; } = PermissionFlags.None;
        
        /// <summary>
        /// Permissions on schemes
        /// Key: SchemeId, Value: PermissionFlags
        /// </summary>
        public Dictionary<long, PermissionFlags> SchemePermissions { get; set; } = new();
        
        /// <summary>
        /// Cache version (for invalidation)
        /// </summary>
        public long Version { get; set; } = 1;
        
        /// <summary>
        /// Check if cache is expired
        /// </summary>
        public bool IsExpired => DateTimeOffset.UtcNow > ExpiresAt;
        
        /// <summary>
        /// Get permissions for object considering hierarchy
        /// </summary>
        public PermissionFlags GetPermissionsForObject(long objectId, long schemeId)
        {
            // 1. Check specific permissions on object
            if (ObjectPermissions.TryGetValue(objectId, out var objectPerms) && objectPerms != PermissionFlags.None)
            {
                return objectPerms;
            }
            
            // 2. Check permissions on scheme
            if (SchemePermissions.TryGetValue(schemeId, out var schemePerms) && schemePerms != PermissionFlags.None)
            {
                return schemePerms;
            }
            
            // 3. Return global permissions
            return GlobalPermissions;
        }
        
        /// <summary>
        /// Add permission on object
        /// </summary>
        public void AddObjectPermission(long objectId, PermissionFlags permissions)
        {
            ObjectPermissions[objectId] = permissions;
        }
        
        /// <summary>
        /// Add permission on scheme
        /// </summary>
        public void AddSchemePermission(long schemeId, PermissionFlags permissions)
        {
            SchemePermissions[schemeId] = permissions;
        }
        
        /// <summary>
        /// Set global permissions
        /// </summary>
        public void SetGlobalPermissions(PermissionFlags permissions)
        {
            GlobalPermissions = permissions;
        }
        
        /// <summary>
        /// Check if user can perform operation on object
        /// </summary>
        public bool CanPerformOperation(long objectId, long schemeId, PermissionFlags requiredPermission)
        {
            var userPermissions = GetPermissionsForObject(objectId, schemeId);
            return userPermissions.HasFlag(requiredPermission);
        }
        
        /// <summary>
        /// Invalidate cache (set as expired)
        /// </summary>
        public void Invalidate()
        {
            ExpiresAt = DateTimeOffset.UtcNow.AddSeconds(-1);
        }
        
        /// <summary>
        /// Extend cache lifetime
        /// </summary>
        public void ExtendExpiration(TimeSpan extension)
        {
            ExpiresAt = DateTimeOffset.UtcNow.Add(extension);
        }
        
        /// <summary>
        /// Get cache statistics
        /// </summary>
        public CacheStatistics GetStatistics()
        {
            return new CacheStatistics
            {
                UserId = UserId,
                ObjectPermissionsCount = ObjectPermissions.Count,
                SchemePermissionsCount = SchemePermissions.Count,
                HasGlobalPermissions = GlobalPermissions != PermissionFlags.None,
                CreatedAt = CreatedAt,
                ExpiresAt = ExpiresAt,
                IsExpired = IsExpired,
                Version = Version
            };
        }
    }
    
    /// <summary>
    /// Permissions cache statistics
    /// </summary>
    public class CacheStatistics
    {
        public long UserId { get; set; }
        public int ObjectPermissionsCount { get; set; }
        public int SchemePermissionsCount { get; set; }
        public bool HasGlobalPermissions { get; set; }
        public DateTimeOffset CreatedAt { get; set; }
        public DateTimeOffset ExpiresAt { get; set; }
        public bool IsExpired { get; set; }
        public long Version { get; set; }
        
        public override string ToString()
        {
            return $"User {UserId}: {ObjectPermissionsCount} objects, {SchemePermissionsCount} schemes, " +
                   $"Global: {HasGlobalPermissions}, Expired: {IsExpired}, Version: {Version}";
        }
    }
}
