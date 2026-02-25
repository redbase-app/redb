using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for tree structure operations.
    /// Permission checks managed centrally via configuration (similar to IObjectStorageProvider).
    /// </summary>
    public interface ITreeProvider
    {
        // ===== BASE METHODS (use _securityContext and configuration) =====
        
        /// <summary>
        /// Load tree/subtree by root object ID (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// Loads root object and all its children recursively.
        /// </summary>
        Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Load tree/subtree (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Get direct children of object (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj) where TProps : class, new();
        
        /// <summary>
        /// Get path from object to root (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj) where TProps : class, new();
        
        /// <summary>
        /// Get all object descendants (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Move object in tree (uses _securityContext and config.DefaultCheckPermissionsOnSave).
        /// </summary>
        Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj);
        
        /// <summary>
        /// Create child object (uses _securityContext and config.DefaultCheckPermissionsOnSave).
        /// </summary>
        Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj) where TProps : class, new();
        
        /// <summary>
        /// Delete object subtree recursively (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        Task<int> DeleteSubtreeAsync(IRedbObject parentObj);

        // ===== OVERLOADS WITH EXPLICIT USER (use configuration) =====
        
        /// <summary>
        /// Load tree/subtree by root object ID with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(long rootObjectId, IRedbUser user, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Load tree/subtree with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<TreeRedbObject<TProps>> LoadTreeAsync<TProps>(IRedbObject rootObj, IRedbUser user, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Get direct children of object with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<IEnumerable<TreeRedbObject<TProps>>> GetChildrenAsync<TProps>(IRedbObject parentObj, IRedbUser user) where TProps : class, new();
        
        /// <summary>
        /// Get path from object to root with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<IEnumerable<TreeRedbObject<TProps>>> GetPathToRootAsync<TProps>(IRedbObject obj, IRedbUser user) where TProps : class, new();
        
        /// <summary>
        /// Get all object descendants with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// </summary>
        Task<IEnumerable<TreeRedbObject<TProps>>> GetDescendantsAsync<TProps>(IRedbObject parentObj, IRedbUser user, int? maxDepth = null) where TProps : class, new();
        
        /// <summary>
        /// Move object in tree with explicit user (uses config.DefaultCheckPermissionsOnSave).
        /// </summary>
        Task MoveObjectAsync(IRedbObject obj, IRedbObject? newParentObj, IRedbUser user);
        
        /// <summary>
        /// Create child object with explicit user (uses config.DefaultCheckPermissionsOnSave).
        /// </summary>
        Task<long> CreateChildAsync<TProps>(TreeRedbObject<TProps> obj, IRedbObject parentObj, IRedbUser user) where TProps : class, new();
        
        /// <summary>
        /// Delete object subtree recursively with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        Task<int> DeleteSubtreeAsync(IRedbObject parentObj, IRedbUser user);

        // ===== POLYMORPHIC METHODS (for mixed trees) =====
        
        /// <summary>
        /// Load polymorphic tree/subtree - supports objects of different schemes in one tree.
        /// Uses _securityContext and config.DefaultCheckPermissionsOnLoad.
        /// </summary>
        Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, int? maxDepth = null);
        
        /// <summary>
        /// Get all direct children of object regardless of their schemes.
        /// Uses _securityContext and config.DefaultCheckPermissionsOnLoad.
        /// </summary>
        Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj);
        
        /// <summary>
        /// Get polymorphic path from object to root - objects can be of different schemes.
        /// Uses _securityContext and config.DefaultCheckPermissionsOnLoad.
        /// </summary>
        Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj);
        
        /// <summary>
        /// Get all polymorphic descendants of object regardless of their schemes.
        /// Uses _securityContext and config.DefaultCheckPermissionsOnLoad.
        /// </summary>
        Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, int? maxDepth = null);
        


        // ===== POLYMORPHIC METHODS WITH EXPLICIT USER =====
        
        /// <summary>
        /// Load polymorphic tree/subtree with explicit user.
        /// Uses config.DefaultCheckPermissionsOnLoad.
        /// </summary>
        Task<ITreeRedbObject> LoadPolymorphicTreeAsync(IRedbObject rootObj, IRedbUser user, int? maxDepth = null);
        
        /// <summary>
        /// Get all direct children of object regardless of their schemes with explicit user.
        /// Uses config.DefaultCheckPermissionsOnLoad.
        /// </summary>
        Task<IEnumerable<ITreeRedbObject>> GetPolymorphicChildrenAsync(IRedbObject parentObj, IRedbUser user);
        
        /// <summary>
        /// Get polymorphic path from object to root with explicit user.
        /// Uses config.DefaultCheckPermissionsOnLoad.
        /// </summary>
        Task<IEnumerable<ITreeRedbObject>> GetPolymorphicPathToRootAsync(IRedbObject obj, IRedbUser user);
        
        /// <summary>
        /// Get all polymorphic descendants of object with explicit user.
        /// Uses config.DefaultCheckPermissionsOnLoad.
        /// </summary>
        Task<IEnumerable<ITreeRedbObject>> GetPolymorphicDescendantsAsync(IRedbObject parentObj, IRedbUser user, int? maxDepth = null);
        
        // ===== POLYMORPHISM INITIALIZATION =====
        
        /// <summary>
        /// Initialize AutomaticTypeRegistry for polymorphic operation support.
        /// Should be called at application startup.
        /// </summary>
        Task InitializeTypeRegistryAsync();

    }
}
