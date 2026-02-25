using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Core.Services;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Providers
{
    /// <summary>
    /// Provider for saving/loading objects in EAV storage.
    /// Permission checks are managed centrally via configuration.
    /// </summary>
    public interface IObjectStorageProvider
    {
        // ===== BASE METHODS (use _securityContext and configuration) =====
        
        /// <summary>
        /// Load object from EAV by ID (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        /// <param name="lazyLoadProps">If null - taken from config.EnableLazyLoadingForProps, if true - loads only base fields, Props loaded on first access</param>
        Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();
        
        /// <summary>
        /// Load object from EAV (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        /// <param name="lazyLoadProps">If null - taken from config.EnableLazyLoadingForProps, if true - loads only base fields, Props loaded on first access</param>
        Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Load object from EAV with explicit user by ID (uses config.DefaultCheckPermissionsOnLoad).
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        /// <param name="lazyLoadProps">If null - taken from config.EnableLazyLoadingForProps, if true - loads only base fields, Props loaded on first access</param>
        Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Load object from EAV with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        /// <param name="lazyLoadProps">If null - taken from config.EnableLazyLoadingForProps, if true - loads only base fields, Props loaded on first access</param>
        Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Save object to EAV (uses _securityContext and config.DefaultCheckPermissionsOnSave).
        /// Determines type (generic/non-generic) internally.
        /// </summary>
        Task<long> SaveAsync(IRedbObject obj);
        
        /// <summary>
        /// Save generic object to EAV (uses _securityContext and config.DefaultCheckPermissionsOnSave).
        /// </summary>
        Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj) where TProps : class, new();
        
        /// <summary>
        /// Delete object (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        Task<bool> DeleteAsync(IRedbObject obj);
        
        // ===== OVERLOADS WITH EXPLICIT USER (use configuration) =====
        
       
        /// <summary>
        /// Save object to EAV with explicit user. Determines type internally.
        /// </summary>
        Task<long> SaveAsync(IRedbObject obj, IRedbUser user);
        
        /// <summary>
        /// Save generic object to EAV with explicit user (uses config.DefaultCheckPermissionsOnSave).
        /// </summary>
        Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, IRedbUser user) where TProps : class, new();
        
        /// <summary>
        /// Delete object with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        Task<bool> DeleteAsync(IRedbObject obj, IRedbUser user);

        // ===== DELETE BY ID =====
        
        /// <summary>
        /// Delete object by ID (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>true if object deleted, false if not found</returns>
        Task<bool> DeleteAsync(long objectId);
        
        /// <summary>
        /// Delete object by ID with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>true if object deleted, false if not found</returns>
        Task<bool> DeleteAsync(long objectId, IRedbUser user);

        // ===== BULK OPERATIONS WITH PERMISSION CHECK =====
        
        /// <summary>
        /// Bulk delete objects by ID (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>Number of deleted objects</returns>
        Task<int> DeleteAsync(IEnumerable<long> objectIds);
        
        /// <summary>
        /// Bulk delete objects by ID with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>Number of deleted objects</returns>
        Task<int> DeleteAsync(IEnumerable<long> objectIds, IRedbUser user);
        
        /// <summary>
        /// Bulk delete objects by interface (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>Number of deleted objects</returns>
        Task<int> DeleteAsync(IEnumerable<IRedbObject> objects);
        
        /// <summary>
        /// Bulk delete objects by interface with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>Number of deleted objects</returns>
        Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user);
        
        /// <summary>
        /// Bulk polymorphic load of objects by ID (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// Supports objects of different schemes in one request.
        /// </summary>
        /// <param name="objectIds">List of object IDs to load</param>
        /// <param name="depth">Depth for loading nested objects (EAGER mode only)</param>
        /// <param name="lazyLoadProps">If null - taken from config.EnableLazyLoadingForProps (default true), true = LAZY (_objects + LoadPropsForManyAsync), false = EAGER (get_object_json)</param>
        /// <returns>List of polymorphic IRedbObject instances</returns>
        Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, int depth = 10, bool? lazyLoadProps = null);
        
        /// <summary>
        /// Bulk polymorphic load of objects by ID with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// Supports objects of different schemes in one request.
        /// </summary>
        /// <param name="objectIds">List of object IDs to load</param>
        /// <param name="user">User for permission check</param>
        /// <param name="depth">Depth for loading nested objects (EAGER mode only)</param>
        /// <param name="lazyLoadProps">If null - taken from config.EnableLazyLoadingForProps (default true), true = LAZY (_objects + LoadPropsForManyAsync), false = EAGER (get_object_json)</param>
        /// <returns>List of polymorphic IRedbObject instances</returns>
        Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, bool? lazyLoadProps = null);
        
        /// <summary>
        /// Bulk save of polymorphic objects (uses _securityContext and config).
        /// Supports new and existing objects, nested objects.
        /// Uses two strategies: DeleteInsert (bulk operations) or ChangeTracking (EF diff).
        /// </summary>
        /// <returns>List of IDs of all main objects</returns>
        Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects);
        
        /// <summary>
        /// Bulk save of polymorphic objects with explicit user (uses config).
        /// Supports new and existing objects, nested objects.
        /// Uses two strategies: DeleteInsert (bulk operations) or ChangeTracking (EF diff).
        /// </summary>
        /// <returns>List of IDs of all main objects</returns>
        Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, IRedbUser user);
        
        // ===== SOFT DELETE (BACKGROUND DELETION) =====
        
        /// <summary>
        /// Mark objects for soft-deletion (uses _securityContext).
        /// Creates a trash container and moves objects and their descendants under it.
        /// Actual deletion happens in background via IBackgroundDeletionService.
        /// </summary>
        /// <param name="objectIds">IDs of objects to mark for deletion</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        /// <returns>Deletion mark with trash container ID and count of marked objects</returns>
        Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, long? trashParentId = null);
        
        /// <summary>
        /// Mark objects for soft-deletion with explicit user.
        /// Creates a trash container and moves objects and their descendants under it.
        /// Actual deletion happens in background via IBackgroundDeletionService.
        /// </summary>
        /// <param name="objectIds">IDs of objects to mark for deletion</param>
        /// <param name="user">User performing the operation</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        /// <returns>Deletion mark with trash container ID and count of marked objects</returns>
        Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, IRedbUser user, long? trashParentId = null);
        
        /// <summary>
        /// Mark objects for soft-deletion (uses _securityContext).
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        /// <param name="objects">Objects to mark for deletion</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        /// <returns>Deletion mark with trash container ID and count of marked objects</returns>
        Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, long? trashParentId = null);
        
        /// <summary>
        /// Mark objects for soft-deletion with explicit user.
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        /// <param name="objects">Objects to mark for deletion</param>
        /// <param name="user">User performing the operation</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        /// <returns>Deletion mark with trash container ID and count of marked objects</returns>
        Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, long? trashParentId = null);
        
        /// <summary>
        /// Delete objects with background purge and progress reporting.
        /// Marks objects for deletion, then purges them in batches with progress callback.
        /// </summary>
        /// <param name="objectIds">IDs of objects to delete</param>
        /// <param name="batchSize">Number of objects to delete per batch</param>
        /// <param name="progress">Optional progress reporter</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        Task DeleteWithPurgeAsync(
            IEnumerable<long> objectIds, 
            int batchSize = 10,
            IProgress<PurgeProgress>? progress = null,
            CancellationToken cancellationToken = default,
            long? trashParentId = null);
        
        /// <summary>
        /// Purge a trash container created by SoftDeleteAsync.
        /// Physically deletes objects in batches with progress callback.
        /// Call this after SoftDeleteAsync if you want to control purge timing separately.
        /// </summary>
        /// <param name="trashId">Trash container ID from DeletionMark.TrashId</param>
        /// <param name="totalCount">Total objects to delete (from DeletionMark.MarkedCount)</param>
        /// <param name="batchSize">Number of objects to delete per batch</param>
        /// <param name="progress">Optional progress reporter</param>
        /// <param name="cancellationToken">Cancellation token</param>
        Task PurgeTrashAsync(
            long trashId,
            int totalCount,
            int batchSize = 10,
            IProgress<PurgeProgress>? progress = null,
            CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Gets deletion progress for a specific trash container from database.
        /// Returns null if trash container not found or already deleted.
        /// </summary>
        /// <param name="trashId">Trash container ID</param>
        Task<PurgeProgress?> GetDeletionProgressAsync(long trashId);
        
        /// <summary>
        /// Gets all active (pending/running) deletions for a user from database.
        /// </summary>
        /// <param name="userId">User ID</param>
        Task<List<PurgeProgress>> GetUserActiveDeletionsAsync(long userId);
        
        /// <summary>
        /// Gets orphaned deletion tasks for recovery at startup.
        /// CLUSTER-SAFE: Returns 'pending' OR 'running' with stale _date_modify.
        /// </summary>
        /// <param name="timeoutMinutes">Minutes after which 'running' task is considered orphaned</param>
        Task<List<OrphanedTask>> GetOrphanedDeletionTasksAsync(int timeoutMinutes = 30);
        
        /// <summary>
        /// Atomically claim an orphaned task for processing.
        /// CLUSTER-SAFE: Uses atomic UPDATE to prevent race conditions.
        /// </summary>
        /// <param name="trashId">Trash container ID to claim</param>
        /// <param name="timeoutMinutes">Minutes for stale check</param>
        /// <returns>True if successfully claimed, false if already taken by another instance</returns>
        Task<bool> TryClaimOrphanedTaskAsync(long trashId, int timeoutMinutes = 30);

        // ===== BULK OPERATIONS (WITHOUT PERMISSION CHECK) =====
        
        /// <summary>
        /// BULK INSERT: Create many new objects in one operation (does NOT check permissions).
        /// - Creates schemes if missing (similar to SaveAsync)
        /// - Generates IDs for objects with id == 0 via GetNextKey
        /// - Fully processes recursive nested objects, arrays, Class fields
        /// - Uses BulkInsert for maximum performance
        /// - If id != 0, relies on DB errors for duplicates (does not check in advance)
        /// </summary>
        Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects) where TProps : class, new();
        
        /// <summary>
        /// BULK INSERT with explicit user: Create many new objects (does NOT check permissions).
        /// - Sets OwnerId and WhoChangeId for all objects from specified user
        /// - Rest of logic identical to AddNewObjectsAsync without user
        /// </summary>
        Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects, IRedbUser user) where TProps : class, new();
        
        // NOTE: Non-generic RedbObject (Object scheme) uses existing methods:
        // - SaveAsync(IEnumerable<IRedbObject>) - RedbObject : IRedbObject
        // - LoadAsync(IEnumerable<long>) - returns List<IRedbObject>, cast to RedbObject

        // ===== LOAD WITH PARENT CHAIN =====

        /// <summary>
        /// Load object from EAV by ID with parent chain to root (uses _securityContext).
        /// Returns TreeRedbObject with populated Parent property up to root.
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        /// <param name="objectId">Object ID to load</param>
        /// <param name="depth">Depth for loading nested objects in Props</param>
        /// <param name="lazyLoadProps">If null - taken from config.EnableLazyLoadingForProps</param>
        Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(long objectId, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Load object from EAV with parent chain to root (uses _securityContext).
        /// Returns TreeRedbObject with populated Parent property up to root.
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(IRedbObject obj, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Load object from EAV by ID with parent chain to root with explicit user.
        /// Returns TreeRedbObject with populated Parent property up to root.
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(long objectId, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Load object from EAV with parent chain to root with explicit user.
        /// Returns TreeRedbObject with populated Parent property up to root.
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Bulk load objects by ID with parent chains to root (uses _securityContext).
        /// Each object has its Parent chain populated up to root.
        /// Parent objects that are common across multiple chains are shared (same reference).
        /// </summary>
        /// <param name="objectIds">Object IDs to load</param>
        /// <param name="depth">Depth for loading nested objects in Props</param>
        /// <param name="lazyLoadProps">If null - taken from config.EnableLazyLoadingForProps</param>
        Task<List<TreeRedbObject<TProps>>> LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Bulk load objects by ID with parent chains to root with explicit user.
        /// Each object has its Parent chain populated up to root.
        /// Parent objects that are common across multiple chains are shared (same reference).
        /// </summary>
        Task<List<TreeRedbObject<TProps>>> LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, bool? lazyLoadProps = null) where TProps : class, new();

        /// <summary>
        /// Bulk polymorphic load of objects by ID with parent chains (uses _securityContext).
        /// Supports objects of different schemes in one request.
        /// Each object has its Parent chain populated up to root.
        /// </summary>
        Task<List<ITreeRedbObject>> LoadWithParentsAsync(IEnumerable<long> objectIds, int depth = 10, bool? lazyLoadProps = null);

        /// <summary>
        /// Bulk polymorphic load of objects by ID with parent chains with explicit user.
        /// Supports objects of different schemes in one request.
        /// Each object has its Parent chain populated up to root.
        /// </summary>
        Task<List<ITreeRedbObject>> LoadWithParentsAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, bool? lazyLoadProps = null);
    }
}
