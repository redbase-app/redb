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
        Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Synchronous <see cref="LoadAsync{TProps}(long, int, CancellationToken)"/> for the
        /// thread-pool-free lazy path (the sync getter of RedbListItem.Object): the whole load runs
        /// on the calling thread down to ADO.NET, so a saturated thread pool cannot slow or
        /// deadlock it. The default falls back to blocking on the async form (pool-coupled);
        /// the in-tree base provider overrides with a true sync path.
        /// </summary>
        RedbObject<TProps>? Load<TProps>(long objectId, int depth = 10) where TProps : class, new()
            => LoadAsync<TProps>(objectId, depth).ConfigureAwait(false).GetAwaiter().GetResult();

        /// <summary>
        /// Load object from EAV (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Load object from EAV with explicit user by ID (uses config.DefaultCheckPermissionsOnLoad).
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<RedbObject<TProps>?> LoadAsync<TProps>(long objectId, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Load object from EAV with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<RedbObject<TProps>?> LoadAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Load the whole object as raw JSON by ID, without a CLR props type — the in-database
        /// materializer (get_object_json on every provider) serializes the full RedbObject tree
        /// to the given depth. Permission check per config.DefaultCheckPermissionsOnLoad; the
        /// typed props cache is not consulted (it is keyed by CLR type).
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<string?> LoadJsonAsync(long objectId, int depth = 10, CancellationToken cancellationToken = default);

        /// <summary>
        /// Load the whole object as raw JSON by ID with explicit user (see
        /// <see cref="LoadJsonAsync(long, int)"/>).
        /// </summary>
        Task<string?> LoadJsonAsync(long objectId, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default);

        /// <summary>
        /// The object whose [RedbUnique] property holds the given value, or null. One probe of the
        /// partial unique index: the value is canonicalised and hashed exactly as the save path
        /// does it. Throws RedbUniqueKeyDefinitionException when the property is not a unique key.
        /// </summary>
        Task<RedbObject<TProps>?> GetByUniqueAsync<TProps>(System.Linq.Expressions.Expression<Func<TProps, object?>> keyProperty, object? value, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <inheritdoc cref="GetByUniqueAsync{TProps}(System.Linq.Expressions.Expression{Func{TProps, object?}}, object?, int, CancellationToken)"/>
        Task<RedbObject<TProps>?> GetByUniqueAsync<TProps>(string propertyName, object? value, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Upsert by the object key (P1): resolves the _objects row by (scheme, ValueUnique) and
        /// saves onto it - the values ride the ordinary pipeline in the same save. A lost creation
        /// race retries onto the winner's row, so a concurrent accept of one key is idempotent:
        /// exactly one object per key, last writer's content. Returns the object id.
        /// Throws RedbUniqueKeyDefinitionException when ValueUnique is empty.
        /// </summary>
        Task<long> SaveByUniqueAsync<TProps>(RedbObject<TProps> obj, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Save object to EAV (uses _securityContext and config.DefaultCheckPermissionsOnSave).
        /// Determines type (generic/non-generic) internally.
        /// </summary>
        Task<long> SaveAsync(IRedbObject obj, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Save generic object to EAV (uses _securityContext and config.DefaultCheckPermissionsOnSave).
        /// </summary>
        Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, CancellationToken cancellationToken = default) where TProps : class, new();
        
        /// <summary>
        /// Delete object (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        Task<bool> DeleteAsync(IRedbObject obj, CancellationToken cancellationToken = default);
        
        // ===== OVERLOADS WITH EXPLICIT USER (use configuration) =====
        
       
        /// <summary>
        /// Save object to EAV with explicit user. Determines type internally.
        /// </summary>
        Task<long> SaveAsync(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Save generic object to EAV with explicit user (uses config.DefaultCheckPermissionsOnSave).
        /// </summary>
        Task<long> SaveAsync<TProps>(IRedbObject<TProps> obj, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new();
        
        /// <summary>
        /// Delete object with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        Task<bool> DeleteAsync(IRedbObject obj, IRedbUser user, CancellationToken cancellationToken = default);

        // ===== DELETE BY ID =====
        
        /// <summary>
        /// Delete object by ID (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>true if object deleted, false if not found</returns>
        Task<bool> DeleteAsync(long objectId, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Delete object by ID with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>true if object deleted, false if not found</returns>
        Task<bool> DeleteAsync(long objectId, IRedbUser user, CancellationToken cancellationToken = default);

        // ===== BULK OPERATIONS WITH PERMISSION CHECK =====
        
        /// <summary>
        /// Bulk delete objects by ID (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>Number of deleted objects</returns>
        Task<int> DeleteAsync(IEnumerable<long> objectIds, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk delete objects by ID with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>Number of deleted objects</returns>
        Task<int> DeleteAsync(IEnumerable<long> objectIds, IRedbUser user, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk delete objects by interface (uses _securityContext and config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>Number of deleted objects</returns>
        Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk delete objects by interface with explicit user (uses config.DefaultCheckPermissionsOnDelete).
        /// </summary>
        /// <returns>Number of deleted objects</returns>
        Task<int> DeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk polymorphic load of objects by ID (uses _securityContext and config.DefaultCheckPermissionsOnLoad).
        /// Supports objects of different schemes in one request.
        /// </summary>
        /// <param name="objectIds">List of object IDs to load</param>
        /// <param name="depth">Depth for loading nested objects (EAGER mode only)</param>
        /// <returns>List of polymorphic IRedbObject instances</returns>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, int depth = 10, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk polymorphic load of objects by ID with explicit user (uses config.DefaultCheckPermissionsOnLoad).
        /// Supports objects of different schemes in one request.
        /// </summary>
        /// <param name="objectIds">List of object IDs to load</param>
        /// <param name="user">User for permission check</param>
        /// <param name="depth">Depth for loading nested objects (EAGER mode only)</param>
        /// <returns>List of polymorphic IRedbObject instances</returns>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<List<IRedbObject>> LoadAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk save of polymorphic objects (uses _securityContext and config).
        /// Supports new and existing objects, nested objects.
        /// Uses two strategies: DeleteInsert (bulk operations) or ChangeTracking (EF diff).
        /// </summary>
        /// <returns>List of IDs of all main objects</returns>
        Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Bulk save of polymorphic objects with explicit user (uses config).
        /// Supports new and existing objects, nested objects.
        /// Uses two strategies: DeleteInsert (bulk operations) or ChangeTracking (EF diff).
        /// </summary>
        /// <returns>List of IDs of all main objects</returns>
        Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, IRedbUser user, CancellationToken cancellationToken = default);
        
        // ===== SOFT DELETE (BACKGROUND DELETION) =====
        
        /// <summary>
        /// Mark objects for soft-deletion (uses _securityContext).
        /// Creates a trash container and moves objects and their descendants under it.
        /// Actual deletion happens in background via IBackgroundDeletionService.
        /// </summary>
        /// <param name="objectIds">IDs of objects to mark for deletion</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        /// <returns>Deletion mark with trash container ID and count of marked objects</returns>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, long? trashParentId = null, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Mark objects for soft-deletion with explicit user.
        /// Creates a trash container and moves objects and their descendants under it.
        /// Actual deletion happens in background via IBackgroundDeletionService.
        /// </summary>
        /// <param name="objectIds">IDs of objects to mark for deletion</param>
        /// <param name="user">User performing the operation</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        /// <returns>Deletion mark with trash container ID and count of marked objects</returns>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<DeletionMark> SoftDeleteAsync(IEnumerable<long> objectIds, IRedbUser user, long? trashParentId = null, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Mark objects for soft-deletion (uses _securityContext).
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        /// <param name="objects">Objects to mark for deletion</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        /// <returns>Deletion mark with trash container ID and count of marked objects</returns>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, long? trashParentId = null, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Mark objects for soft-deletion with explicit user.
        /// Creates a trash container and moves objects and their descendants under it.
        /// </summary>
        /// <param name="objects">Objects to mark for deletion</param>
        /// <param name="user">User performing the operation</param>
        /// <param name="trashParentId">Optional parent ID for trash container (null = root level)</param>
        /// <returns>Deletion mark with trash container ID and count of marked objects</returns>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<DeletionMark> SoftDeleteAsync(IEnumerable<IRedbObject> objects, IRedbUser user, long? trashParentId = null, CancellationToken cancellationToken = default);
        
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
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<PurgeProgress?> GetDeletionProgressAsync(long trashId, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Gets all active (pending/running) deletions for a user from database.
        /// </summary>
        /// <param name="userId">User ID</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<List<PurgeProgress>> GetUserActiveDeletionsAsync(long userId, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Gets orphaned deletion tasks for recovery at startup.
        /// CLUSTER-SAFE: Returns 'pending' OR 'running' with stale _date_modify.
        /// </summary>
        /// <param name="timeoutMinutes">Minutes after which 'running' task is considered orphaned</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<List<OrphanedTask>> GetOrphanedDeletionTasksAsync(int timeoutMinutes = 30, CancellationToken cancellationToken = default);
        
        /// <summary>
        /// Atomically claim an orphaned task for processing.
        /// CLUSTER-SAFE: Uses atomic UPDATE to prevent race conditions.
        /// </summary>
        /// <param name="trashId">Trash container ID to claim</param>
        /// <param name="timeoutMinutes">Minutes for stale check</param>
        /// <returns>True if successfully claimed, false if already taken by another instance</returns>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<bool> TryClaimOrphanedTaskAsync(long trashId, int timeoutMinutes = 30, CancellationToken cancellationToken = default);

        // ===== BULK OPERATIONS (WITHOUT PERMISSION CHECK) =====
        
        /// <summary>
        /// BULK INSERT: Create many new objects in one operation (does NOT check permissions).
        /// - Creates schemes if missing (similar to SaveAsync)
        /// - Generates IDs for objects with id == 0 via GetNextKey
        /// - Fully processes recursive nested objects, arrays, Class fields
        /// - Uses BulkInsert for maximum performance
        /// - If id != 0, relies on DB errors for duplicates (does not check in advance)
        /// </summary>
        Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new();
        
        /// <summary>
        /// BULK INSERT with explicit user: Create many new objects (does NOT check permissions).
        /// - Sets OwnerId and WhoChangeId for all objects from specified user
        /// - Rest of logic identical to AddNewObjectsAsync without user
        /// </summary>
        Task<List<long>> AddNewObjectsAsync<TProps>(IEnumerable<IRedbObject<TProps>> objects, IRedbUser user, CancellationToken cancellationToken = default) where TProps : class, new();
        
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
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(long objectId, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Load object from EAV with parent chain to root (uses _securityContext).
        /// Returns TreeRedbObject with populated Parent property up to root.
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(IRedbObject obj, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Load object from EAV by ID with parent chain to root with explicit user.
        /// Returns TreeRedbObject with populated Parent property up to root.
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(long objectId, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Load object from EAV with parent chain to root with explicit user.
        /// Returns TreeRedbObject with populated Parent property up to root.
        /// Returns null if object not found and config.ThrowOnObjectNotFound = false.
        /// </summary>
        Task<TreeRedbObject<TProps>?> LoadWithParentsAsync<TProps>(IRedbObject obj, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Bulk load objects by ID with parent chains to root (uses _securityContext).
        /// Each object has its Parent chain populated up to root.
        /// Parent objects that are common across multiple chains are shared (same reference).
        /// </summary>
        /// <param name="objectIds">Object IDs to load</param>
        /// <param name="depth">Depth for loading nested objects in Props</param>
        /// <param name="cancellationToken">Cancels the operation; see the cancellation contract in the plan (OCE only, rollback always completes).</param>
        Task<List<TreeRedbObject<TProps>>> LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Bulk load objects by ID with parent chains to root with explicit user.
        /// Each object has its Parent chain populated up to root.
        /// Parent objects that are common across multiple chains are shared (same reference).
        /// </summary>
        Task<List<TreeRedbObject<TProps>>> LoadWithParentsAsync<TProps>(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default) where TProps : class, new();

        /// <summary>
        /// Bulk polymorphic load of objects by ID with parent chains (uses _securityContext).
        /// Supports objects of different schemes in one request.
        /// Each object has its Parent chain populated up to root.
        /// </summary>
        Task<List<ITreeRedbObject>> LoadWithParentsAsync(IEnumerable<long> objectIds, int depth = 10, CancellationToken cancellationToken = default);

        /// <summary>
        /// Bulk polymorphic load of objects by ID with parent chains with explicit user.
        /// Supports objects of different schemes in one request.
        /// Each object has its Parent chain populated up to root.
        /// </summary>
        Task<List<ITreeRedbObject>> LoadWithParentsAsync(IEnumerable<long> objectIds, IRedbUser user, int depth = 10, CancellationToken cancellationToken = default);

        // ===== LAZY REFERENCE RELOAD (V4, LAZY Л2 §4.6) =====

        /// <summary>
        /// Loads a COLLECTION of reference stubs under a loaded parent in ONE batch query - the
        /// antidote to N+1 on lazy collections. Already-loaded references are skipped; a stub whose
        /// target left for the trash gets null Props, not an exception.
        /// </summary>
        Task LoadReferencesAsync<TProps, TRef>(RedbObject<TProps> parent,
            Func<TProps, IEnumerable<RedbObject<TRef>?>?> references,
            CancellationToken cancellationToken = default)
            where TProps : class, new() where TRef : class, new();

        /// <summary>Single-reference form of <see cref="LoadReferencesAsync{TProps,TRef}(RedbObject{TProps},Func{TProps,System.Collections.Generic.IEnumerable{RedbObject{TRef}}},CancellationToken)"/>.</summary>
        Task LoadReferencesAsync<TProps, TRef>(RedbObject<TProps> parent,
            Func<TProps, RedbObject<TRef>?> reference,
            CancellationToken cancellationToken = default)
            where TProps : class, new() where TRef : class, new();

        // ===== ROW-LEVEL LOCKING =====

        /// <summary>
        /// Acquire row-level locks on objects by id within the current transaction and report how
        /// many of the requested rows exist (and are therefore locked). A deleted or never-created
        /// id locks nothing and is NOT an error here - the returned count is the signal; compare it
        /// with your distinct id set (BR-9, 2026-09-02: a silent no-op lock plus the default
        /// MissingObjectStrategy.AutoSwitchToInsert let a CAS save resurrect a deleted object).
        /// Must be called inside an active transaction (<see cref="IRedbContext.BeginTransactionAsync"/>
        /// or <see cref="IRedbContext.ExecuteAtomicAsync(Func{Task})"/>): locks release when the
        /// transaction ends. Rows are locked in ascending id order, so concurrent lockers do not
        /// deadlock on each other. PostgreSQL: SELECT ... FOR UPDATE. MSSQL: WITH (UPDLOCK, ROWLOCK).
        /// SQLite: an existence check only - writers are serialized database-wide.
        /// </summary>
        /// <param name="objectIds">IDs of objects to lock</param>
        /// <returns>How many distinct requested ids exist in _objects (those rows are locked).</returns>
        Task<int> LockForUpdateAsync(params long[] objectIds);

        /// <inheritdoc cref="LockForUpdateAsync(long[])"/>
        /// <remarks>Paired overload: params is incompatible with a trailing token (B1 amendment).</remarks>
        Task<int> LockForUpdateAsync(long[] objectIds, CancellationToken cancellationToken);

        /// <summary>
        /// Strict form of <see cref="LockForUpdateAsync(long[])"/> for CAS patterns (lock -> re-read -> save):
        /// every requested id must exist. Throws <see cref="Exceptions.RedbLockNotAcquiredException"/>
        /// naming the missing ids, and refuses with <see cref="InvalidOperationException"/> to run
        /// outside an active transaction, where a "lock" would guarantee nothing.
        /// </summary>
        /// <param name="objectIds">IDs of objects that must all be locked</param>
        Task LockForUpdateRequiredAsync(params long[] objectIds);

        /// <inheritdoc cref="LockForUpdateRequiredAsync(long[])"/>
        /// <remarks>Paired overload: params is incompatible with a trailing token (B1 amendment).</remarks>
        Task LockForUpdateRequiredAsync(long[] objectIds, CancellationToken cancellationToken);
    }
}
