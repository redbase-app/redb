using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;

namespace redb.Core.Services;

/// <summary>
/// Background deletion service. Fire-and-forget delete: callers mark objects, return
/// immediately, and a polling worker physically purges the trash on a separate DB
/// connection. State of truth lives entirely in <c>_objects</c> rows (trash containers
/// with <c>_value_string='pending'</c>) — the worker drives off DB polling, so a
/// worker crash or force-kill leaves no in-memory state to recover from.
/// </summary>
public interface IBackgroundDeletionService
{
    /// <summary>
    /// Mark objects for deletion. Returns once <see cref="IObjectStorageProvider.SoftDeleteAsync(IEnumerable{long}, long?)"/>
    /// has re-parented the objects under the trash scheme — they disappear from regular
    /// queries immediately. The physical purge of the <c>_values</c> cascade runs in
    /// the next worker poll cycle.
    /// </summary>
    Task<DeletionMark> DeleteAsync(
        IEnumerable<long> objectIds,
        IRedbUser user,
        int batchSize = 10,
        long? trashParentId = null);

    /// <summary>
    /// No-op in the DB-polling design. Kept on the interface so callers that did a
    /// manual <c>SoftDeleteAsync</c> + <c>EnqueuePurge</c> handshake don't break — the
    /// trash row they wrote is picked up by the worker's next poll cycle on its own.
    /// </summary>
    void EnqueuePurge(long trashId, int totalCount, long userId, int batchSize = 10);

    /// <summary>
    /// Get current progress for a trash container from database.
    /// Returns null if trash container not found or already deleted.
    /// </summary>
    Task<PurgeProgress?> GetProgressAsync(long trashId);

    /// <summary>
    /// Get all active (pending/running) deletions for a user from database.
    /// </summary>
    Task<List<PurgeProgress>> GetUserActiveProgressAsync(long userId);

    /// <summary>
    /// Always 0 in the DB-polling design — there is no in-memory queue. Callers who
    /// want the actual count of pending trash containers should query the DB directly
    /// (see <c>IObjectStorageProvider.GetOrphanedDeletionTasksAsync</c>).
    /// </summary>
    int QueueLength { get; }
}

/// <summary>
/// Result of marking objects for deletion.
/// </summary>
/// <param name="TrashId">ID of the created trash container object</param>
/// <param name="MarkedCount">Number of objects marked for deletion (including descendants)</param>
public record DeletionMark(long TrashId, int MarkedCount);

/// <summary>
/// Progress of trash purge operation.
/// </summary>
/// <param name="TrashId">ID of the trash container being purged</param>
/// <param name="Deleted">Number of objects already deleted</param>
/// <param name="Remaining">Number of objects still remaining</param>
/// <param name="Status">Current status of the purge operation</param>
/// <param name="StartedAt">When the purge operation started</param>
/// <param name="UserId">ID of the user who initiated the deletion</param>
public record PurgeProgress(
    long TrashId, 
    int Deleted, 
    int Remaining, 
    PurgeStatus Status,
    DateTimeOffset StartedAt,
    long UserId)
{
    /// <summary>
    /// Total number of objects to delete.
    /// </summary>
    public int Total => Deleted + Remaining;
    
    /// <summary>
    /// Whether the purge operation is complete.
    /// </summary>
    public bool IsCompleted => Status == PurgeStatus.Completed;
}

/// <summary>
/// Status of a purge operation.
/// </summary>
public enum PurgeStatus
{
    /// <summary>Objects marked for deletion, purge not started yet</summary>
    Pending,
    
    /// <summary>Purge is in progress</summary>
    Running,
    
    /// <summary>Purge completed successfully</summary>
    Completed,
    
    /// <summary>Purge failed with error</summary>
    Failed,
    
    /// <summary>Purge was cancelled</summary>
    Cancelled
}

/// <summary>
/// Orphaned deletion task found at startup.
/// Used for cluster-safe recovery of incomplete deletions.
/// </summary>
/// <param name="TrashId">ID of the trash container</param>
/// <param name="Total">Total objects to delete</param>
/// <param name="Deleted">Already deleted count</param>
/// <param name="Status">Current status</param>
/// <param name="OwnerId">User who initiated the deletion</param>
public record OrphanedTask(long TrashId, int Total, int Deleted, string Status, long OwnerId);
