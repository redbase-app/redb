using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using redb.Core.Models.Contracts;

namespace redb.Core.Services;

/// <summary>
/// Background deletion service with queue-based processing.
/// Provides fire-and-forget deletion: mark objects, enqueue purge, return immediately.
/// Purge runs in background thread with separate DB connection.
/// Progress is stored in trash object in database (persistent).
/// </summary>
public interface IBackgroundDeletionService
{
    /// <summary>
    /// Mark objects for deletion and enqueue background purge.
    /// Returns immediately after marking - purge runs in background.
    /// </summary>
    /// <param name="objectIds">IDs of objects to delete</param>
    /// <param name="user">User performing the operation</param>
    /// <param name="batchSize">Objects to delete per batch (default: 10)</param>
    /// <param name="trashParentId">Optional parent for trash container</param>
    /// <returns>Deletion mark with trash ID and count</returns>
    Task<DeletionMark> DeleteAsync(
        IEnumerable<long> objectIds, 
        IRedbUser user, 
        int batchSize = 10,
        long? trashParentId = null);
    
    /// <summary>
    /// Enqueue purge for an existing trash container.
    /// Use after manual SoftDeleteAsync call.
    /// </summary>
    /// <param name="trashId">Trash container ID</param>
    /// <param name="totalCount">Total objects to delete</param>
    /// <param name="userId">User ID for tracking</param>
    /// <param name="batchSize">Objects to delete per batch</param>
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
    /// Number of pending tasks in queue.
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
