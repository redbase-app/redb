using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Contracts;

namespace redb.Core.Services;

/// <summary>
/// Task for background purge queue.
/// </summary>
internal record PurgeTask(long TrashId, int TotalCount, long UserId, int BatchSize);

/// <summary>
/// Background deletion service with queue-based processing.
/// Implements IHostedService for background task execution.
/// Uses Channel for thread-safe task queue.
/// Each purge runs in separate DI scope with its own DB connection.
/// Progress is stored in trash object in database (no memory cache).
/// CLUSTER-SAFE: Recovers orphaned tasks at startup with atomic claiming.
/// </summary>
public class BackgroundDeletionService : BackgroundService, IBackgroundDeletionService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<BackgroundDeletionService>? _logger;
    
    /// <summary>
    /// Minutes after which a 'running' task is considered orphaned.
    /// Conservative timeout to avoid claiming tasks from live instances.
    /// </summary>
    private const int OrphanTimeoutMinutes = 30;
    
    /// <summary>
    /// Default batch size for orphaned task recovery.
    /// </summary>
    private const int DefaultBatchSize = 10;
    
    private readonly Channel<PurgeTask> _queue = Channel.CreateUnbounded<PurgeTask>(
        new UnboundedChannelOptions { SingleReader = true });
    
    /// <summary>
    /// Creates a new BackgroundDeletionService.
    /// </summary>
    /// <param name="serviceProvider">Service provider for creating scopes</param>
    /// <param name="logger">Optional logger</param>
    public BackgroundDeletionService(
        IServiceProvider serviceProvider,
        ILogger<BackgroundDeletionService>? logger = null)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger;
    }
    
    /// <summary>
    /// Number of pending tasks in queue.
    /// </summary>
    public int QueueLength => _queue.Reader.Count;
    
    /// <summary>
    /// Mark objects for deletion and enqueue background purge.
    /// Returns immediately after marking - purge runs in background.
    /// </summary>
    public async Task<DeletionMark> DeleteAsync(
        IEnumerable<long> objectIds, 
        IRedbUser user, 
        int batchSize = 10,
        long? trashParentId = null)
    {
        var ids = objectIds.ToArray();
        if (ids.Length == 0)
            return new DeletionMark(0, 0);
        
        // Use scoped service for marking
        await using var scope = _serviceProvider.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        
        // Mark for deletion (fast, atomic) - progress saved to trash object in DB
        var mark = await redb.SoftDeleteAsync(ids, user, trashParentId);
        
        if (mark.MarkedCount > 0)
        {
            EnqueuePurge(mark.TrashId, mark.MarkedCount, user.Id, batchSize);
        }
        
        return mark;
    }
    
    /// <summary>
    /// Enqueue purge for an existing trash container.
    /// </summary>
    public void EnqueuePurge(long trashId, int totalCount, long userId, int batchSize = 10)
    {
        var task = new PurgeTask(trashId, totalCount, userId, batchSize);
        
        if (!_queue.Writer.TryWrite(task))
        {
            _logger?.LogWarning("Failed to enqueue purge task for TrashId={TrashId}", trashId);
            return;
        }
        
        _logger?.LogDebug("Enqueued purge task: TrashId={TrashId}, Total={Total}", trashId, totalCount);
    }
    
    /// <summary>
    /// Get current progress for a trash container from database.
    /// </summary>
    public async Task<PurgeProgress?> GetProgressAsync(long trashId)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        return await redb.GetDeletionProgressAsync(trashId);
    }
    
    /// <summary>
    /// Get all active deletions for a user from database.
    /// </summary>
    public async Task<List<PurgeProgress>> GetUserActiveProgressAsync(long userId)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        return await redb.GetUserActiveDeletionsAsync(userId);
    }
    
    /// <summary>
    /// Background task processor - reads from queue and executes purges.
    /// CLUSTER-SAFE: Recovers orphaned tasks at startup before processing queue.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger?.LogInformation("BackgroundDeletionService started");
        
        // CLUSTER-SAFE: Recover orphaned tasks from previous crashes/restarts
        try
        {
            await RecoverOrphanedTasksAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            // Don't fail startup if recovery fails - just log and continue
            _logger?.LogWarning(ex, "Failed to recover orphaned deletion tasks");
        }
        
        // Process queue
        await foreach (var task in _queue.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                await ProcessPurgeTask(task, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                _logger?.LogInformation("BackgroundDeletionService stopping");
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Purge failed for TrashId={TrashId}", task.TrashId);
            }
        }
        
        _logger?.LogInformation("BackgroundDeletionService stopped");
    }
    
    /// <summary>
    /// Recover orphaned deletion tasks from database.
    /// CLUSTER-SAFE: Uses atomic UPDATE to claim tasks, preventing race conditions.
    /// Only claims 'pending' tasks OR 'running' tasks with stale _date_modify.
    /// </summary>
    private async Task RecoverOrphanedTasksAsync(CancellationToken ct)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        
        // Find orphaned tasks: pending OR running with stale timestamp
        var orphanedTasks = await redb.GetOrphanedDeletionTasksAsync(OrphanTimeoutMinutes);
        
        if (orphanedTasks.Count == 0)
        {
            _logger?.LogDebug("No orphaned deletion tasks found");
            return;
        }
        
        _logger?.LogInformation("Found {Count} orphaned deletion tasks, attempting recovery", orphanedTasks.Count);
        
        var claimedCount = 0;
        foreach (var task in orphanedTasks)
        {
            if (ct.IsCancellationRequested) break;
            
            // CLUSTER-SAFE: Atomically try to claim the task
            // If another instance already claimed it, this returns false
            var claimed = await redb.TryClaimOrphanedTaskAsync(task.TrashId, OrphanTimeoutMinutes);
            
            if (claimed)
            {
                claimedCount++;
                var remaining = task.Total - task.Deleted;
                
                _logger?.LogInformation(
                    "Claimed orphaned task: TrashId={TrashId}, Status={Status}, Total={Total}, Deleted={Deleted}, Remaining={Remaining}",
                    task.TrashId, task.Status, task.Total, task.Deleted, remaining);
                
                // Enqueue for processing (already claimed, will continue from where it left off)
                EnqueuePurge(task.TrashId, task.Total, task.OwnerId, DefaultBatchSize);
            }
            else
            {
                _logger?.LogDebug(
                    "Task TrashId={TrashId} already claimed by another instance",
                    task.TrashId);
            }
        }
        
        _logger?.LogInformation("Recovered {Claimed} of {Total} orphaned tasks", claimedCount, orphanedTasks.Count);
    }
    
    private async Task ProcessPurgeTask(PurgeTask task, CancellationToken ct)
    {
        _logger?.LogDebug("Processing purge: TrashId={TrashId}", task.TrashId);
        
        while (!ct.IsCancellationRequested)
        {
            // Create new scope = new DB connection for each batch
            await using var scope = _serviceProvider.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            
            // Check if trash still exists
            var progress = await redb.GetDeletionProgressAsync(task.TrashId);
            if (progress == null)
            {
                // Trash container was deleted (completed or manually removed)
                _logger?.LogDebug("Trash container {TrashId} not found, purge complete", task.TrashId);
                break;
            }
            
            if (progress.Status == PurgeStatus.Completed)
            {
                _logger?.LogInformation("Purge completed: TrashId={TrashId}", task.TrashId);
                break;
            }
            
            // Execute one batch - this updates progress in DB
            await redb.PurgeTrashAsync(task.TrashId, task.TotalCount, task.BatchSize);
            
            // Small delay between batches
            await Task.Delay(50, ct);
        }
        
        if (ct.IsCancellationRequested)
        {
            _logger?.LogWarning("Purge cancelled for TrashId={TrashId}", task.TrashId);
        }
    }
}
