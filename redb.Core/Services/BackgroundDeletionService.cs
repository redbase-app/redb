using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Contracts;

namespace redb.Core.Services;

/// <summary>
/// Background deletion service backed by DB polling.
/// <para>
/// Earlier revisions used an in-memory <c>Channel&lt;PurgeTask&gt;</c> for low-latency
/// wake-up plus a startup-only <c>RecoverOrphanedTasksAsync</c> sweep for crash
/// recovery. That created a dual-state problem: state of truth split between the
/// channel (in memory, lost on kill) and the DB (durable). Worker force-kills always
/// left a tail of orphaned 'pending' rows that the next startup had to drain in a
/// flood of single-item purges. The "fix" of a periodic sweeper would have raced
/// against the live channel reader unless we added age filters to the SQL.
/// </para>
/// <para>
/// This revision drops the channel entirely. The DB IS the queue. <see cref="ExecuteAsync"/>
/// polls <see cref="IObjectStorageProvider.GetOrphanedDeletionTasksAsync"/> on a fixed
/// interval, atomically claims each pending row via
/// <see cref="IObjectStorageProvider.TryClaimOrphanedTaskAsync"/> (cluster-safe), and
/// purges each container with the existing batched <see cref="IRedbService.PurgeTrashAsync"/>.
/// Force-kill leaves nothing in memory because nothing was in memory — the next poll
/// cycle (or next startup) just claims whatever's still 'pending'.
/// </para>
/// <para>
/// Trade-off: cleanup latency shifts from "milliseconds via channel" to "≤ poll
/// interval", but cleanup is invisible to API consumers anyway (the object is
/// re-parented under the trash scheme by <c>SoftDeleteAsync</c> synchronously, so
/// it disappears from queries immediately — only the physical purge of the
/// <c>_values</c> cascade is deferred).
/// </para>
/// </summary>
public class BackgroundDeletionService : BackgroundService, IBackgroundDeletionService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<BackgroundDeletionService>? _logger;

    /// <summary>
    /// Minutes after which a 'running' task is considered orphaned and re-claimed.
    /// Conservative — covers the case where another live instance is still working it.
    /// </summary>
    private const int OrphanTimeoutMinutes = 30;

    /// <summary>Default batch size for trash-container purges.</summary>
    private const int DefaultBatchSize = 10;

    /// <summary>How often the polling loop scans the DB for pending purge work.</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(5);

    /// <summary>Maximum number of trash containers claimed per poll cycle.</summary>
    private const int PollPageSize = 50;

    /// <summary>
    /// Creates a new BackgroundDeletionService.
    /// </summary>
    public BackgroundDeletionService(
        IServiceProvider serviceProvider,
        ILogger<BackgroundDeletionService>? logger = null)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger;
    }

    /// <summary>
    /// Always 0 in the DB-polling design — there is no in-memory queue. The "queue" lives
    /// in <c>_objects</c> as rows with <c>_value_string='pending'</c>; callers that need
    /// that count should call <see cref="IRedbService.GetOrphanedDeletionTasksAsync"/>
    /// directly.
    /// </summary>
    public int QueueLength => 0;

    /// <summary>
    /// Mark objects for deletion. Returns once <c>SoftDeleteAsync</c> has re-parented
    /// the objects under the trash scheme (so they immediately disappear from regular
    /// queries). The physical purge runs in a future <see cref="ExecuteAsync"/> poll
    /// cycle on a separate DB connection.
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

        await using var scope = _serviceProvider.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        // SoftDeleteAsync writes the trash container with status='pending' in DB. The
        // polling loop in ExecuteAsync picks it up on the next cycle (≤ PollInterval
        // wait, typically much less). No channel write — DB is the queue.
        return await redb.SoftDeleteAsync(ids, user, trashParentId);
    }

    /// <summary>
    /// No-op. Kept on the interface for backward compatibility with callers like
    /// <c>GroupService.AddMemberAsync</c> that did manual <c>SoftDeleteAsync</c> +
    /// <c>EnqueuePurge</c>. The trash row those callers created is picked up by the
    /// next poll cycle, so the explicit wake-up signal is no longer needed.
    /// </summary>
    public void EnqueuePurge(long trashId, int totalCount, long userId, int batchSize = 10)
    {
        // Intentionally empty — the polling loop finds the row by itself.
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
    /// Polling loop — every <see cref="PollInterval"/> finds up to <see cref="PollPageSize"/>
    /// pending trash containers, atomically claims each via
    /// <see cref="IRedbService.TryClaimOrphanedTaskAsync"/>, and purges with
    /// <see cref="ProcessPurgeTask"/>. Cluster-safe by the claim's atomic UPDATE — if
    /// another worker instance grabbed the same row, the claim returns false and we
    /// skip it.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger?.LogInformation("BackgroundDeletionService started (DB polling, interval={Interval})", PollInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await PollAndProcessAsync(stoppingToken).ConfigureAwait(false);
                // No back-off when we found work — drain as fast as the DB lets us; one
                // poll cycle handles up to PollPageSize containers. When the page is
                // empty, sleep PollInterval. Saturates only when there is real load.
                if (processed == 0)
                {
                    try { await Task.Delay(PollInterval, stoppingToken).ConfigureAwait(false); }
                    catch (OperationCanceledException) { break; }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "BackgroundDeletionService poll cycle failed; backing off {Interval}", PollInterval);
                try { await Task.Delay(PollInterval, stoppingToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
        }

        _logger?.LogInformation("BackgroundDeletionService stopped");
    }

    /// <summary>
    /// One poll cycle: scan, claim, and process up to <see cref="PollPageSize"/> tasks.
    /// Returns the number of tasks actually processed (after claim contention with
    /// peer workers) so the outer loop can decide whether to keep draining or sleep.
    /// </summary>
    private async Task<int> PollAndProcessAsync(CancellationToken ct)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        var tasks = await redb.GetOrphanedDeletionTasksAsync(OrphanTimeoutMinutes).ConfigureAwait(false);
        if (tasks.Count == 0) return 0;

        // Cap per cycle so a giant backlog doesn't block shutdown — anything not handled
        // here gets picked up on the next cycle (or by another worker in a cluster).
        var page = tasks.Take(PollPageSize).ToList();
        _logger?.LogDebug("Poll cycle found {Total} pending tasks, processing up to {Page}", tasks.Count, page.Count);

        var processed = 0;
        foreach (var task in page)
        {
            if (ct.IsCancellationRequested) break;

            var claimed = await redb.TryClaimOrphanedTaskAsync(task.TrashId, OrphanTimeoutMinutes).ConfigureAwait(false);
            if (!claimed)
            {
                _logger?.LogDebug("Task TrashId={TrashId} already claimed by another worker", task.TrashId);
                continue;
            }

            try
            {
                await ProcessPurgeTask(task.TrashId, task.Total, ct).ConfigureAwait(false);
                processed++;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Purge failed for TrashId={TrashId}; will be retried next cycle once stale", task.TrashId);
            }
        }

        return processed;
    }

    /// <summary>
    /// Drain one trash container in batches of <see cref="DefaultBatchSize"/>. The
    /// inter-batch <c>Task.Delay(50)</c> is backpressure: a single user-delete can
    /// cascade to thousands of <c>_values</c> rows, and the small pause lets live
    /// traffic (token issuance, /me reads) fit between purge batches instead of
    /// fighting for the same row locks.
    /// </summary>
    private async Task ProcessPurgeTask(long trashId, int totalCount, CancellationToken ct)
    {
        _logger?.LogDebug("Processing purge: TrashId={TrashId}, Total={Total}", trashId, totalCount);

        while (!ct.IsCancellationRequested)
        {
            await using var scope = _serviceProvider.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

            var progress = await redb.GetDeletionProgressAsync(trashId).ConfigureAwait(false);
            if (progress == null)
            {
                _logger?.LogDebug("Trash container {TrashId} already gone, purge done", trashId);
                break;
            }
            if (progress.Status == PurgeStatus.Completed)
            {
                _logger?.LogDebug("Purge completed: TrashId={TrashId}", trashId);
                break;
            }

            await redb.PurgeTrashAsync(trashId, totalCount, DefaultBatchSize).ConfigureAwait(false);

            try { await Task.Delay(50, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { break; }
        }
    }
}
