using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace redb.Core.Providers.Base;

/// <summary>
/// The schema lock around a scheme sync. Nodes of a cluster sync the same schemes at once: they start together,
/// their modules share Props classes, a new version of a module adds a field on every node at the same moment.
/// Two of them inserting the same structure failed on the unique keys of <c>_schemes</c> and <c>_structures</c>,
/// two rebuilding the metadata cache of one scheme on its rows.
///
/// <para>
/// A sync first runs as a check: the same code, reading only. The first write it reaches stops it, and the sync
/// runs again under the schema lock of the service, now writing; a node that waited for the lock finds the work done
/// and writes nothing. A scheme already in step (nearly every start) never reaches a write and never takes the lock.
/// </para>
/// </summary>
public abstract partial class SchemeSyncProviderBase
{
    /// <summary>
    /// Runs work under the schema lock of the service that owns this provider (reentrant). Set by the service right
    /// after it builds the provider; left null, the sync writes without a lock, as a provider built on its own did.
    /// </summary>
    public Func<Func<Task>, CancellationToken, Task>? RunUnderSchemaLock { get; set; }

    /// <summary>
    /// Receives the schema problems a sync meets (duplicate root structures it leaves alone): the service logs them as
    /// errors and lists them in <see cref="IRedbService.SchemaProblems"/>. Left null, the provider logs them itself.
    /// </summary>
    public Action<Models.RedbSchemaProblem>? SchemaProblemFound { get; set; }

    private void ReportSchemaProblem(Models.RedbSchemaProblem problem)
    {
        if (SchemaProblemFound != null)
            SchemaProblemFound(problem);
        else
            Logger?.LogError("{Problem}", problem.Message);
    }

    private enum SyncPass { Check, Write }

    // The pass of the sync running in this flow; null outside one. A sync nested in another (SyncSchemeAsync calling
    // SyncStructuresFromTypeAsync) belongs to the outer pass.
    private readonly AsyncLocal<SyncPass?> _syncPass = new();

    /// <summary>Thrown at the first write of a check pass; never leaves <see cref="GuardSchemeSyncAsync{T}"/>.</summary>
    private sealed class SchemeWriteNeededException : Exception
    {
    }

    private bool IsCheckPass => _syncPass.Value == SyncPass.Check;

    /// <summary>Called before every write of a sync: a check pass stops here.</summary>
    private void BeforeSchemeWrite()
    {
        if (IsCheckPass)
            throw new SchemeWriteNeededException();
    }

    private async Task<T> GuardSchemeSyncAsync<T>(Func<Task<T>> sync, CancellationToken cancellationToken)
    {
        // Inside a transaction of the caller the lock would be released before the caller commits, and the next node
        // would not see the rows written under it: such a sync runs as before, unguarded.
        if (_syncPass.Value != null || RunUnderSchemaLock == null || Context.IsInTransaction)
            return await sync();

        _syncPass.Value = SyncPass.Check;
        try
        {
            return await sync();
        }
        catch (SchemeWriteNeededException)
        {
        }

        T result = default!;
        await RunUnderSchemaLock(async () =>
        {
            _syncPass.Value = SyncPass.Write;
            result = await sync();
        }, cancellationToken);
        return result;
    }
}
