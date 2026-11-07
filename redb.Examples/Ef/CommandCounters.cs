using System.Data.Common;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore.Diagnostics;
using redb.Core.Diagnostics;

namespace redb.Examples.Ef;

/// <summary>
/// Counts database round trips on both sides of the comparison, so the table can show how many commands
/// each library sent for the same task. The runner reads the counter before and after an example.
/// </summary>
internal static class CommandCounters
{
    private static long _redb;
    private static long _ef;
    private static ActivityListener? _redbListener;

    /// <summary>Commands redb has sent so far.</summary>
    public static long Redb => Interlocked.Read(ref _redb);

    /// <summary>Commands EF Core has sent so far.</summary>
    public static long Ef => Interlocked.Read(ref _ef);

    /// <summary>
    /// Starts counting redb commands. redb declares every database command as an activity on
    /// <see cref="RedbActivitySource"/>; the listener only counts them and asks for no tags.
    /// </summary>
    public static void StartRedb()
    {
        if (_redbListener != null)
            return;

        _redbListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RedbActivitySource.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.PropagationData,
            ActivityStarted = _ => Interlocked.Increment(ref _redb)
        };
        ActivitySource.AddActivityListener(_redbListener);
    }

    /// <summary>Interceptor that counts every command EF Core executes; one instance for all contexts.</summary>
    public static readonly DbCommandInterceptor EfInterceptor = new EfCommandCounter();

    private sealed class EfCommandCounter : DbCommandInterceptor
    {
        public override InterceptionResult<DbDataReader> ReaderExecuting(
            DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
        {
            Interlocked.Increment(ref _ef);
            return result;
        }

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _ef);
            return ValueTask.FromResult(result);
        }

        public override InterceptionResult<object> ScalarExecuting(
            DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
        {
            Interlocked.Increment(ref _ef);
            return result;
        }

        public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(
            DbCommand command, CommandEventData eventData, InterceptionResult<object> result,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _ef);
            return ValueTask.FromResult(result);
        }

        public override InterceptionResult<int> NonQueryExecuting(
            DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
        {
            Interlocked.Increment(ref _ef);
            return result;
        }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
            DbCommand command, CommandEventData eventData, InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _ef);
            return ValueTask.FromResult(result);
        }
    }
}
