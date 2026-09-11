using System;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// Mutual exclusion between the commands and the teardown of ONE physical connection
    /// (tsum garage report, 2026-09-11). A redb connection wrapper is single-command by
    /// design and always carried a fail-fast guard against two concurrent commands - but
    /// teardown was exempt from it, so an HTTP scope's dispose could run under a lazy
    /// loader's in-flight SELECT: NpgsqlOperationInProgressException on Reset, or (timing
    /// depending) an outright hang of scope disposal.
    ///
    /// The gate closes both holes and is SHARED by all three providers so the contract
    /// cannot drift:
    /// - a command entered after teardown began is refused with
    ///   <see cref="ObjectDisposedException"/> - the lazy loader's cue to fall back to a
    ///   detached (fresh-scope) load;
    /// - teardown waits for the in-flight command before touching the physical connection;
    ///   new entrants bounce the moment the flag is set, so the wait is bounded by one
    ///   command.
    /// </summary>
    public sealed class CommandGate
    {
        private readonly string _owner;
        private int _inUse;
        private volatile bool _disposed;

        public CommandGate(string ownerName) => _owner = ownerName;

        /// <summary>Teardown has begun (or completed); new commands are refused.</summary>
        public bool IsDisposed => _disposed;

        /// <summary>
        /// Enters a command. Throws <see cref="InvalidOperationException"/> on CONCURRENT use
        /// (one scoped service shared across exchanges) and <see cref="ObjectDisposedException"/>
        /// on entry after teardown began.
        /// </summary>
        public Releaser Enter()
        {
            ThrowIfDisposed();
            if (Interlocked.CompareExchange(ref _inUse, 1, 0) != 0)
                throw new InvalidOperationException(
                    $"IRedbService used concurrently: the same {_owner} was entered from two threads. " +
                    "Each exchange/request must resolve its OWN scoped IRedbService (ProcessWithRedb / controller.Redb()) — " +
                    "one instance is a single, non-thread-safe DB connection.");
            if (_disposed)
            {
                // Teardown began between the flag check and the CAS: hand the slot back so the
                // waiting dispose proceeds, and refuse exactly like the early check does.
                Interlocked.Exchange(ref _inUse, 0);
                ThrowIfDisposed();
            }
            return new Releaser(this);
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(_owner,
                    "The scope that owned this connection has ended; load through a fresh scope instead.");
        }

        /// <summary>
        /// The teardown wait budget derived from the connection's OWN command timeout: a
        /// command may legally run right up to it, so teardown waits that long plus a little
        /// slack for the driver to surface its own timeout first. Zero or negative means the
        /// driver allows infinite commands - teardown still must end, so a generous fixed
        /// budget applies; the command's own fault stays observable on its side either way.
        /// </summary>
        public static TimeSpan BudgetFrom(int commandTimeoutSeconds)
            => commandTimeoutSeconds > 0
                ? TimeSpan.FromSeconds(commandTimeoutSeconds + 5)
                : TimeSpan.FromMinutes(10);

        /// <summary>
        /// Marks the gate disposed and WAITS until the in-flight command (if any) releases
        /// the slot. The budget is deliberately REQUIRED - derive it from the connection's own
        /// command timeout via <see cref="BudgetFrom"/>; there is no magic default. It is a
        /// last-resort escape for a command stuck beyond its own timeout: teardown then
        /// proceeds rather than hang forever.
        /// </summary>
        public async ValueTask DisposeAndWaitAsync(TimeSpan budget)
        {
            _disposed = true;
            var clock = System.Diagnostics.Stopwatch.StartNew();
            while (Volatile.Read(ref _inUse) != 0 && clock.Elapsed < budget)
                await Task.Delay(10).ConfigureAwait(false);
        }

        public readonly struct Releaser : IDisposable
        {
            private readonly CommandGate _gate;
            public Releaser(CommandGate gate) => _gate = gate;
            public void Dispose() => Interlocked.Exchange(ref _gate._inUse, 0);
        }
    }
}
