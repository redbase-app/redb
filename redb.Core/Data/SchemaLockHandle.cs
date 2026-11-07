using System;
using System.Threading;
using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// The lock that serializes the schema work of processes starting against one database: creating the schema,
    /// deploying the SQL module, building the indexes of the streams form, upgrading a SQLite file, synchronizing the
    /// schemes. Two nodes of a cluster that met an old database used to run the same upgrade at once, and the second
    /// failed on what the first had just created ("index already exists", "tuple concurrently updated").
    ///
    /// <para>
    /// A provider hands one per database: a server-side named lock on a connection of its own (PostgreSQL advisory
    /// lock, SQL Server application lock) or the single write lock of the file (SQLite). The attempt never blocks,
    /// so the wait, its log and its time-out live once, in <see cref="RedbServiceBase"/>. Disposing releases the lock
    /// when it was taken and frees the connection.
    /// </para>
    /// </summary>
    public abstract class SchemaLockHandle : IAsyncDisposable
    {
        /// <summary>One attempt to take the lock: true when it is now held, false when another process holds it.</summary>
        public abstract Task<bool> TryAcquireAsync(CancellationToken cancellationToken);

        /// <summary>What the lock is called on the server, for the log of a waiting node.</summary>
        public abstract string Name { get; }

        /// <inheritdoc />
        public abstract ValueTask DisposeAsync();

        /// <summary>A lock that is always granted: for a context the provider cannot lock (a custom one).</summary>
        public static SchemaLockHandle None { get; } = new NoLock();

        private sealed class NoLock : SchemaLockHandle
        {
            public override string Name => "none";
            public override Task<bool> TryAcquireAsync(CancellationToken cancellationToken) => Task.FromResult(true);
            public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
