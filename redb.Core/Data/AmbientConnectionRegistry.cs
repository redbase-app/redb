using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace redb.Core.Data
{
    /// <summary>
    /// One physical connection per database per ambient <see cref="Transaction"/> (plan
    /// docs/V4/AMBIENT_TRANSACTION_PLAN.md, 2026-09-14).
    /// <para>
    /// A redb.Route <c>.Transacted()</c> block is a <see cref="TransactionScope"/>, and everything it writes -
    /// redb objects, raw SQL through <c>redb.Context</c>, a store resolving a scope of its own, the key
    /// generator - must land in that one transaction. Two connections open at the same time inside it turn
    /// it into a distributed transaction: SQL Server refuses it, PostgreSQL aborts at commit, and SQLite
    /// never takes part in System.Transactions at all. So the TRANSACTION holds the connection: every redb
    /// connection wrapper of the same database asks this registry for it while the transaction is ambient.
    /// </para>
    /// <list type="bullet">
    ///   <item>The connection is opened inside the transaction on first use, so SqlClient and Npgsql enlist
    ///   it themselves.</item>
    ///   <item>A provider whose driver cannot enlist (SQLite) begins its own transaction on it; the entry
    ///   enlists as a volatile resource and commits or rolls that transaction back with the scope.</item>
    ///   <item>The entry is released when the transaction completes.</item>
    ///   <item>Dependent clones share the transaction and therefore the entry: two commands at the same time
    ///   are refused (owner decision: parallel branches in one transaction are not supported).</item>
    /// </list>
    /// </summary>
    public static class AmbientConnectionRegistry
    {
        private const string ConcurrentUseMessage =
            "The connection of an ambient transaction was used from two threads at the same time. One transaction " +
            "has one connection per database, and a connection runs one command at a time: parallel branches " +
            "cannot share a transaction. Run the branches sequentially, or give each branch its own transaction " +
            "(a sub-route with its own Transacted(), the parent route without one).";

        private const string TransactionEndedMessage =
            "The ambient transaction of this connection has already ended (committed, rolled back - for example " +
            "because a parallel branch failed - or timed out). Commands after its end are refused: they would " +
            "run outside the transaction.";

        private const string SessionSettingsMismatchMessage =
            "Two redb configurations of the same database that differ in their connection parameters or session settings " +
            "are used inside one transaction. The transaction holds one connection per database, set up by the " +
            "configuration that opened it, and a command of the other configuration would run on it with settings that " +
            "are not its own. Inside one transaction, reach this database through one configuration, or give each " +
            "configuration its own transaction.";

        private static readonly ConcurrentDictionary<string, AmbientConnection> Entries = new(StringComparer.Ordinal);
        // One open at a time per transaction and database. One process-wide lock held every other transaction's first
        // open behind a SQLite BEGIN IMMEDIATE waiting for its write lock (review after 4.0.0).
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> Opening = new(StringComparer.Ordinal);

        /// <summary>Number of live entries (diagnostics and tests).</summary>
        public static int Count => Entries.Count;

        /// <summary>
        /// The connection of <paramref name="transaction"/> for <paramref name="databaseKey"/>, opened on first
        /// use, with its command gate entered. Dispose the lease when the command is done.
        /// </summary>
        /// <param name="transaction">The ambient transaction (<see cref="Transaction.Current"/>).</param>
        /// <param name="databaseKey">Provider-qualified identity of the database (server, database, user).</param>
        /// <param name="sessionSignature">The caller's configuration: the cache domain of its connection string
        /// (<see cref="redb.Core.Models.Configuration.RedbServiceConfiguration.ComputeCacheDomain"/>) plus the session
        /// settings redb applies on open. A different signature on a connection the transaction already holds is
        /// refused: that connection carries the settings of the configuration that opened it.</param>
        /// <param name="openAsync">Opens a new connection with the provider's session settings applied.</param>
        /// <param name="beginOwnTransaction">For a driver that cannot enlist: begins the provider transaction
        /// the entry commits or rolls back with the scope. Null for drivers that enlist.</param>
        /// <param name="cancellationToken">Cancels opening the connection.</param>
        public static async Task<AmbientLease> AcquireAsync(
            Transaction transaction, string databaseKey, string sessionSignature,
            Func<CancellationToken, Task<DbConnection>> openAsync,
            Func<DbConnection, DbTransaction>? beginOwnTransaction,
            CancellationToken cancellationToken = default)
        {
            var entry = await GetOrOpenAsync(transaction, databaseKey, sessionSignature, openAsync, beginOwnTransaction, cancellationToken)
                .ConfigureAwait(false);
            return new AmbientLease(entry, entry.Gate.Enter());
        }

        /// <summary>Synchronous <see cref="AcquireAsync"/> for the thread-pool-free paths.</summary>
        public static AmbientLease Acquire(
            Transaction transaction, string databaseKey, string sessionSignature,
            Func<DbConnection> open,
            Func<DbConnection, DbTransaction>? beginOwnTransaction)
        {
            var entry = GetOrOpen(transaction, databaseKey, sessionSignature, open, beginOwnTransaction);
            return new AmbientLease(entry, entry.Gate.Enter());
        }

        /// <summary>
        /// The entry without entering its gate - for work that runs between commands of the same flow
        /// (bulk copy, key generation) and must use the transaction's connection.
        /// </summary>
        public static async Task<AmbientConnection> GetOrOpenAsync(
            Transaction transaction, string databaseKey, string sessionSignature,
            Func<CancellationToken, Task<DbConnection>> openAsync,
            Func<DbConnection, DbTransaction>? beginOwnTransaction,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(transaction);
            ArgumentNullException.ThrowIfNull(sessionSignature);
            var key = KeyFor(transaction, databaseKey);
            if (Entries.TryGetValue(key, out var existing))
                return Matching(existing, sessionSignature);

            var opening = Opening.GetOrAdd(key, static _ => new SemaphoreSlim(1, 1));
            await opening.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (Entries.TryGetValue(key, out existing))
                    return Matching(existing, sessionSignature);

                var connection = await openAsync(cancellationToken).ConfigureAwait(false);
                return Register(transaction, key, sessionSignature, connection, beginOwnTransaction);
            }
            finally
            {
                opening.Release();
            }
        }

        /// <summary>Synchronous <see cref="GetOrOpenAsync"/>.</summary>
        public static AmbientConnection GetOrOpen(
            Transaction transaction, string databaseKey, string sessionSignature,
            Func<DbConnection> open,
            Func<DbConnection, DbTransaction>? beginOwnTransaction)
        {
            ArgumentNullException.ThrowIfNull(transaction);
            ArgumentNullException.ThrowIfNull(sessionSignature);
            var key = KeyFor(transaction, databaseKey);
            if (Entries.TryGetValue(key, out var existing))
                return Matching(existing, sessionSignature);

            var opening = Opening.GetOrAdd(key, static _ => new SemaphoreSlim(1, 1));
            opening.Wait();
            try
            {
                if (Entries.TryGetValue(key, out existing))
                    return Matching(existing, sessionSignature);

                return Register(transaction, key, sessionSignature, open(), beginOwnTransaction);
            }
            finally
            {
                opening.Release();
            }
        }

        /// <summary>The live entry of <paramref name="transaction"/> for the database, or null when none was opened.</summary>
        public static AmbientConnection? TryGet(Transaction transaction, string databaseKey)
        {
            ArgumentNullException.ThrowIfNull(transaction);
            return Entries.TryGetValue(KeyFor(transaction, databaseKey), out var entry) ? entry : null;
        }

        // Dependent clones report the local identifier of the transaction they depend on, so every branch of
        // one transaction resolves the same entry; RequiresNew gets an identifier of its own.
        private static string KeyFor(Transaction transaction, string databaseKey)
            => transaction.TransactionInformation.LocalIdentifier + "|" + databaseKey;

        private static AmbientConnection Matching(AmbientConnection entry, string sessionSignature)
            => string.Equals(entry.SessionSignature, sessionSignature, StringComparison.Ordinal)
                ? entry
                : throw new InvalidOperationException(SessionSettingsMismatchMessage);

        private static AmbientConnection Register(
            Transaction transaction, string key, string sessionSignature, DbConnection connection,
            Func<DbConnection, DbTransaction>? beginOwnTransaction)
        {
            DbTransaction? own = null;
            AmbientConnection? entry = null;
            try
            {
                // A connection opened for a transaction that already ended would run its commands outside it.
                var status = transaction.TransactionInformation.Status;
                if (status == TransactionStatus.Aborted)
                    throw new TransactionAbortedException(TransactionEndedMessage);
                if (status != TransactionStatus.Active)
                    throw new TransactionException(TransactionEndedMessage);

                if (beginOwnTransaction != null)
                    own = beginOwnTransaction(connection);

                // A command may legally run up to the connection's own command timeout; the end of the
                // transaction waits that long for the in-flight one.
                TimeSpan budget;
                using (var probe = connection.CreateCommand())
                    budget = CommandGate.BudgetFrom(probe.CommandTimeout);

                entry = new AmbientConnection(key, sessionSignature, connection, own, budget,
                    new CommandGate("ambient transaction connection", ConcurrentUseMessage, TransactionEndedMessage));
                // Reachable before the completion handler is attached: on a transaction that ends in between,
                // the handler runs at once and must find the entry to remove.
                Entries[key] = entry;
                if (own != null)
                    transaction.EnlistVolatile(entry, EnlistmentOptions.None);
                transaction.TransactionCompleted += (_, _) => Release(entry);
                return entry;
            }
            catch
            {
                // Nothing else will end this transaction or close the connection.
                if (entry != null)
                    Entries.TryRemove(new System.Collections.Generic.KeyValuePair<string, AmbientConnection>(key, entry));
                Opening.TryRemove(key, out _);
                try
                {
                    own?.Dispose();
                }
                finally
                {
                    connection.Dispose();
                }
                throw;
            }
        }

        private static void Release(AmbientConnection entry)
        {
            Entries.TryRemove(new System.Collections.Generic.KeyValuePair<string, AmbientConnection>(entry.Key, entry));
            // The transaction ended: a later caller of this key opens nothing (Register refuses), so its gate is done.
            Opening.TryRemove(entry.Key, out _);
            entry.Close();
        }
    }

    /// <summary>
    /// The connection an ambient transaction holds for one database. See <see cref="AmbientConnectionRegistry"/>.
    /// </summary>
    public sealed class AmbientConnection : ISinglePhaseNotification
    {
        private readonly TimeSpan _teardownBudget;

        internal AmbientConnection(
            string key, string sessionSignature, DbConnection connection, DbTransaction? ownTransaction, TimeSpan teardownBudget,
            CommandGate gate)
        {
            Key = key;
            SessionSignature = sessionSignature;
            Connection = connection;
            OwnTransaction = ownTransaction;
            _teardownBudget = teardownBudget;
            Gate = gate;
        }

        internal string Key { get; }

        /// <summary>Connection parameters and session settings of the configuration that opened the connection.</summary>
        public string SessionSignature { get; }

        internal CommandGate Gate { get; }

        /// <summary>The open connection. MSSQL and PostgreSQL enlisted it in the transaction when it opened.</summary>
        public DbConnection Connection { get; }

        /// <summary>
        /// The provider transaction a driver that cannot enlist runs under (SQLite); commands must be bound to
        /// it. Null for drivers that enlist.
        /// </summary>
        public DbTransaction? OwnTransaction { get; }

        /// <summary>
        /// Refuses new commands and waits for the in-flight one. The transaction can end while a branch still
        /// runs a command - a failed dependent clone rolls the whole transaction back at once, and so does the
        /// scope timeout. Ending the provider transaction under that command races a non-thread-safe
        /// connection, and on SQLite a command that slips in after ROLLBACK runs in autocommit and stays.
        /// </summary>
        private void EndCommands()
            => Gate.DisposeAndWaitAsync(_teardownBudget).AsTask().GetAwaiter().GetResult();

        void IEnlistmentNotification.Prepare(PreparingEnlistment preparingEnlistment)
        {
            EndCommands();
            preparingEnlistment.Prepared();
        }

        void IEnlistmentNotification.Commit(Enlistment enlistment)
        {
            try
            {
                EndCommands();
                OwnTransaction?.Commit();
            }
            finally
            {
                enlistment.Done();
            }
        }

        void IEnlistmentNotification.Rollback(Enlistment enlistment)
        {
            try
            {
                EndCommands();
                OwnTransaction?.Rollback();
            }
            finally
            {
                enlistment.Done();
            }
        }

        void IEnlistmentNotification.InDoubt(Enlistment enlistment)
        {
            try
            {
                EndCommands();
                OwnTransaction?.Rollback();
            }
            finally
            {
                enlistment.Done();
            }
        }

        void ISinglePhaseNotification.SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
        {
            try
            {
                EndCommands();
                OwnTransaction?.Commit();
            }
            catch (Exception commitFailure)
            {
                // The transaction manager reports the abort to the scope's owner; this is not a swallow.
                singlePhaseEnlistment.Aborted(commitFailure);
                return;
            }
            singlePhaseEnlistment.Committed();
        }

        internal void Close()
        {
            // MSSQL and PostgreSQL end the transaction in the driver, not through this entry: the wait for a
            // branch's in-flight command happens here, before the connection is disposed under it.
            EndCommands();
            try
            {
                OwnTransaction?.Dispose();
            }
            finally
            {
                Connection.Dispose();
            }
        }
    }

    /// <summary>One command's hold on an <see cref="AmbientConnection"/>: releases the connection's gate on dispose.</summary>
    public sealed class AmbientLease : IDisposable
    {
        private readonly CommandGate.Releaser _releaser;
        private int _disposed;

        internal AmbientLease(AmbientConnection entry, CommandGate.Releaser releaser)
        {
            Entry = entry;
            _releaser = releaser;
        }

        /// <summary>The transaction's connection for this database.</summary>
        public AmbientConnection Entry { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                _releaser.Dispose();
        }
    }
}
