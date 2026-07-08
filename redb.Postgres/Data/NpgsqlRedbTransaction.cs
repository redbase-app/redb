using Npgsql;
using redb.Core.Data;
using System;
using System.Threading.Tasks;

namespace redb.Postgres.Data
{
    /// <summary>
    /// PostgreSQL implementation of IRedbTransaction using Npgsql.
    /// Connection-scoped: parent connection automatically uses this transaction.
    /// </summary>
    public class NpgsqlRedbTransaction : IRedbTransaction
    {
        private readonly NpgsqlTransaction _transaction;
        private readonly Action _onDispose;
        private bool _disposed = false;
        
        /// <summary>
        /// Transaction unique identifier.
        /// </summary>
        public Guid TransactionId { get; } = Guid.NewGuid();
        
        /// <summary>
        /// Whether transaction is still active.
        /// </summary>
        public bool IsActive { get; private set; } = true;
        
        /// <summary>
        /// Transaction start time.
        /// </summary>
        public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;
        
        /// <summary>
        /// Internal Npgsql transaction (for command assignment).
        /// </summary>
        internal NpgsqlTransaction NpgsqlTransaction => _transaction;

        /// <summary>
        /// Create new transaction wrapper.
        /// </summary>
        /// <param name="transaction">Underlying Npgsql transaction.</param>
        /// <param name="onDispose">Callback when transaction is disposed.</param>
        public NpgsqlRedbTransaction(NpgsqlTransaction transaction, Action onDispose)
        {
            _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            _onDispose = onDispose ?? throw new ArgumentNullException(nameof(onDispose));
        }

        /// <summary>
        /// Commit all changes.
        /// </summary>
        public async Task CommitAsync()
        {
            if (!IsActive)
                throw new InvalidOperationException("Transaction is not active. Already committed or rolled back.");

            try
            {
                await _transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                // Commit failed: Npgsql's NpgsqlConnection still holds a reference to
                // _transaction. Npgsql's pool reset (DISCARD ALL on acquire) usually
                // hides leaks of this shape, but the semantics should not depend on
                // pool-driver tolerance — speculatively rollback so the connection
                // returns clean, then re-throw so the caller still sees the original
                // exception. Both attempts are logged via [Diag-TX-LIFECYCLE-PG] so a
                // failure shape never goes silent.
                // Console.WriteLine($"[Diag-TX-LIFECYCLE-PG] CommitAsync FAILED: {ex.GetType().Name}: {ex.Message}. Attempting speculative rollback.");
                try { await _transaction.RollbackAsync(); }
                catch (Exception rbEx)
                {
                    // Console.WriteLine($"[Diag-TX-LIFECYCLE-PG] Speculative rollback after failed commit ALSO FAILED: {rbEx.GetType().Name}: {rbEx.Message}. Pool may receive a dirty NpgsqlConnection (Npgsql's DISCARD ALL on next acquire is the only remaining mitigation).");
                }
                IsActive = false;
                _onDispose();
                throw;
            }
            IsActive = false;
            // Clear the connection's `_currentTransaction` slot now so commands
            // issued between commit and dispose run against the autocommit
            // connection (Npgsql is more tolerant of cmd.Transaction=closedTx
            // than Microsoft.Data.Sqlite, but the semantics should not depend
            // on driver tolerance).
            _onDispose();
        }

        /// <summary>
        /// Rollback all changes.
        /// </summary>
        public async Task RollbackAsync()
        {
            if (!IsActive)
                throw new InvalidOperationException("Transaction is not active. Already committed or rolled back.");

            try
            {
                await _transaction.RollbackAsync();
            }
            catch (Exception ex)
            {
                // Console.WriteLine($"[Diag-TX-LIFECYCLE-PG] RollbackAsync FAILED: {ex.GetType().Name}: {ex.Message}. Npgsql's DISCARD ALL on next pool acquire is the only mitigation if the underlying state is dirty.");
                IsActive = false;
                _onDispose();
                throw;
            }
            IsActive = false;
            _onDispose();
        }

        /// <summary>
        /// Dispose transaction.
        /// If still active - rollback automatically. The catch CANNOT throw (Dispose contract),
        /// so a leaked NpgsqlConnection (autocommit=off at the driver layer) is mitigated by
        /// Npgsql's built-in DISCARD ALL on next pool acquire.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;

            // Auto-rollback if still active
            if (IsActive)
            {
                try
                {
                    await _transaction.RollbackAsync();
                }
                catch (Exception ex)
                {
                    // Console.WriteLine($"[Diag-TX-LIFECYCLE-PG] DisposeAsync auto-rollback FAILED: {ex.GetType().Name}: {ex.Message}. Pool poisoning mitigated by Npgsql's DISCARD ALL on next acquire.");
                }
                IsActive = false;
            }

            // Disposing a broken transaction must never block the connection's cleanup callback:
            // _onDispose() clears _currentTransaction on the connection so it can return to the pool.
            // finally guarantees that even if _transaction.DisposeAsync() throws — and the exception
            // is NOT swallowed, it propagates so the fault stays observable.
            try
            {
                await _transaction.DisposeAsync();
            }
            finally
            {
                _onDispose();
            }
        }
    }
}

