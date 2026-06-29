using Microsoft.Data.Sqlite;
using redb.Core.Data;
using System;
using System.Threading.Tasks;

namespace redb.SQLite.Data
{
    /// <summary>
    /// SQLite implementation of IRedbTransaction using Sqlite.
    /// Connection-scoped: parent connection automatically uses this transaction.
    /// </summary>
    public class SqliteRedbTransaction : IRedbTransaction
    {
        private readonly SqliteTransaction _transaction;
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
        /// Internal Sqlite transaction (for command assignment).
        /// </summary>
        internal SqliteTransaction SqliteTransaction => _transaction;

        /// <summary>
        /// Create new transaction wrapper.
        /// </summary>
        /// <param name="transaction">Underlying Sqlite transaction.</param>
        /// <param name="onDispose">Callback when transaction is disposed.</param>
        public SqliteRedbTransaction(SqliteTransaction transaction, Action onDispose)
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
                // Commit failed: the underlying sqlite3 handle may still have autocommit=0.
                // Speculatively rollback so the pooled SqliteConnection returns clean.
                // Both errors are visible via [Diag-TX-LIFECYCLE] so the source is traceable.
                // Console.WriteLine($"[Diag-TX-LIFECYCLE] CommitAsync FAILED: {ex.GetType().Name}: {ex.Message}. Attempting speculative rollback.");
                try { await _transaction.RollbackAsync(); }
                catch (Exception rbEx)
                {
                    // Console.WriteLine($"[Diag-TX-LIFECYCLE] Speculative rollback after failed commit ALSO FAILED: {rbEx.GetType().Name}: {rbEx.Message}. Pool may receive a dirty handle (mitigated by SqliteDataSource.EnsureCleanTransactionState on next acquire).");
                }
                IsActive = false;
                _onDispose();
                throw;
            }
            IsActive = false;
            // Clear the connection's `_currentTransaction` slot now — any
            // command issued between commit and dispose must NOT bind to this
            // (now closed) SqliteTransaction. DisposeAsync is idempotent on
            // _onDispose (the callback just nulls a field).
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
                // Console.WriteLine($"[Diag-TX-LIFECYCLE] RollbackAsync FAILED: {ex.GetType().Name}: {ex.Message}. Pool may receive a dirty handle (mitigated by SqliteDataSource.EnsureCleanTransactionState on next acquire).");
                IsActive = false;
                _onDispose();
                throw;
            }
            IsActive = false;
            _onDispose();
        }

        /// <summary>
        /// Dispose transaction.
        /// If still active - rollback automatically.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;

            // Auto-rollback if still active. The catch CANNOT throw (Dispose contract),
            // so leaks here are mitigated by SqliteDataSource.EnsureCleanTransactionState
            // which runs a speculative ROLLBACK on every pooled connection acquire.
            if (IsActive)
            {
                try
                {
                    await _transaction.RollbackAsync();
                }
                catch (Exception ex)
                {
                    // Console.WriteLine($"[Diag-TX-LIFECYCLE] DisposeAsync auto-rollback FAILED: {ex.GetType().Name}: {ex.Message}. Pool poisoning mitigated by SqliteDataSource.EnsureCleanTransactionState on next acquire.");
                }
                IsActive = false;
            }

            await _transaction.DisposeAsync();
            _onDispose();
        }
    }
}

