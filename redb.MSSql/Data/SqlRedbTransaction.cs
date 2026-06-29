using Microsoft.Data.SqlClient;
using redb.Core.Data;

namespace redb.MSSql.Data;

/// <summary>
/// MS SQL Server implementation of IRedbTransaction using SqlTransaction.
/// Connection-scoped: parent connection automatically uses this transaction.
/// </summary>
public class SqlRedbTransaction : IRedbTransaction
{
    private readonly SqlTransaction _transaction;
    private readonly Action _onDispose;
    private bool _disposed;
    
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
    /// Internal SqlTransaction (for command assignment).
    /// </summary>
    internal SqlTransaction SqlTransaction => _transaction;

    /// <summary>
    /// Create new transaction wrapper.
    /// </summary>
    /// <param name="transaction">Underlying SqlTransaction.</param>
    /// <param name="onDispose">Callback when transaction is disposed.</param>
    public SqlRedbTransaction(SqlTransaction transaction, Action onDispose)
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
            // Commit failed: SqlConnection._currentTransaction may still be set.
            // Try speculative rollback so the connection returns to the pool clean;
            // otherwise the next caller's BeginTransaction fails with
            // "SqlConnection does not support parallel transactions".
            // Console.WriteLine($"[Diag-TX-LIFECYCLE-MSSQL] CommitAsync FAILED: {ex.GetType().Name}: {ex.Message}. Attempting speculative rollback.");
            try { await _transaction.RollbackAsync(); }
            catch (Exception rbEx)
            {
                // Console.WriteLine($"[Diag-TX-LIFECYCLE-MSSQL] Speculative rollback after failed commit ALSO FAILED: {rbEx.GetType().Name}: {rbEx.Message}. Pool may receive a dirty SqlConnection (next BeginTransaction will throw 'parallel transactions').");
            }
            IsActive = false;
            _onDispose();
            throw;
        }
        IsActive = false;
        // Clear the connection's `_currentTransaction` slot now so commands
        // issued between commit and dispose run against the autocommit
        // connection (Microsoft.Data.SqlClient is more tolerant of
        // cmd.Transaction=closedTx than Microsoft.Data.Sqlite, but the
        // semantics should not depend on driver tolerance).
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
            // Console.WriteLine($"[Diag-TX-LIFECYCLE-MSSQL] RollbackAsync FAILED: {ex.GetType().Name}: {ex.Message}. Pool may receive a dirty SqlConnection (next BeginTransaction will throw 'parallel transactions').");
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
    /// so a leaked SqlConnection (autocommit=off at the driver layer) is mitigated by
    /// SqlDataSource.EnsureCleanTransactionState's speculative ROLLBACK on next pool acquire.
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
                // Console.WriteLine($"[Diag-TX-LIFECYCLE-MSSQL] DisposeAsync auto-rollback FAILED: {ex.GetType().Name}: {ex.Message}. Pool poisoning mitigated by SqlDataSource.EnsureCleanTransactionState on next acquire.");
            }
            IsActive = false;
        }

        await _transaction.DisposeAsync();
        _onDispose();
    }
}

