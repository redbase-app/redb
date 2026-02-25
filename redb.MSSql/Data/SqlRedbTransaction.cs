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
        
        await _transaction.CommitAsync();
        IsActive = false;
    }

    /// <summary>
    /// Rollback all changes.
    /// </summary>
    public async Task RollbackAsync()
    {
        if (!IsActive)
            throw new InvalidOperationException("Transaction is not active. Already committed or rolled back.");
        
        await _transaction.RollbackAsync();
        IsActive = false;
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
        
        // Auto-rollback if still active
        if (IsActive)
        {
            try
            {
                await _transaction.RollbackAsync();
            }
            catch
            {
                // Ignore rollback errors during dispose
            }
            IsActive = false;
        }
        
        await _transaction.DisposeAsync();
        _onDispose();
    }
}

