using System;
using System.Threading.Tasks;

namespace redb.Core.Data
{
    /// <summary>
    /// Database transaction abstraction for REDB.
    /// Connection-scoped: all operations through parent connection
    /// automatically participate in this transaction.
    /// </summary>
    public interface IRedbTransaction : IAsyncDisposable
    {
        /// <summary>
        /// Transaction unique identifier (for diagnostics).
        /// </summary>
        Guid TransactionId { get; }
        
        /// <summary>
        /// Whether transaction is still active (not committed or rolled back).
        /// </summary>
        bool IsActive { get; }
        
        /// <summary>
        /// Transaction start time.
        /// </summary>
        DateTimeOffset StartedAt { get; }
        
        /// <summary>
        /// Commit all changes in transaction.
        /// After commit, transaction becomes inactive.
        /// </summary>
        Task CommitAsync();
        
        /// <summary>
        /// Rollback all changes in transaction.
        /// After rollback, transaction becomes inactive.
        /// </summary>
        Task RollbackAsync();
    }
}

