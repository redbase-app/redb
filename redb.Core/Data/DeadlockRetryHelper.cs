namespace redb.Core.Data;

/// <summary>
/// Retries an async operation on SQL deadlock (MsSql error 1205, Postgres state 40P01).
/// Cluster-safe: each connection independently detects and retries.
/// <para>
/// The unit that owns the transaction retries it, and only that unit: the server has already rolled the deadlock
/// victim's transaction back, so a command run again inside it fails with a second error (PostgreSQL 25P02, an aborted
/// transaction on SQL Server) that replaces the deadlock. So the helper never retries inside an ambient
/// <see cref="System.Transactions.TransactionScope"/>, and the overloads taking an <see cref="IRedbContext"/> never retry
/// when that context is already in a transaction (an explicit one or <c>ExecuteAtomicAsync</c>'s) when the operation
/// starts. The original exception then leaves at once, for the owner of the transaction to run its unit again.
/// </para>
/// <para>
/// The operation is run again as a whole, so it must open its own transaction (or run as single statements) and be
/// safe to repeat: wrap the unit that owns the transaction, for example a whole <c>ExecuteAtomicAsync</c> call made
/// outside any transaction. After the last attempt the deadlock leaves unchanged.
/// </para>
/// </summary>
public static class DeadlockRetryHelper
{
    private const int DefaultMaxRetries = 3;
    private const int DefaultBaseDelayMs = 50;

    public static Task ExecuteWithRetryAsync(Func<Task> operation, int maxRetries = DefaultMaxRetries, int baseDelayMs = DefaultBaseDelayMs, System.Threading.CancellationToken cancellationToken = default)
    {
        return ExecuteWithRetryInternalAsync(operation, owned: true, maxRetries, baseDelayMs, cancellationToken);
    }

    public static Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, int maxRetries = DefaultMaxRetries, int baseDelayMs = DefaultBaseDelayMs, System.Threading.CancellationToken cancellationToken = default)
    {
        return ExecuteWithRetryInternalAsync(operation, owned: true, maxRetries, baseDelayMs, cancellationToken);
    }

    /// <summary>
    /// Retries <paramref name="operation"/> on a deadlock unless <paramref name="context"/> is in a transaction when it
    /// starts: then the transaction belongs to the caller, and the deadlock leaves at once.
    /// </summary>
    public static Task ExecuteWithRetryAsync(IRedbContext context, Func<Task> operation, int maxRetries = DefaultMaxRetries, int baseDelayMs = DefaultBaseDelayMs, System.Threading.CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        return ExecuteWithRetryInternalAsync(operation, owned: !context.IsInTransaction, maxRetries, baseDelayMs, cancellationToken);
    }

    /// <summary>
    /// Retries <paramref name="operation"/> on a deadlock unless <paramref name="context"/> is in a transaction when it
    /// starts: then the transaction belongs to the caller, and the deadlock leaves at once.
    /// </summary>
    public static Task<T> ExecuteWithRetryAsync<T>(IRedbContext context, Func<Task<T>> operation, int maxRetries = DefaultMaxRetries, int baseDelayMs = DefaultBaseDelayMs, System.Threading.CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        return ExecuteWithRetryInternalAsync(operation, owned: !context.IsInTransaction, maxRetries, baseDelayMs, cancellationToken);
    }

    private static async Task ExecuteWithRetryInternalAsync(Func<Task> operation, bool owned, int maxRetries, int baseDelayMs, System.Threading.CancellationToken cancellationToken)
    {
        for (int attempt = 0; ; attempt++)
        {
            // Cancellation checks before each attempt and inside the backoff delay (never
            // mid-transaction: the operation itself owns its rollback path).
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await operation();
                return;
            }
            catch (Exception ex) when (owned && attempt < maxRetries && IsDeadlock(ex) && !InAmbientTransaction())
            {
                var delay = baseDelayMs * (1 << attempt);
                var jitter = Random.Shared.Next(delay);
                await Task.Delay(delay + jitter, cancellationToken);
            }
        }
    }

    private static async Task<T> ExecuteWithRetryInternalAsync<T>(Func<Task<T>> operation, bool owned, int maxRetries, int baseDelayMs, System.Threading.CancellationToken cancellationToken)
    {
        for (int attempt = 0; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await operation();
            }
            catch (Exception ex) when (owned && attempt < maxRetries && IsDeadlock(ex) && !InAmbientTransaction())
            {
                var delay = baseDelayMs * (1 << attempt);
                var jitter = Random.Shared.Next(delay);
                await Task.Delay(delay + jitter, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Deadlock detection lives in <see cref="DbErrorClassifier"/> with every other code check;
    /// this is kept as the name the retry loop and its tests use.
    /// </summary>
    internal static bool IsDeadlock(Exception ex) => DbErrorClassifier.IsDeadlock(ex);

    // Read only after a deadlock, inside the exception filter: between TransactionScope.Complete() and Dispose() the
    // getter throws, and a throwing filter counts as "no match", so the original deadlock still leaves unretried.
    private static bool InAmbientTransaction() => System.Transactions.Transaction.Current != null;
}
