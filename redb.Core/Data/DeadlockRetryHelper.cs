namespace redb.Core.Data;

/// <summary>
/// Retries an async operation on SQL deadlock (MsSql error 1205, Postgres state 40P01).
/// Cluster-safe: each connection independently detects and retries.
/// </summary>
public static class DeadlockRetryHelper
{
    private const int DefaultMaxRetries = 3;
    private const int DefaultBaseDelayMs = 50;

    public static Task ExecuteWithRetryAsync(Func<Task> operation, int maxRetries = DefaultMaxRetries, int baseDelayMs = DefaultBaseDelayMs, System.Threading.CancellationToken cancellationToken = default)
    {
        return ExecuteWithRetryInternalAsync(operation, maxRetries, baseDelayMs, cancellationToken);
    }

    public static Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, int maxRetries = DefaultMaxRetries, int baseDelayMs = DefaultBaseDelayMs, System.Threading.CancellationToken cancellationToken = default)
    {
        return ExecuteWithRetryInternalAsync(operation, maxRetries, baseDelayMs, cancellationToken);
    }

    private static async Task ExecuteWithRetryInternalAsync(Func<Task> operation, int maxRetries, int baseDelayMs, System.Threading.CancellationToken cancellationToken)
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
            catch (Exception ex) when (attempt < maxRetries && IsDeadlock(ex))
            {
                var delay = baseDelayMs * (1 << attempt);
                var jitter = Random.Shared.Next(delay);
                await Task.Delay(delay + jitter, cancellationToken);
            }
        }
    }

    private static async Task<T> ExecuteWithRetryInternalAsync<T>(Func<Task<T>> operation, int maxRetries, int baseDelayMs, System.Threading.CancellationToken cancellationToken)
    {
        for (int attempt = 0; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await operation();
            }
            catch (Exception ex) when (attempt < maxRetries && IsDeadlock(ex))
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
}
