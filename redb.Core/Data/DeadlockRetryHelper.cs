namespace redb.Core.Data;

/// <summary>
/// Retries an async operation on SQL deadlock (MsSql error 1205, Postgres state 40P01).
/// Cluster-safe: each connection independently detects and retries.
/// </summary>
public static class DeadlockRetryHelper
{
    private const int DefaultMaxRetries = 3;
    private const int DefaultBaseDelayMs = 50;

    public static Task ExecuteWithRetryAsync(Func<Task> operation, int maxRetries = DefaultMaxRetries, int baseDelayMs = DefaultBaseDelayMs)
    {
        return ExecuteWithRetryInternalAsync(operation, maxRetries, baseDelayMs);
    }

    public static Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, int maxRetries = DefaultMaxRetries, int baseDelayMs = DefaultBaseDelayMs)
    {
        return ExecuteWithRetryInternalAsync(operation, maxRetries, baseDelayMs);
    }

    private static async Task ExecuteWithRetryInternalAsync(Func<Task> operation, int maxRetries, int baseDelayMs)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await operation();
                return;
            }
            catch (Exception ex) when (attempt < maxRetries && IsDeadlock(ex))
            {
                var delay = baseDelayMs * (1 << attempt);
                var jitter = Random.Shared.Next(delay);
                await Task.Delay(delay + jitter);
            }
        }
    }

    private static async Task<T> ExecuteWithRetryInternalAsync<T>(Func<Task<T>> operation, int maxRetries, int baseDelayMs)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (Exception ex) when (attempt < maxRetries && IsDeadlock(ex))
            {
                var delay = baseDelayMs * (1 << attempt);
                var jitter = Random.Shared.Next(delay);
                await Task.Delay(delay + jitter);
            }
        }
    }

    /// <summary>
    /// Walks the full exception chain to detect deadlock.
    /// MsSql: SqlException.Number == 1205
    /// Postgres: PostgresException.SqlState == "40P01"
    /// </summary>
    internal static bool IsDeadlock(Exception ex)
    {
        var current = ex;
        while (current != null)
        {
            var typeName = current.GetType().Name;
            if (typeName == "SqlException" && GetErrorNumber(current) == 1205)
                return true;
            if (typeName == "PostgresException" && GetSqlState(current) == "40P01")
                return true;
            current = current.InnerException;
        }

        return false;
    }

    // Reflection-based to avoid hard dependency on Npgsql/Microsoft.Data.SqlClient
    private static int GetErrorNumber(Exception ex)
        => (int)(ex.GetType().GetProperty("Number")?.GetValue(ex) ?? 0);

    private static string? GetSqlState(Exception ex)
        => ex.GetType().GetProperty("SqlState")?.GetValue(ex) as string;
}
