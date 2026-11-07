using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace redb.Core.Diagnostics;

/// <summary>
/// Wraps one database command in an <see cref="Activity"/> on <see cref="RedbActivitySource.Source"/>.
/// The span carries the operation name and the database system only, never the statement text or the
/// parameters (they may hold secrets or personal data). A command that throws marks the span an error,
/// a caller's cancellation does not; either way the exception is rethrown unchanged. When nothing
/// subscribes, the caller runs the connection directly: no span, no extra allocation.
/// </summary>
internal static class RedbTelemetry
{
    /// <summary>Value of the <c>db.system</c> tag on every REDB span.</summary>
    public const string System = "redb";

    /// <summary>A read returning rows or scalars.</summary>
    public const string Query = "query";

    /// <summary>A single scalar read.</summary>
    public const string Scalar = "scalar";

    /// <summary>A write (INSERT, UPDATE, DELETE).</summary>
    public const string Execute = "execute";

    /// <summary>A raw call returning JSON.</summary>
    public const string Json = "json";

    /// <summary>Whether anything subscribes to the source; when false a wrapper is a no-op and skipped.</summary>
    public static bool Enabled => RedbActivitySource.Source.HasListeners();

    /// <summary>Runs <paramref name="action"/> inside a span named for <paramref name="operation"/>.</summary>
    public static async Task<T> RunAsync<T>(string operation, Func<Task<T>> action)
    {
        using var activity = Start(operation);
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Fail(activity, ex);
            throw;
        }
    }

    /// <summary>Runs <paramref name="action"/> inside a span named for <paramref name="operation"/>.</summary>
    public static async Task RunAsync(string operation, Func<Task> action)
    {
        using var activity = Start(operation);
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Fail(activity, ex);
            throw;
        }
    }

    private static Activity? Start(string operation)
    {
        var activity = RedbActivitySource.Source.StartActivity($"redb.{operation}", ActivityKind.Internal);
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("db.system", System);
            activity.SetTag("db.operation", operation);
        }
        return activity;
    }

    private static void Fail(Activity? activity, Exception ex)
    {
        if (activity is null)
            return;

        // The exception is always recorded; only the error status is withheld for a caller's
        // cancellation, which is not a failure of the database.
#if NET9_0_OR_GREATER
        activity.AddException(ex);
#else
        activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
        {
            { "exception.type", ex.GetType().FullName },
            { "exception.message", ex.Message },
            { "exception.stacktrace", ex.ToString() },
        }));
#endif
        if (ex is not OperationCanceledException)
            activity.SetStatus(ActivityStatusCode.Error, ex.Message);
    }
}
