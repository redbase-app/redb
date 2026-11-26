using System.Diagnostics;
using redb.Core.Diagnostics;

namespace redb.Tests.Integration.Helpers;

/// <summary>
/// Counts the database commands an action runs: the spans of the redb.Core ActivitySource (one per command) under a
/// parent span of the caller only, so commands of tests running in parallel are not counted.
/// </summary>
public static class CommandCounter
{
    private const string ProbeName = "redb.Tests.CommandCount";
    private static readonly ActivitySource Probe = new(ProbeName);

    /// <summary>The database commands <paramref name="action"/> runs.</summary>
    public static Task<int> CountAsync(Func<Task> action) => CountAsync(action, excluded: null);

    /// <summary>
    /// The commands <paramref name="action"/> runs other than scalar side commands (<c>db.operation</c> = scalar): its
    /// data statements, whatever the provider reads them with, and any metadata it reads. The PostgreSQL statistics guard
    /// is such a side command: until the tables have planner statistics it checks them and sets enable_nestloop around a
    /// statement with scalar commands, on a recheck interval, so whether a counted call meets it depends on timing.
    /// </summary>
    public static Task<int> CountWithoutScalarsAsync(Func<Task> action) => CountAsync(action, excluded: RedbTelemetry.Scalar);

    private static async Task<int> CountAsync(Func<Task> action, string? excluded)
    {
        var spans = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == RedbActivitySource.SourceName || s.Name == ProbeName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a => { lock (spans) spans.Add(a); },
        };
        ActivitySource.AddActivityListener(listener);
        ActivityTraceId trace;
        using (var parent = Probe.StartActivity("count")!)
        {
            trace = parent.TraceId;
            await action();
        }
        lock (spans)
            return spans.Count(a => a.Source.Name == RedbActivitySource.SourceName && a.TraceId == trace
                                    && (excluded == null || (a.GetTagItem("db.operation") as string) != excluded));
    }
}
