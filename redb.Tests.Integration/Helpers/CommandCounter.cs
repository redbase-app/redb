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
    public static async Task<int> CountAsync(Func<Task> action)
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
            return spans.Count(a => a.Source.Name == RedbActivitySource.SourceName && a.TraceId == trace);
    }
}
