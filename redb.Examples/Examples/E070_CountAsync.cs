using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Count employees using CountAsync.
/// Get total count and filtered count.
/// </summary>
[ExampleMeta("E070", "CountAsync - Total and Filtered", "Query",
    ExampleTier.Free, 1, "Count", "Aggregate")]
public class E070_CountAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Total count
        var totalCount = await redb.Query<EmployeeProps>().CountAsync();

        // Filtered count
        var managersCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Position == "Manager")
            .CountAsync();

        sw.Stop();

        return Ok("E070", "CountAsync - Total and Filtered", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Total: {totalCount}, Managers: {managersCount}"]);
    }
}
