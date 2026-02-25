using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates FirstOrDefaultAsync to get a single object matching criteria.
/// Returns the first match or null if none found.
/// </summary>
[ExampleMeta("E183", "FirstOrDefaultAsync", "Query",
    ExampleTier.Free, 183, "First", "Single", "Query", RelatedApis = ["IRedbQueryable.FirstOrDefaultAsync"])]
public class E183_FirstOrDefaultAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Get first employee with salary > 80000
        var highEarner = await redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 80000m)
            .OrderByDescending(e => e.Salary)
            .FirstOrDefaultAsync();

        // Get first with impossible condition (should be null)
        var notFound = await redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 10_000_000m)
            .FirstOrDefaultAsync();

        sw.Stop();

        if (highEarner == null)
        {
            return Fail("E183", "FirstOrDefaultAsync", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No employees with salary > 80000 found.");
        }

        return Ok("E183", "FirstOrDefaultAsync", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"First high earner: {highEarner.Props.FirstName} {highEarner.Props.LastName}",
             $"Salary: ${highEarner.Props.Salary:N0}",
             $"Not found (null): {notFound == null}"]);
    }
}
