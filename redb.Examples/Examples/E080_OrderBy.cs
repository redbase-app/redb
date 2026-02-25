using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Sort employees by salary using OrderBy.
/// Get top 5 lowest and highest earners.
/// </summary>
[ExampleMeta("E080", "OrderBy - Sort by Salary", "Query",
    ExampleTier.Free, 1, "OrderBy", "Sort")]
public class E080_OrderBy : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ascending - lowest earners
        var lowest = await redb.Query<EmployeeProps>()
            .OrderBy(e => e.Salary)
            .Take(5)
            .ToListAsync();

        // Descending - highest earners
        var highest = await redb.Query<EmployeeProps>()
            .OrderByDescending(e => e.Salary)
            .Take(5)
            .ToListAsync();

        sw.Stop();

        var lowestSalary = lowest.FirstOrDefault()?.Props.Salary ?? 0;
        var highestSalary = highest.FirstOrDefault()?.Props.Salary ?? 0;

        return Ok("E080", "OrderBy - Sort by Salary", ExampleTier.Free, sw.ElapsedMilliseconds, lowest.Count + highest.Count,
            [$"Lowest: ${lowestSalary:N0}, Highest: ${highestSalary:N0}"]);
    }
}
