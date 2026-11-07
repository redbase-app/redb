using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Window function SUM() OVER for running totals.
/// Calculates cumulative salary within each department.
/// </summary>
[ExampleMeta("E133", "Window - Running Sum", "Analytics",
    ExampleTier.Free, 133, "Window", "Sum", "Running", "Cumulative", RelatedApis = ["IRedbQueryable.WithWindow", "Win.Sum"])]
public class E133_WindowRunningSum : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var windowQuery = redb.Query<EmployeeProps>()
            .Take(50)
            .WithWindow(w => w
                .PartitionBy(x => x.Department)
                .OrderBy(x => x.HireDate));

        // Uncomment to see generated SQL:
        // var sql = await windowQuery.ToSqlStringAsync(x => new { x.Props.Salary, Total = Win.Sum(x.Props.Salary) });
        // Console.WriteLine(sql);

        var running = await windowQuery.SelectAsync(x => new
        {
            Name = x.Props.FirstName,
            Department = x.Props.Department,
            Salary = x.Props.Salary,
            RunningTotal = Win.Sum(x.Props.Salary)
        });

        sw.Stop();

        var sample = running.FirstOrDefault();
        return Ok("E133", "Window - Running Sum", ExampleTier.Free, sw.ElapsedMilliseconds, running.Count,
            [$"SUM(Salary) OVER (PARTITION BY Dept ORDER BY HireDate)", $"Sample: {sample?.Name ?? "N/A"}, Running: {sample?.RunningTotal ?? 0:N0}"]);
    }
}
