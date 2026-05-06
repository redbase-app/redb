using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// GroupBy with composite key (multiple fields).
/// Group by Department AND Position for detailed breakdown.
/// </summary>
[ExampleMeta("E130", "GroupBy - Composite Key", "Analytics",
    ExampleTier.Free, 130, "GroupBy", "Composite", "MultiKey", "Analytics", RelatedApis = ["IRedbQueryable.GroupBy"])]
public class E130_GroupByMultiKey : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Uncomment to see generated SQL:
        // var sql = await redb.Query<EmployeeProps>()
        //     .GroupBy(x => new { x.Department, x.Position })
        //     .ToSqlStringAsync(g => new { g.Key.Department, g.Key.Position, Count = Agg.Count(g) });
        // Console.WriteLine(sql);

        var byDeptPosition = await redb.Query<EmployeeProps>()
            .GroupBy(x => new { x.Department, x.Position })
            .SelectAsync(g => new
            {
                g.Key.Department,
                g.Key.Position,
                TotalSalary = Agg.Sum(g, x => x.Salary),
                Count = Agg.Count(g)
            });

        sw.Stop();

        var first = byDeptPosition.FirstOrDefault();
        return Ok("E130", "GroupBy - Composite Key", ExampleTier.Free, sw.ElapsedMilliseconds, byDeptPosition.Count,
            [$"Groups: {byDeptPosition.Count}", $"First: {first?.Department ?? "N/A"}/{first?.Position ?? "N/A"}"]);
    }
}
