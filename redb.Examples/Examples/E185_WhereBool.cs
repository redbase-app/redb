using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates filtering by conditions that can be simplified.
/// Shows different ways to filter including combined conditions.
/// </summary>
[ExampleMeta("E185", "Where - Combined Conditions", "Query",
    ExampleTier.Free, 185, "Where", "Combined", "Multiple", RelatedApis = ["IRedbQueryable.Where"])]
public class E185_WhereBool : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Combined conditions: Age > 30 AND Salary > 70000
        var expHighEarnersCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Age > 30 && e.Salary > 70000m)
            .CountAsync();

        var expHighEarners = await redb.Query<EmployeeProps>()
            .Where(e => e.Age > 30 && e.Salary > 70000m)
            .Take(5)
            .ToListAsync();

        // OR conditions: Department == "IT" OR Department == "Engineering"
        var techDeptCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Department == "IT" || e.Department == "Engineering")
            .CountAsync();

        sw.Stop();

        var names = expHighEarners.Take(3).Select(e => $"{e.Props.FirstName} ({e.Props.Age}, ${e.Props.Salary:N0})");

        return Ok("E185", "Where - Combined Conditions", ExampleTier.Free, sw.ElapsedMilliseconds, 
            expHighEarnersCount + techDeptCount,
            [$"Age > 30 AND Salary > 70000: {expHighEarnersCount}",
             $"IT OR Engineering: {techDeptCount}",
             $"Samples: {string.Join(", ", names)}"]);
    }
}
