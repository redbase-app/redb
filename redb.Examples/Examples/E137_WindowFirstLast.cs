using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Window functions FIRST_VALUE/LAST_VALUE.
/// Get first and last salary in partition for comparison.
/// </summary>
[ExampleMeta("E137", "Window - FirstValue/LastValue", "Analytics",
    ExampleTier.Free, 137, "Window", "FirstValue", "LastValue", RelatedApis = ["Win.FirstValue", "Win.LastValue"])]
public class E137_WindowFirstLast : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var windowQuery = redb.Query<EmployeeProps>()
            .Take(50)
            .WithWindow(w => w
                .PartitionBy(x => x.Department)
                .OrderByDesc(x => x.Salary));

        // Uncomment to see generated SQL:
        // var sql = await windowQuery.ToSqlStringAsync(x => new { Max = Win.FirstValue(x.Props.Salary), Min = Win.LastValue(x.Props.Salary) });
        // Console.WriteLine(sql);

        var withMinMax = await windowQuery.SelectAsync(x => new
        {
            Name = x.Props.FirstName,
            Department = x.Props.Department,
            Salary = x.Props.Salary,
            MaxInDept = Win.FirstValue(x.Props.Salary),  // First = highest (DESC order)
            MinInDept = Win.LastValue(x.Props.Salary)    // Last = lowest
        });

        sw.Stop();

        var sample = withMinMax.FirstOrDefault();
        return Ok("E137", "Window - FirstValue/LastValue", ExampleTier.Free, sw.ElapsedMilliseconds, withMinMax.Count,
            [$"FIRST_VALUE/LAST_VALUE in partition", $"{sample?.Department ?? "N/A"}: max={sample?.MaxInDept:N0}, min={sample?.MinInDept:N0}"]);
    }
}
