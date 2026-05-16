using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Window function with Where filter applied before windowing.
/// Filter employees by salary threshold, then apply ROW_NUMBER.
/// Tests Pro filter handling in Window queries.
/// </summary>
[ExampleMeta("E191", "Window - With Filter", "Analytics",
    ExampleTier.Free, 191, "Window", "Where", "Filter", "RowNumber", RelatedApis = ["IRedbQueryable.WithWindow", "IRedbQueryable.Where", "Win.RowNumber"])]
public class E191_WindowFiltered : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Window with filter: only high earners (Salary > 70000)
        var windowQuery = redb.Query<EmployeeProps>()
            .Where(x => x.Salary > 70000m)
            .Take(20)
            .WithWindow(w => w
                .PartitionBy(x => x.Department)
                .OrderByDesc(x => x.Salary));

        // Uncomment to see generated SQL:
        //var sql = await windowQuery.ToSqlStringAsync(x => new { x.Props.FirstName, Rank = Win.RowNumber() });
        //Console.WriteLine(sql);

        var ranked = await windowQuery.SelectAsync(x => new
        {
            Name = x.Props.FirstName,
            Department = x.Props.Department,
            Salary = x.Props.Salary,
            Rank = Win.RowNumber()
        });

        sw.Stop();

        var top = ranked.FirstOrDefault();
        return Ok("E191", "Window - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, ranked.Count,
            [$"Filter: Salary > 70k", $"Results: {ranked.Count}", $"#1 in {top?.Department ?? "N/A"}: {top?.Name ?? "N/A"} ({top?.Salary:N0})"]);
    }
}
