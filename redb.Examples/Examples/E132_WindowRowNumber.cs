using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Window function ROW_NUMBER() with PARTITION BY.
/// Ranks employees within each department by salary.
/// </summary>
[ExampleMeta("E132", "Window - RowNumber", "Analytics",
    ExampleTier.Free, 132, "Window", "RowNumber", "Rank", "Partition", RelatedApis = ["IRedbQueryable.WithWindow", "Win.RowNumber"])]
public class E132_WindowRowNumber : ExampleBase
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
        // var sql = await windowQuery.ToSqlStringAsync(x => new { x.Name, Rank = Win.RowNumber() });
        // Console.WriteLine(sql);

        var ranked = await windowQuery.SelectAsync(x => new
        {
            Name = x.Props.FirstName,
            Department = x.Props.Department,
            Salary = x.Props.Salary,
            Rank = Win.RowNumber()
        });

        sw.Stop();

        var top = ranked.FirstOrDefault();
        return Ok("E132", "Window - RowNumber", ExampleTier.Free, sw.ElapsedMilliseconds, ranked.Count,
            [$"ROW_NUMBER() PARTITION BY Department", $"#1 in {top?.Department ?? "N/A"}: {top?.Name ?? "N/A"}"]);
    }
}
