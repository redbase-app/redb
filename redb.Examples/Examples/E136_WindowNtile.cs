using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Window function NTILE for bucket distribution.
/// Divides employees into N equal groups (quartiles, deciles, etc.).
/// </summary>
[ExampleMeta("E136", "Window - Ntile", "Analytics",
    ExampleTier.Free, 136, "Window", "Ntile", "Quartile", "Bucket", RelatedApis = ["Win.Ntile"])]
public class E136_WindowNtile : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var windowQuery = redb.Query<EmployeeProps>()
            .Take(100)
            .WithWindow(w => w.OrderByDesc(x => x.Salary));

        // Uncomment to see generated SQL:
        // var sql = await windowQuery.ToSqlStringAsync(x => new { Quartile = Win.Ntile(4) });
        // Console.WriteLine(sql);

        // Divide into 4 quartiles by salary
        var quartiles = await windowQuery.SelectAsync(x => new
        {
            Name = x.Props.FirstName,
            Salary = x.Props.Salary,
            Quartile = Win.Ntile(4)
        });

        sw.Stop();

        var q1Count = quartiles.Count(x => x.Quartile == 1);
        var topEarner = quartiles.FirstOrDefault();
        return Ok("E136", "Window - Ntile", ExampleTier.Free, sw.ElapsedMilliseconds, quartiles.Count,
            [$"NTILE(4) - split into quartiles", $"Q1 (top 25%): {q1Count} employees"]);
    }
}
