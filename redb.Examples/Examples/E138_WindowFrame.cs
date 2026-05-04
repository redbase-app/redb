using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Window function with custom frame (sliding window).
/// Calculate moving average over last N rows.
/// </summary>
[ExampleMeta("E138", "Window - Frame (Sliding)", "Analytics",
    ExampleTier.Free, 138, "Window", "Frame", "Sliding", "Moving", RelatedApis = ["WindowBuilder.Frame", "Frame.Rows"])]
public class E138_WindowFrame : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var windowQuery = redb.Query<EmployeeProps>()
            .Take(50)
            .WithWindow(w => w
                .PartitionBy(x => x.Department)
                .OrderBy(x => x.HireDate)
                .Frame(Frame.Rows(3)));  // ROWS BETWEEN 3 PRECEDING AND CURRENT ROW

        // Uncomment to see generated SQL:
        // var sql = await windowQuery.ToSqlStringAsync(x => new { MovingSum = Win.Sum(x.Props.Salary) });
        // Console.WriteLine(sql);

        var movingAvg = await windowQuery.SelectAsync(x => new
        {
            Name = x.Props.FirstName,
            Salary = x.Props.Salary,
            MovingSum = Win.Sum(x.Props.Salary)  // Sum of last 4 rows (3 + current)
        });

        sw.Stop();

        var sample = movingAvg.Skip(3).FirstOrDefault(); // Skip first 3 (partial windows)
        return Ok("E138", "Window - Frame (Sliding)", ExampleTier.Free, sw.ElapsedMilliseconds, movingAvg.Count,
            [$"ROWS BETWEEN 3 PRECEDING AND CURRENT", $"Moving sum (4 rows): {sample?.MovingSum:N0}"]);
    }
}
