using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Window functions LAG/LEAD for previous/next values.
/// Access previous and next employee salary in sequence.
/// </summary>
[ExampleMeta("E134", "Window - Lag/Lead", "Analytics",
    ExampleTier.Free, 134, "Window", "Lag", "Lead", "Previous", "Next", RelatedApis = ["Win.Lag", "Win.Lead"])]
public class E134_WindowLagLead : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var windowQuery = redb.Query<EmployeeProps>()
            .Take(50)
            .WithWindow(w => w
                .PartitionBy(x => x.Department)
                .OrderBy(x => x.Salary));

        // Uncomment to see generated SQL:
        // var sql = await windowQuery.ToSqlStringAsync(x => new { Prev = Win.Lag(x.Props.Salary), Next = Win.Lead(x.Props.Salary) });
        // Console.WriteLine(sql);

        var withLagLead = await windowQuery.SelectAsync(x => new
        {
            Name = x.Props.FirstName,
            Salary = x.Props.Salary,
            PrevSalary = Win.Lag(x.Props.Salary),
            NextSalary = Win.Lead(x.Props.Salary)
        });

        sw.Stop();

        var sample = withLagLead.Skip(1).FirstOrDefault(); // Skip first (no prev)
        return Ok("E134", "Window - Lag/Lead", ExampleTier.Free, sw.ElapsedMilliseconds, withLagLead.Count,
            [$"LAG(Salary), LEAD(Salary)", $"Sample: {sample?.Salary:N0} (prev: {sample?.PrevSalary:N0}, next: {sample?.NextSalary:N0})"]);
    }
}
