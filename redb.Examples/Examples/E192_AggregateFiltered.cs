using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Multiple aggregations with Where filter applied before aggregation.
/// Filter employees by position, then calculate Sum, Avg, Min, Max.
/// Tests Pro filter handling in batch Aggregation.
/// </summary>
[ExampleMeta("E192", "AggregateAsync - With Filter", "Analytics",
    ExampleTier.Free, 192, "Aggregate", "Where", "Filter", "Sum", "Avg", RelatedApis = ["IRedbQueryable.AggregateAsync", "IRedbQueryable.Where"])]
public class E192_AggregateFiltered : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Aggregate only Developers
        var query = redb.Query<EmployeeProps>()
            .Where(x => x.Position == "Developer");

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var stats = await query.AggregateAsync(x => new
        {
            TotalSalary = Agg.Sum(x.Props.Salary),
            AvgSalary = Agg.Average(x.Props.Salary),
            MinSalary = Agg.Min(x.Props.Salary),
            MaxSalary = Agg.Max(x.Props.Salary),
            Count = Agg.Count()
        });

        sw.Stop();

        return Ok("E192", "AggregateAsync - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
        [
            $"Filter: Position = 'Developer'",
            $"Count: {stats?.Count ?? 0}",
            $"Salary: {stats?.MinSalary:N0} - {stats?.MaxSalary:N0} (Avg: {stats?.AvgSalary:N0})"
        ]);
    }
}
