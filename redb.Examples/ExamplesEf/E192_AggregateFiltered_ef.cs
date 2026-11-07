using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E192_AggregateFiltered"/>: five aggregates over the developers in one query.</summary>
[ExampleMeta("E192_ef", "EF: AggregateAsync - With Filter", "Analytics", ExampleTier.Free, 1, "EF", "Aggregate")]
public class E192_AggregateFiltered_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var stats = await db.EmployeeProps
            .Where(x => x.Position == "Developer" && x.EmployeeProps_HomeAddress!.City != "Tokyo")
            .Select(x => new
            {
                x.Salary,
                Floor = x.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor
            })
            .GroupBy(_ => 1)
            .Select(g => new
            {
                TotalSalary = g.Sum(x => x.Salary),
                AvgSalary = g.Average(x => x.Salary),
                MinSalary = g.Min(x => x.Salary),
                MaxSalary = g.Max(x => x.Salary),
                MinFloor = g.Min(x => x.Floor),
                MaxFloor = g.Max(x => x.Floor),
                AvgFloor = g.Average(x => x.Floor),
                Count = g.Count()
            })
            .FirstOrDefaultAsync();

        sw.Stop();

        return Ok("E192_ef", "EF: AggregateAsync - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
        [
            "Filter: Position = 'Developer' AND HomeAddress.City != 'Tokyo'",
            $"Count: {stats?.Count ?? 0}",
            $"Salary: {stats?.MinSalary:N0} - {stats?.MaxSalary:N0} (Avg: {stats?.AvgSalary:N0})",
            $"Home floor: {stats?.MinFloor} - {stats?.MaxFloor} (Avg: {stats?.AvgFloor:F2})"
        ]);
    }
}
