using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Window functions RANK/DENSE_RANK for ranking with ties.
/// RANK skips numbers after ties, DENSE_RANK does not.
/// </summary>
[ExampleMeta("E135", "Window - Rank/DenseRank", "Analytics",
    ExampleTier.Free, 135, "Window", "Rank", "DenseRank", "Ranking", RelatedApis = ["Win.Rank", "Win.DenseRank"])]
public class E135_WindowRank : ExampleBase
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
        // var sql = await windowQuery.ToSqlStringAsync(x => new { Rank = Win.Rank(), Dense = Win.DenseRank() });
        // Console.WriteLine(sql);

        var ranked = await windowQuery.SelectAsync(x => new
        {
            Name = x.Props.FirstName,
            Department = x.Props.Department,
            Salary = x.Props.Salary,
            Rank = Win.Rank(),
            DenseRank = Win.DenseRank()
        });

        sw.Stop();

        var sample = ranked.FirstOrDefault();
        return Ok("E135", "Window - Rank/DenseRank", ExampleTier.Free, sw.ElapsedMilliseconds, ranked.Count,
            [$"RANK() vs DENSE_RANK() by Salary", $"Top: {sample?.Name ?? "N/A"}, Rank: {sample?.Rank}, Dense: {sample?.DenseRank}"]);
    }
}
