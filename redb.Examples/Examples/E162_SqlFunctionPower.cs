using System.Diagnostics;
using redb.Core;
using redb.Core.Query;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates Sql.Function for mathematical SQL functions.
/// Uses POWER to square the age and filter employees with Age^2 > 900 (Age > 30).
/// Pro feature: arbitrary SQL function calls via Sql.Function&lt;T&gt;().
/// </summary>
[ExampleMeta("E162", "Sql.Function - POWER", "Query",
    ExampleTier.Pro, 162, "Sql.Function", "POWER", "Math", RelatedApis = ["Sql.Function"])]
public class E162_SqlFunctionPower : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // POWER(Age, 2) > 900 means Age^2 > 900, so Age > 30
        var query = redb.Query<EmployeeProps>()
            .Where(e => Sql.Function<double>("POWER", e.Age, 2) > 900)
            .Take(100);

        // Uncomment to see generated SQL (contains POWER):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => Sql.Function<double>("POWER", e.Age, 2) > 900)
            .CountAsync();

        sw.Stop();

        var ages = results.Take(5).Select(e => $"{e.Props.Age} (^2={e.Props.Age * e.Props.Age})");

        return Ok("E162", "Sql.Function - POWER", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"SQL: POWER(Age, 2) > 900 (Age > 30)",
             $"Found: {totalCount} employees",
             $"Ages: {string.Join(", ", ages)}"]);
    }
}
