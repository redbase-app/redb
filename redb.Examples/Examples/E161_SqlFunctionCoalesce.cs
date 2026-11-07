using System.Diagnostics;
using redb.Core;
using redb.Core.Query;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates Sql.Function for calling custom SQL functions.
/// Uses COALESCE to safely handle potentially NULL values in comparisons.
/// Pro feature: arbitrary SQL function calls via Sql.Function&lt;T&gt;().
/// </summary>
[ExampleMeta("E161", "Sql.Function - COALESCE", "Query",
    ExampleTier.Pro, 161, "Sql.Function", "COALESCE", "NULL", RelatedApis = ["Sql.Function"])]
public class E161_SqlFunctionCoalesce : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // COALESCE returns first non-NULL argument
        // Useful for NULL-safe comparisons: COALESCE(Age, 0) > 25
        var query = redb.Query<EmployeeProps>()
            .Where(e => Sql.Function<int>("COALESCE", e.Age, 0) > 30)
            .Take(100);

        // Uncomment to see generated SQL (contains COALESCE):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => Sql.Function<int>("COALESCE", e.Age, 0) > 30)
            .CountAsync();

        sw.Stop();

        var ages = results.Take(5).Select(e => e.Props.Age);

        return Ok("E161", "Sql.Function - COALESCE", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"SQL: COALESCE(Age, 0) > 30",
             $"Found: {totalCount} employees",
             $"Ages: {string.Join(", ", ages)}"]);
    }
}
