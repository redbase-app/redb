using System.Diagnostics;
using redb.Core;
using redb.Core.Query;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates nested Sql.Function calls for complex SQL expressions.
/// Uses SQRT(Salary) > 250 to find employees with Salary > 62,500.
/// Pro feature: nested arbitrary SQL function calls.
/// </summary>
[ExampleMeta("E163", "Sql.Function - Nested", "Query",
    ExampleTier.Pro, 163, "Sql.Function", "SQRT", "Nested", RelatedApis = ["Sql.Function"])]
public class E163_SqlFunctionNested : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // SQRT(Salary) > 250 means Salary > 62,500
        var query = redb.Query<EmployeeProps>()
            .Where(e => Sql.Function<double>("SQRT", e.Salary) > 250)
            .Take(100);

        // Uncomment to see generated SQL (contains SQRT):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => Sql.Function<double>("SQRT", e.Salary) > 250)
            .CountAsync();

        sw.Stop();

        var salaries = results.Take(5).Select(e => $"${e.Props.Salary:N0} (√={Math.Sqrt((double)e.Props.Salary):F0})");

        return Ok("E163", "Sql.Function - Nested", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"SQL: SQRT(Salary) > 250 (Salary > 62,500)",
             $"Found: {totalCount} employees",
             $"Salaries: {string.Join(", ", salaries)}"]);
    }
}
