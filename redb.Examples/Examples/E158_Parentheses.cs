using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates parentheses for operation priority in Where clause.
/// Calculates monthly salary (annual / 12) and filters employees earning over $7,000/month.
/// Pro feature: server-side arithmetic with correct operator precedence.
/// </summary>
[ExampleMeta("E158", "Where - Parentheses Priority", "Query",
    ExampleTier.Pro, 158, "Arithmetic", "Parentheses", "Priority", RelatedApis = ["IRedbQueryable.Where"])]
public class E158_Parentheses : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees with monthly salary > $7,000
        // Parentheses ensure division happens first: (Salary / 12) > 7000
        var query = redb.Query<EmployeeProps>()
            .Where(e => (e.Salary / 12m) > 7000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => (e.Salary / 12m) > 7000m)
            .CountAsync();

        sw.Stop();

        var first = results.FirstOrDefault();
        var monthly = first?.Props.Salary / 12m ?? 0;

        return Ok("E158", "Where - Parentheses Priority", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: (Salary / 12) > 7,000 (monthly)",
             $"Found: {totalCount} employees",
             $"First: {first?.Props.FirstName}, Monthly: ${monthly:N0}"]);
    }
}
