using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Sort employees by multiple fields using ThenBy.
/// Sort by department, then by salary descending.
/// </summary>
[ExampleMeta("E081", "OrderBy + ThenBy", "Query",
    ExampleTier.Free, 1, "OrderBy", "ThenBy", "Sort")]
public class E081_OrderByThenBy : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .OrderBy(e => e.Department)
            .ThenByDescending(e => e.Salary)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        var first = result.FirstOrDefault();
        var info = first != null ? $"{first.Props.Department}: ${first.Props.Salary:N0}" : "-";

        return Ok("E081", "OrderBy + ThenBy", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Sort: Department ASC, Salary DESC", $"First: {info}"]);
    }
}
