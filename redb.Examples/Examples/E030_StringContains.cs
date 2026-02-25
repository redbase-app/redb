using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees using String.Contains.
/// Find employees whose last name contains "Smith".
/// </summary>
[ExampleMeta("E030", "Where - String Contains", "Query",
    ExampleTier.Free, 1, "Where", "String", "Contains")]
public class E030_StringContains : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.LastName.Contains("Smith"))
            .Take(100);

        // Uncomment to see generated SQL:
        //var sql = await query.ToSqlStringAsync();
        //Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E030", "Where - String Contains", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: LastName.Contains('Smith')"]);
    }
}
