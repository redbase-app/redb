using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees using NOT equal condition.
/// Find all non-managers.
/// </summary>
[ExampleMeta("E013", "Where - NOT Equal", "Query",
    ExampleTier.Free, 1, "Where", "Query", "NOT")]
public class E013_WhereNot : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Position != "Manager")
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E013", "Where - NOT Equal", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Position != 'Manager'"]);
    }
}
