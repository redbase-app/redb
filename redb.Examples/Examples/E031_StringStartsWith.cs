using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees using String.StartsWith.
/// Find employees whose first name starts with "John".
/// </summary>
[ExampleMeta("E031", "Where - String StartsWith", "Query",
    ExampleTier.Free, 1, "Where", "String", "StartsWith")]
public class E031_StringStartsWith : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.StartsWith("John"))
            .Take(100);

        // Uncomment to see generated SQL:
        //var sql = await query.ToSqlStringAsync();
        //Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E031", "Where - String StartsWith", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: FirstName.StartsWith('John')"]);
    }
}
