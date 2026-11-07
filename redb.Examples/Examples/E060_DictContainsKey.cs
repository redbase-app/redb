using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees using Dictionary.ContainsKey.
/// Find employees who have a desk phone.
/// </summary>
[ExampleMeta("E060", "Dict ContainsKey - Phone Directory", "Query",
    ExampleTier.Free, 2, "Dictionary", "ContainsKey", "Pro")]
public class E060_DictContainsKey : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: Query dictionary with ContainsKey
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.PhoneDirectory!.ContainsKey("desk"))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E060", "Dict ContainsKey - Phone Directory", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: PhoneDirectory.ContainsKey('desk')"]);
    }
}
