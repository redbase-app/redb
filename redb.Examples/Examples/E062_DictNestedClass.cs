using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees using Dict indexer with nested class (Pro).
/// Find employees whose HQ office is in specific city.
/// </summary>
[ExampleMeta("E062", "Dict - Nested Class Property", "Query",
    ExampleTier.Free, 3, "Dictionary", "Nested", "Class", "Pro")]
public class E062_DictNestedClass : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: Dictionary<string, Address> with nested property access
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.OfficeLocations!["HQ"].City == "New York")
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E062", "Dict - Nested Class Property", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: OfficeLocations['HQ'].City == 'New York'"]);
    }
}
