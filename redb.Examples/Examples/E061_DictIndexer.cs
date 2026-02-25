using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees using Dictionary indexer.
/// Find employees with bonus over 6000 in 2023.
/// </summary>
[ExampleMeta("E061", "Dict Indexer - Bonus by Year", "Query",
    ExampleTier.Free, 2, "Dictionary", "Indexer", "Pro")]
public class E061_DictIndexer : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: Query dictionary with indexer
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.BonusByYear![2023] > 6000m)
            .Take(100);

        // Uncomment to see generated SQL:
        //var sql = await query.ToSqlStringAsync();
        //Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E061", "Dict Indexer - Bonus by Year", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: BonusByYear[2023] > 6000"]);
    }
}
