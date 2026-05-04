using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees using Dict with tuple key (Pro).
/// Find employees with specific performance review.
/// </summary>
[ExampleMeta("E063", "Dict - Tuple Key", "Query",
    ExampleTier.Free, 3, "Dictionary", "Tuple", "Key", "Pro")]
public class E063_DictTupleKey : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: Dictionary<(int Year, string Quarter), string> with tuple key
        // Tuple key must be in a variable for Expression Tree to work
        var reviewKey = (Year: 2024, Quarter: "Q1");

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.PerformanceReviews![reviewKey] == "Excellent")
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E063", "Dict - Tuple Key", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: PerformanceReviews[(2024, 'Q1')] == 'Excellent'"]);
    }
}
