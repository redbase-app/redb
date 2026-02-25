using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Check if any employees match a condition using AnyAsync(predicate).
/// More efficient than Where().AnyAsync() - stops at first match.
/// </summary>
[ExampleMeta("E072", "AnyAsync - With Predicate", "Query",
    ExampleTier.Free, 72, "Any", "Predicate", "Filter", RelatedApis = ["IRedbQueryable.AnyAsync"])]
public class E072_AnyWithPredicate : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Check if any high earners exist (salary > 100k)
        var query = redb.Query<EmployeeProps>();
        
        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);
        
        var anyHighEarners = await query.AnyAsync(e => e.Salary > 100_000m);

        sw.Stop();

        return Ok("E072", "AnyAsync - With Predicate", ExampleTier.Free, sw.ElapsedMilliseconds, 
            anyHighEarners ? 1 : 0,
            [$"Any with Salary > 100k: {anyHighEarners}"]);
    }
}
