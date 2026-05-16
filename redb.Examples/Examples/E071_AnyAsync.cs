using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Check if any employees exist using AnyAsync().
/// Returns true if at least one record exists.
/// </summary>
[ExampleMeta("E071", "AnyAsync - Check Existence", "Query",
    ExampleTier.Free, 71, "Any", "Exists", RelatedApis = ["IRedbQueryable.AnyAsync"])]
public class E071_AnyAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Check if any employees exist (no predicate)
        var query = redb.Query<EmployeeProps>();
        
        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);
        
        var anyExist = await query.AnyAsync();

        sw.Stop();

        return Ok("E071", "AnyAsync - Check Existence", ExampleTier.Free, sw.ElapsedMilliseconds, anyExist ? 1 : 0,
            [$"Any employees exist: {anyExist}"]);
    }
}
