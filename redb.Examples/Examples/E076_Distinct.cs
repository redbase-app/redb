using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Get unique objects by Props hash using Distinct().
/// Server-side: SELECT DISTINCT ON (_hash) - deduplicates by Props content.
/// </summary>
[ExampleMeta("E076", "Distinct - Unique by Props", "Query",
    ExampleTier.Free, 76, "Distinct", "Unique", "Dedupe", RelatedApis = ["IRedbQueryable.Distinct"])]
public class E076_Distinct : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Server-side Distinct by Props hash
        var query = redb.Query<EmployeeProps>()
            .Distinct()
            .Take(100);
        
        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);
        
        var uniqueEmployees = await query.ToListAsync();

        // Compare with total count
        var totalCount = await redb.Query<EmployeeProps>().CountAsync();

        sw.Stop();

        return Ok("E076", "Distinct - Unique by Props", ExampleTier.Free, sw.ElapsedMilliseconds, 
            uniqueEmployees.Count,
            [$"Unique loaded: {uniqueEmployees.Count}", $"Total in DB: {totalCount}"]);
    }
}
