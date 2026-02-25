using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// DISTINCT ON base fields (without Id) for finding duplicates.
/// Finds objects with identical Name, CodeInt etc. (potential duplicates).
/// </summary>
[ExampleMeta("E139", "DistinctRedb - Find Duplicates", "Analytics",
    ExampleTier.Pro, 139, "Distinct", "DistinctRedb", "Duplicates", RelatedApis = ["IRedbQueryable.DistinctRedb"])]
public class E139_DistinctRedb : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .DistinctRedb()
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var unique = await query.ToListAsync();

        sw.Stop();

        return Ok("E139", "DistinctRedb - Find Duplicates", ExampleTier.Pro, sw.ElapsedMilliseconds, unique.Count,
            [$"DISTINCT ON (base fields except _id)", $"Unique: {unique.Count} objects"]);
    }
}
