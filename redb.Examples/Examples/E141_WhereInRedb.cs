using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter by list of base field values.
/// WHERE o._id IN (...) without JOIN - very fast.
/// </summary>
[ExampleMeta("E141", "WhereInRedb - Base Field", "Analytics",
    ExampleTier.Free, 141, "WhereIn", "WhereInRedb", "Base", "Filter", RelatedApis = ["IRedbQueryable.WhereInRedb"])]
public class E141_WhereInRedb : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // First get some IDs to filter by
        var sampleIds = await redb.Query<EmployeeProps>()
            .Take(5)
            .Select(x => x.id)
            .ToListAsync();

        if (sampleIds.Count == 0)
        {
            sw.Stop();
            return Fail("E141", "WhereInRedb - Base Field", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No employees found. Run E000 first.");
        }

        var query = redb.Query<EmployeeProps>()
            .WhereInRedb(x => x.Id, sampleIds);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();

        sw.Stop();

        return Ok("E141", "WhereInRedb - Base Field", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"WHERE o._id IN ({string.Join(",", sampleIds.Take(3))}...)", $"Found: {results.Count} (NO JOIN!)"]);
    }
}
