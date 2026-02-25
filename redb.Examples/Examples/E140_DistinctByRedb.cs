using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// DISTINCT ON specific base field.
/// Get one object per unique Name (first by Id).
/// </summary>
[ExampleMeta("E140", "DistinctByRedb - By Field", "Analytics",
    ExampleTier.Pro, 140, "Distinct", "DistinctByRedb", "Unique", RelatedApis = ["IRedbQueryable.DistinctByRedb"])]
public class E140_DistinctByRedb : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .DistinctByRedb(x => x.Name)
            .Take(100);

        // Uncomment to see generated SQL:
        //var sql = await query.ToSqlStringAsync();
        //Console.WriteLine(sql);

        var uniqueByName = await query.ToListAsync();

        sw.Stop();

        var names = string.Join(", ", uniqueByName.Take(3).Select(x => x.name));
        return Ok("E140", "DistinctByRedb - By Field", ExampleTier.Pro, sw.ElapsedMilliseconds, uniqueByName.Count,
            [$"DISTINCT ON (o._name)", $"First 3: {names}..."]);
    }
}
