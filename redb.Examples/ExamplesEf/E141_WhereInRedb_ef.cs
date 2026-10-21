using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E141_WhereInRedb"/>: five keys, then the employees with those keys as whole objects.</summary>
[ExampleMeta("E141_ef", "EF: WhereInRedb - Base Field", "Analytics", ExampleTier.Free, 1, "EF", "WhereIn")]
public class E141_WhereInRedb_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var sampleIds = await db.EmployeeProps.Select(e => e._id).Take(5).ToListAsync();
        if (sampleIds.Count == 0)
        {
            sw.Stop();
            return Fail("E141_ef", "EF: WhereInRedb - Base Field", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No employees found. Run E203_ef first.");
        }

        var results = await db.EmployeeProps.WithGraph()
            .Where(e => sampleIds.Contains(e._id))
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E141_ef", "EF: WhereInRedb - Base Field", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"WHERE _id IN ({string.Join(",", sampleIds.Take(3))}...)", $"Found: {results.Count}"]);
    }
}
