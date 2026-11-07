using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E140_DistinctByRedb"/>: up to 100 employees with distinct names, as whole objects.
/// EF has no DISTINCT ON; it groups by the name, keeps the smallest key of each group and loads by those keys.
/// </summary>
[ExampleMeta("E140_ef", "EF: DistinctByRedb - By Field", "Analytics", ExampleTier.Pro, 1, "EF", "Distinct")]
public class E140_DistinctByRedb_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var ids = await db.EmployeeProps
            .GroupBy(e => e.objects._name)
            .Select(g => g.Min(e => e._id))
            .OrderBy(id => id)
            .Take(100)
            .ToListAsync();

        var uniqueByName = await db.EmployeeProps.WithGraph()
            .Where(e => ids.Contains(e._id))
            .ToBusinessListAsync();

        sw.Stop();

        var names = string.Join(", ", uniqueByName.Take(3).Select(x => x.name));
        return Ok("E140_ef", "EF: DistinctByRedb - By Field", ExampleTier.Pro, sw.ElapsedMilliseconds, uniqueByName.Count,
            ["GROUP BY objects._name, then load by key", $"First 3: {names}..."]);
    }
}
