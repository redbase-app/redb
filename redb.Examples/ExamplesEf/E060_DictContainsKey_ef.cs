using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E060_DictContainsKey"/>: PhoneDirectory has the key desk, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E060_ef", "EF: Dict ContainsKey - Phone Directory", "Query", ExampleTier.Free, 1, "EF")]
public class E060_DictContainsKey_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.EmployeeProps_PhoneDirectory.Any(d => d._key == "desk"))
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E060_ef", "EF: Dict ContainsKey - Phone Directory", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: PhoneDirectory.ContainsKey('desk')"]);
    }
}