using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E184_WhereNullable"/>: EmployeeCode null and not null, five whole objects and a
/// count of each.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E184_ef", "EF: Where - Nullable Field", "Query", ExampleTier.Free, 1, "EF")]
public class E184_WhereNullable_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var withoutCode = await db.EmployeeProps.WithGraph().Where(e => e.EmployeeCode == null).OrderBy(e => e._id).Take(5).ToBusinessListAsync();
        var nullCount = await db.EmployeeProps.Where(e => e.EmployeeCode == null).CountAsync();
        var withCode = await db.EmployeeProps.WithGraph().Where(e => e.EmployeeCode != null).OrderBy(e => e._id).Take(5).ToBusinessListAsync();
        var notNullCount = await db.EmployeeProps.Where(e => e.EmployeeCode != null).CountAsync();

        sw.Stop();

        var sampleCode = withCode.FirstOrDefault()?.Props?.EmployeeCode ?? "N/A";
        return Ok("E184_ef", "EF: Where - Nullable Field", ExampleTier.Free, sw.ElapsedMilliseconds, nullCount + notNullCount,
            [$"EmployeeCode == null: {nullCount}",
             $"EmployeeCode != null: {notNullCount}",
             $"Sample code: {sampleCode}"]);
    }
}
