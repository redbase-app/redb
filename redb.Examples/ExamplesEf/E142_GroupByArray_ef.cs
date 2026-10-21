using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E142_GroupByArray"/>: the elements of Contacts grouped by their type.</summary>
[ExampleMeta("E142_ef", "EF: GroupByArray - Array Elements", "Analytics", ExampleTier.Free, 1, "EF", "GroupBy")]
public class E142_GroupByArray_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var byContactType = await db.EmployeeProps_Contacts
            .GroupBy(c => c.Type)
            .Select(g => new { ContactType = g.Key, EmployeeCount = g.Count() })
            .ToListAsync();

        sw.Stop();

        var output = byContactType.OrderByDescending(g => g.EmployeeCount)
            .Select(g => $"{g.ContactType}: {g.EmployeeCount} employees").ToArray();
        return Ok("E142_ef", "EF: GroupByArray - Array Elements", ExampleTier.Free, sw.ElapsedMilliseconds, byContactType.Count,
            output.Prepend($"Groups by Contact.Type: {byContactType.Count}").ToArray());
    }
}
