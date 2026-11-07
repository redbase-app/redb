using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E142_GroupByArray"/>: the metadata entries of the contacts of employees living
/// above the 10th floor, grouped by label. The depth costs EF joins from the metadata table through the contact, the
/// employee, its address and building. EF Core does not translate window functions (dotnet/efcore#12747), so the
/// RANK over the groups is computed in memory from the grouped rows.</summary>
[ExampleMeta("E142_ef", "EF: GroupByArray - Nested Array Elements", "Analytics", ExampleTier.Free, 1, "EF", "GroupBy")]
public class E142_GroupByArray_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var query = db.EmployeeProps_Contacts_Metadata
            .Where(m => m._owner._owner.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor > 10)
            .GroupBy(m => m.Label)
            .Select(g => new
            {
                Label = g.Key,
                Entries = g.Count(),
                CorporateDomain = g.Count(m => m.Value == "company.com")
            });

        var groups = await query.ToListAsync();
        var ranked = groups
            .Select(g => new { g.Label, g.Entries, g.CorporateDomain, Rank = 1 + groups.Count(o => o.Entries > g.Entries) })
            .ToList();

        sw.Stop();

        var output = ranked.OrderBy(g => g.Rank).ThenBy(g => g.Label)
            .Select(g => $"#{g.Rank} {g.Label}: {g.Entries} entries, {g.CorporateDomain} with value company.com")
            .Prepend($"Groups by Contacts[].Metadata[].Label, home floor > 10: {ranked.Count}")
            .Append("SQL:")
            .Concat(query.ToQueryString().Split('\n').Select(l => l.TrimEnd('\r')))
            .ToArray();

        return Ok("E142_ef", "EF: GroupByArray - Nested Array Elements", ExampleTier.Free, sw.ElapsedMilliseconds, ranked.Count, output);
    }
}
