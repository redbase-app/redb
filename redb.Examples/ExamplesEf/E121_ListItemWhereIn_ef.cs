using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E121_ListItemWhereIn"/>: persons whose status item value is one of three, whole.</summary>
[ExampleMeta("E121_ef", "EF: ListItem - WhereIn by Value", "List", ExampleTier.Free, 1, "EF", "List", "WhereIn")]
public class E121_ListItemWhereIn_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var targetValues = new[] { "Active", "Pending", "Blocked" };
        var results = (await db.Persons()
                .Where(p => targetValues.Contains(p.StatusNavigation!._value))
                .OrderBy(p => p._id)
                .Take(100)
                .ToListAsync())
            .Select(PersonEfMapping.ToBusiness).ToList();

        sw.Stop();

        if (results.Count == 0)
            return Ok("E121_ef", "EF: ListItem - WhereIn by Value", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                [$"Filter: Status.Value IN [{string.Join(", ", targetValues)}]", "No matching persons found"]);

        var statuses = results.Select(r => r.Props?.Status?.Value ?? "N/A").Distinct();
        return Ok("E121_ef", "EF: ListItem - WhereIn by Value", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"Filter: Status.Value IN [{string.Join(", ", targetValues)}]", $"Found statuses: {string.Join(", ", statuses)}"]);
    }
}
