using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees using triple AND condition.
/// Find experienced developers with high salary, highest paid first.
/// The run is split into phases so the cost of the query itself can be told apart
/// from the cost of materializing the objects it returns.
/// </summary>
[ExampleMeta("E015", "Where - Triple AND", "Query",
    ExampleTier.Free, 2, "Where", "AND", "Complex")]
public class E015_WhereTripleAnd : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Position != "" && e.Age >= 30 && e.Salary > 70000m)
            .OrderByDescending(e => e.Salary)
            .Take(100);

        // Phase 1: compile the query to SQL text (no database round trip for the query itself).
        var phase = Stopwatch.StartNew();
        var sql = await query.ToSqlStringAsync();
        var compileMs = phase.ElapsedMilliseconds;
        // Console.WriteLine(sql);

        // Phase 0: query and materialization in one call, before anything else has touched
        // these objects (nothing of them is in the Props cache yet).
        phase.Restart();
        var firstList = await query.ToListAsync();
        var firstListMs = phase.ElapsedMilliseconds;

        // Phase 2: the query alone - ids only, no Props.
        phase.Restart();
        var ids = await query.Select(x => x.Id).ToListAsync();
        var idsMs = phase.ElapsedMilliseconds;

        // Phase 3: materialization alone - load the same objects by id.
        phase.Restart();
        var loaded = await redb.LoadAsync(ids);
        var loadMs = phase.ElapsedMilliseconds;

        // Phase 4: the same single call again, now that the objects have been loaded once.
        phase.Restart();
        var result = await query.ToListAsync();
        var listMs = phase.ElapsedMilliseconds;

        // Phase 5: the same query on the aggregate form, in the same process and with the same
        // warm caches, so the two forms are compared like for like.
        var planner = redb.Configuration.EnablePvtPrefilter;
        long baseIdsMs, baseListMs;
        try
        {
            redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            await query.Select(x => x.Id).ToListAsync();   // first call on this form is not timed
            phase.Restart();
            await query.Select(x => x.Id).ToListAsync();
            baseIdsMs = phase.ElapsedMilliseconds;
            phase.Restart();
            await query.ToListAsync();
            baseListMs = phase.ElapsedMilliseconds;
        }
        finally
        {
            redb.UpdateConfiguration(c => c.EnablePvtPrefilter = planner);
        }

        // And once more on the planner's form, measured the same way as the aggregate one.
        phase.Restart();
        await query.Select(x => x.Id).ToListAsync();
        var idsAgainMs = phase.ElapsedMilliseconds;

        string[] phases =
        [
            $"ToSqlStringAsync:        {compileMs} ms",
            $"ToListAsync, first:      {firstListMs} ms ({firstList.Count} objects)",
            $"Select(Id).ToListAsync:  {idsMs} ms ({ids.Count} ids)",
            $"LoadAsync(ids):          {loadMs} ms ({loaded.Count} objects)",
            $"ToListAsync, again:      {listMs} ms ({result.Count} objects)",
            $"planner off, Select(Id): {baseIdsMs} ms",
            $"planner off, ToList:     {baseListMs} ms",
            $"planner on,  Select(Id): {idsAgainMs} ms"
        ];
        // The result table shows two lines per example, so the breakdown is printed here as well.
        foreach (var line in phases)
            Console.WriteLine("-- " + line);

        return Ok("E015", "Where - Triple AND", ExampleTier.Free, firstListMs, result.Count,
            [$"Filter: Position != '' AND Age >= 30 AND Salary > 70k, ORDER BY Salary DESC, top 100", .. phases]);
    }
}
