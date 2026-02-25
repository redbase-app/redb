using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// LINQ queries on tree structures.
/// TreeQuery combines tree traversal with Where/OrderBy/Take.
/// </summary>
[ExampleMeta("E096", "Tree Query - LINQ on Trees", "Trees",
    ExampleTier.Free, 3, "Tree", "TreeQuery", "Where", "OrderBy", "Pro")]
public class E096_TreeQuery : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // TreeQuery: find all IT departments
        var itDepts = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Name.Contains("IT"))
            .OrderBy(d => d.Name)
            .ToListAsync();

        // var sql = await redb.TreeQuery<DepartmentProps>()
        //     .Where(d => d.Name.Contains("IT"))
        //     .ToSqlStringAsync();
        // Console.WriteLine(sql);

        sw.Stop();

        var names = itDepts.Select(d => $"{d.Name} ({d.Props.Code})").ToArray();

        return Ok("E096", "Tree Query - LINQ on Trees", ExampleTier.Free, sw.ElapsedMilliseconds, itDepts.Count,
        [
            "Filter: Name.Contains('IT')",
            $"Found: {string.Join(", ", names)}"
        ]);
    }
}
