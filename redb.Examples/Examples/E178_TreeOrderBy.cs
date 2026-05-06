using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates OrderBy on TreeQuery to sort tree nodes by a Props field.
/// Ascending order by department Code.
/// Pro feature: OrderBy with tree context.
/// </summary>
[ExampleMeta("E178", "TreeQuery - OrderBy", "Tree",
    ExampleTier.Free, 178, "Tree", "OrderBy", "Sort", "Ascending", RelatedApis = ["ITreeQueryable.OrderBy"])]
public class E178_TreeOrderBy : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E178", "TreeQuery - OrderBy", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // Sort departments by Code (ascending)
        var query = redb.TreeQuery<DepartmentProps>(root.Id)
            .OrderBy(d => d.Code)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = query.ToSqlString();
        // Console.WriteLine(sql);

        var sorted = await query.ToListAsync();

        sw.Stop();

        var codes = sorted.Take(5).Select(d => d.Props.Code);

        return Ok("E178", "TreeQuery - OrderBy", ExampleTier.Free, sw.ElapsedMilliseconds, sorted.Count,
            [$"OrderBy(Code) ascending",
             $"Loaded: {sorted.Count} departments",
             $"First codes: {string.Join(", ", codes)}"]);
    }
}
