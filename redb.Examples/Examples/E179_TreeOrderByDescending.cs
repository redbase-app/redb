using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates OrderByDescending on TreeQuery to sort tree nodes in reverse order.
/// Descending order by Budget - highest budgets first.
/// Pro feature: OrderByDescending with tree context.
/// </summary>
[ExampleMeta("E179", "TreeQuery - OrderByDescending", "Tree",
    ExampleTier.Free, 179, "Tree", "OrderByDescending", "Sort", "Descending", RelatedApis = ["ITreeQueryable.OrderByDescending"])]
public class E179_TreeOrderByDescending : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E179", "TreeQuery - OrderByDescending", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // Sort departments by Budget (descending) - highest first
        var query = redb.TreeQuery<DepartmentProps>(root.Id)
            .Where(d => d.Budget > 0)
            .OrderByDescending(d => d.Budget)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = query.ToSqlString();
        // Console.WriteLine(sql);

        var sorted = await query.ToListAsync();

        sw.Stop();

        var topBudgets = sorted.Take(5).Select(d => $"{d.Props.Name}: ${d.Props.Budget:N0}");

        return Ok("E179", "TreeQuery - OrderByDescending", ExampleTier.Free, sw.ElapsedMilliseconds, sorted.Count,
            [$"OrderByDescending(Budget) - highest first",
             $"Loaded: {sorted.Count} departments",
             $"Top: {string.Join(", ", topBudgets)}"]);
    }
}
