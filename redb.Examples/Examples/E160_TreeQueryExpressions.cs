using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates arithmetic expressions in TreeQuery Where clause.
/// Finds tree nodes where 10% of budget exceeds $50,000.
/// Pro feature: server-side arithmetic in tree queries.
/// </summary>
[ExampleMeta("E160", "TreeQuery - Expressions", "Tree",
    ExampleTier.Pro, 160, "Tree", "Arithmetic", "Expression", RelatedApis = ["IRedbService.TreeQuery", "ITreeQueryable.Where"])]
public class E160_TreeQueryExpressions : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E160", "TreeQuery - Expressions", ExampleTier.Pro, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // Find departments where 10% of budget > $50,000 (i.e., Budget > $500,000)
        var query = redb.TreeQuery<DepartmentProps>(root.Id)
            .Where(d => d.Budget * 0.1m > 50000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();

        sw.Stop();

        var first = results.FirstOrDefault();
        var tenPercent = first?.Props.Budget * 0.1m ?? 0;

        return Ok("E160", "TreeQuery - Expressions", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"Filter: Budget * 0.1 > 50,000 (Budget > 500k)",
             $"Found: {results.Count} departments",
             $"First: {first?.Props.Name}, 10% = ${tenPercent:N0}"]);
    }
}
