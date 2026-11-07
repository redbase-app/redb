using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates combining DistinctBy and OrderByDescending on TreeQuery.
/// Gets unique values by one field, sorted by another.
/// Pro feature: chained tree query operations.
/// </summary>
[ExampleMeta("E180", "TreeQuery - DistinctBy + OrderBy", "Tree",
    ExampleTier.Pro, 180, "Tree", "DistinctBy", "OrderBy", "Combined", RelatedApis = ["ITreeQueryable.DistinctBy", "ITreeQueryable.OrderByDescending"])]
public class E180_TreeDistinctByOrderBy : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E180", "TreeQuery - DistinctBy + OrderBy", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // Get unique by IsActive, then sort by Budget descending
        var query = redb.TreeQuery<DepartmentProps>(root.Id)
            .Where(d => d.Budget > 0)
            .DistinctBy(d => d.IsActive)
            .OrderByDescending(d => d.Budget)
            .Take(100);

        // Uncomment to see generated SQL:
        //var sql = query.ToSqlString();
        //Console.WriteLine(sql);

        var results = await query.ToListAsync();

        sw.Stop();

        var output = results.Select(d => 
            $"Active={d.Props.IsActive}, Budget=${d.Props.Budget:N0}");

        return Ok("E180", "TreeQuery - DistinctBy + OrderBy", ExampleTier.Pro, sw.ElapsedMilliseconds, results.Count,
            [$"DistinctBy(IsActive) + OrderByDescending(Budget)",
             $"Results: {results.Count}",
             string.Join("; ", output)]);
    }
}
