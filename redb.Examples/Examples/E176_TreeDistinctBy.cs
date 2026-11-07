using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates DistinctBy on TreeQuery to get unique tree nodes by a Props field.
/// Useful for removing duplicates based on specific property values.
/// Pro feature: DistinctBy with tree context.
/// </summary>
[ExampleMeta("E176", "TreeQuery - DistinctBy", "Tree",
    ExampleTier.Pro, 176, "Tree", "DistinctBy", "Unique", RelatedApis = ["ITreeQueryable.DistinctBy"])]
public class E176_TreeDistinctBy : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E176", "TreeQuery - DistinctBy", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // Get total count before distinct
        var totalCount = await redb.TreeQuery<DepartmentProps>(root.Id).CountAsync();

        // Get unique departments by IsActive status (will return max 2: true and false)
        var query = redb.TreeQuery<DepartmentProps>(root.Id)
            .DistinctBy(d => d.IsActive)
            .Take(100);

        // Uncomment to see generated SQL:
        //var sql = query.ToSqlString();
        //Console.WriteLine(sql);

        var uniqueByStatus = await query.ToListAsync();

        sw.Stop();

        var statuses = uniqueByStatus.Select(d => $"IsActive={d.Props.IsActive}");

        return Ok("E176", "TreeQuery - DistinctBy", ExampleTier.Pro, sw.ElapsedMilliseconds, uniqueByStatus.Count,
            [$"DistinctBy(IsActive) on {totalCount} nodes",
             $"Unique statuses: {uniqueByStatus.Count}",
             $"Values: {string.Join(", ", statuses)}"]);
    }
}
