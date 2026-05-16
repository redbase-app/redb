using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates DistinctByRedb on TreeQuery to get unique tree nodes by a base field.
/// Uses _objects table fields directly (no EAV join) - highly optimized.
/// Pro feature: DistinctByRedb with tree context.
/// </summary>
[ExampleMeta("E177", "TreeQuery - DistinctByRedb", "Tree",
    ExampleTier.Pro, 177, "Tree", "DistinctByRedb", "BaseField", RelatedApis = ["ITreeQueryable.DistinctByRedb"])]
public class E177_TreeDistinctByRedb : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E177", "TreeQuery - DistinctByRedb", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // Get total count before distinct
        var totalCount = await redb.TreeQuery<DepartmentProps>(root.Id).CountAsync();

        // Get unique departments by ParentId (base field) - shows unique parent branches
        var query = redb.TreeQuery<DepartmentProps>(root.Id)
            .DistinctByRedb(d => d.ParentId)
            .Take(20);

        // Uncomment to see generated SQL (no _values JOIN):
        // var sql = query.ToSqlString();
        // Console.WriteLine(sql);

        var uniqueByParent = await query.ToListAsync();

        sw.Stop();

        var parentIds = uniqueByParent.Take(5).Select(d => d.parent_id?.ToString() ?? "null");

        return Ok("E177", "TreeQuery - DistinctByRedb", ExampleTier.Pro, sw.ElapsedMilliseconds, uniqueByParent.Count,
            [$"DistinctByRedb(ParentId) on {totalCount} nodes",
             $"Unique parents: {uniqueByParent.Count}",
             $"Sample ParentIds: {string.Join(", ", parentIds)}"]);
    }
}
