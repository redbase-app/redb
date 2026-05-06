using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates GroupBy aggregation on TreeQuery.
/// Groups tree nodes and calculates aggregates (SUM, COUNT) within tree context.
/// Pro feature: TreeQuery + GroupBy for hierarchical data analysis.
/// </summary>
[ExampleMeta("E173", "TreeQuery - GroupBy", "Tree",
    ExampleTier.Free, 173, "Tree", "GroupBy", "Aggregation", RelatedApis = ["ITreeQueryable.GroupBy", "Agg.Sum", "Agg.Count"])]
public class E173_TreeQueryGroupBy : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E173", "TreeQuery - GroupBy", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // Group departments by IsActive status and aggregate budget
        var groupedQuery = redb.TreeQuery<DepartmentProps>(root.Id)
            .Where(d => d.Budget > 0)
            .GroupBy(d => d.IsActive);

        // Uncomment to see generated SQL:
        // var sql = await groupedQuery.ToSqlStringAsync(g => new { 
        //     IsActive = g.Key, 
        //     TotalBudget = Agg.Sum(g, d => d.Budget),
        //     Count = Agg.Count(g)
        // });
        // Console.WriteLine(sql);

        var results = await groupedQuery.SelectAsync(g => new
        {
            IsActive = g.Key,
            TotalBudget = Agg.Sum(g, d => d.Budget),
            DeptCount = Agg.Count(g)
        });

        sw.Stop();

        var output = results.Select(r => 
            $"Active={r.IsActive}: {r.DeptCount} depts, Budget=${r.TotalBudget:N0}").ToArray();

        return Ok("E173", "TreeQuery - GroupBy", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            output.Prepend($"Groups: {results.Count}").ToArray());
    }
}
