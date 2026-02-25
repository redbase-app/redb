using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// PartitionByRedb in Window Functions - partition by base IRedbObject field.
/// SQL: PARTITION BY o._id_scheme (NO JOIN for partition key!)
/// </summary>
[ExampleMeta("E147", "Window - PartitionByRedb", "Analytics",
    ExampleTier.Free, 147, "PartitionByRedb", "Window", "NoJoin", RelatedApis = ["IWindowBuilder.PartitionByRedb", "Win.RowNumber"])]
public class E147_WindowPartitionByRedb : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Window with PartitionByRedb (base field) + OrderByDesc (Props field)
        var ranked = await redb.Query<EmployeeProps>()
            .Take(50)
            .WithWindow(w => w
                .PartitionByRedb(x => x.SchemeId)  // Base field - NO JOIN!
                .OrderByDesc(x => x.Salary))       // Props field
            .SelectAsync(x => new
            {
                x.Name,
                SchemeId = x.SchemeId,
                Salary = x.Props.Salary,
                RankInScheme = Win.RowNumber()
            });

        sw.Stop();

        var first = ranked.FirstOrDefault();

        return Ok("E147", "Window - PartitionByRedb", ExampleTier.Free, sw.ElapsedMilliseconds, ranked.Count,
            [$"PARTITION BY o._id_scheme (NO JOIN!)",
             $"Ranked: {ranked.Count} employees",
             first != null ? $"Top: #{first.RankInScheme} {first.Name} ${first.Salary:N0}" : "No data"]);
    }
}
