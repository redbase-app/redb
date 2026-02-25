using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// OrderByRedb in Window Functions - order by base IRedbObject field.
/// SQL: ORDER BY o._date_create (NO JOIN for order key!)
/// </summary>
[ExampleMeta("E148", "Window - OrderByRedb", "Analytics",
    ExampleTier.Free, 148, "OrderByRedb", "Window", "NoJoin", RelatedApis = ["IWindowBuilder.OrderByRedb", "Win.Count", "Frame.Rows"])]
public class E148_WindowOrderByRedb : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Window with OrderByRedb (base field) + Frame
        var ordered = await redb.Query<EmployeeProps>()
            .Take(50)
            .WithWindow(w => w
                .OrderByRedb(x => x.DateCreate)           // Base field - NO JOIN!
                .Frame(Frame.Rows().UnboundedPreceding()))
            .SelectAsync(x => new
            {
                x.Name,
                Created = x.DateCreate,
                Salary = x.Props.Salary,
                RunningCount = Win.Count()
            });

        sw.Stop();

        var last = ordered.LastOrDefault();

        return Ok("E148", "Window - OrderByRedb", ExampleTier.Free, sw.ElapsedMilliseconds, ordered.Count,
            [$"ORDER BY o._date_create (NO JOIN!)",
             $"Employees: {ordered.Count}",
             last != null ? $"Last: {last.Name} #{last.RunningCount}" : "No data"]);
    }
}
