using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// GroupBy by a ListItem-typed property.
/// Probes 3 flavors:
///   (1) GroupBy(p => p.Status!.Id)    - key as long
///   (2) GroupBy(p => p.Status!.Value) - key as string
///   (3) GroupBy(p => p.Status)        - the bare ListItem class as key
/// Pro/OSS parity check for grouping over RedbListItem fields.
/// Requires E114-E118 to run first.
/// </summary>
[ExampleMeta("E130", "GroupBy - by ListItem (Id/Value/Bare)", "Analytics",
    ExampleTier.Free, 130, "GroupBy", "ListItem", "Aggregation", "Analytics",
    RelatedApis = ["IRedbQueryable.GroupBy", "RedbListItem"])]
public class E130_GroupByListItem : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();
        var notes = new List<string>();

        // (1) GroupBy by Status.Id (long key)
        try
        {
            var q1 = redb.Query<PersonProps>().GroupBy(p => p.Status!.Id);
            var sql1 = await q1.ToSqlStringAsync(g => new
            {
                StatusId = g.Key,
                Count = Agg.Count(g),
                AvgAge = Agg.Average(g, x => x.Age)
            });
            Console.WriteLine("[E130-SQL-1]\n" + sql1);
            var byId = await q1.SelectAsync(g => new
                {
                    StatusId = g.Key,
                    Count = Agg.Count(g),
                    AvgAge = Agg.Average(g, x => x.Age)
                });
            notes.Add($"(1) by Status.Id: groups={byId.Count}; first=Id:{byId.FirstOrDefault()?.StatusId}, cnt={byId.FirstOrDefault()?.Count}");
        }
        catch (Exception ex)
        {
            notes.Add($"(1) by Status.Id: FAIL {ex.GetType().Name}: {ex.Message.Split('\n')[0]}");
            Console.WriteLine("[E130-1] " + ex);
        }

        // (2) GroupBy by Status.Value (string key)
        try
        {
            var q2 = redb.Query<PersonProps>().GroupBy(p => p.Status!.Value);
            var sql2 = await q2.ToSqlStringAsync(g => new
            {
                StatusValue = g.Key,
                Count = Agg.Count(g)
            });
            Console.WriteLine("[E130-SQL-2]\n" + sql2);
            var byValue = await q2.SelectAsync(g => new
                {
                    StatusValue = g.Key,
                    Count = Agg.Count(g)
                });
            notes.Add($"(2) by Status.Value: groups={byValue.Count}; first=\"{byValue.FirstOrDefault()?.StatusValue}\", cnt={byValue.FirstOrDefault()?.Count}");
        }
        catch (Exception ex)
        {
            notes.Add($"(2) by Status.Value: FAIL {ex.GetType().Name}: {ex.Message.Split('\n')[0]}");
            Console.WriteLine("[E130-2] " + ex);
        }

        // (3) GroupBy by bare Status (RedbListItem class itself)
        try
        {
            var q3 = redb.Query<PersonProps>().GroupBy(p => p.Status);
            var sql3 = await q3.ToSqlStringAsync(g => new
            {
                Status = g.Key,
                Count = Agg.Count(g)
            });
            Console.WriteLine("[E130-SQL-3]\n" + sql3);
            var byBare = await q3.SelectAsync(g => new
                {
                    Status = g.Key,
                    Count = Agg.Count(g)
                });
            var first = byBare.FirstOrDefault();
            var firstDesc = first?.Status is RedbListItem li
                ? $"Id:{li.Id}, Value:'{li.Value}'"
                : (first?.Status is null ? "null" : first.Status.ToString());
            notes.Add($"(3) by bare Status: groups={byBare.Count}; first={firstDesc}, cnt={first?.Count}");
        }
        catch (Exception ex)
        {
            notes.Add($"(3) by bare Status: FAIL {ex.GetType().Name}: {ex.Message.Split('\n')[0]}");
            Console.WriteLine("[E130-3] " + ex);
        }

        sw.Stop();
        foreach (var n in notes) Console.WriteLine("[E130-NOTE] " + n);
        return Ok("E130", "GroupBy - by ListItem (Id/Value/Bare)", ExampleTier.Free, sw.ElapsedMilliseconds, notes.Count, notes.ToArray());
    }
}
