using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Groups objects by elements of an array field using GroupByArray.
/// Expands arrays to create one group per element (e.g., group by each Contact.Type).
/// </summary>
[ExampleMeta("E142", "GroupByArray - Array Elements", "Analytics",
    ExampleTier.Free, 142, "GroupByArray", "Array", "Expand", RelatedApis = ["IRedbQueryable.GroupByArray"])]
public class E142_GroupByArray : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Group by each Contact.Type (email, phone, etc.)
        // This expands the Contacts[] array and groups by contact type
        var byContactType = await redb.Query<EmployeeProps>()
            .Where(e => e.Contacts != null)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new
            {
                ContactType = g.Key,
                EmployeeCount = Agg.Count(g)
            });

        // Uncomment to see generated SQL:
        // var sql = await redb.Query<EmployeeProps>()
        //     .GroupByArray(e => e.Contacts!, c => c.Type)
        //     .ToSqlStringAsync(g => new { g.Key, Agg.Count(g) });
        // Console.WriteLine(sql);

        sw.Stop();

        var output = byContactType.OrderByDescending(g => g.EmployeeCount)
            .Select(g => $"{g.ContactType}: {g.EmployeeCount} employees").ToArray();

        return Ok("E142", "GroupByArray - Array Elements", ExampleTier.Free, sw.ElapsedMilliseconds, byContactType.Count,
            output.Prepend($"Groups by Contact.Type: {byContactType.Count}").ToArray());
    }
}
