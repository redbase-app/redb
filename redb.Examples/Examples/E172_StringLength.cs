using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates String.Length property in Where clause.
/// Server-side: LENGTH() in PostgreSQL, LEN() in MSSQL.
/// Pro feature: string property access in expressions.
/// </summary>
[ExampleMeta("E172", "String.Length Filter", "Query",
    ExampleTier.Free, 172, "String", "Length", "Property", RelatedApis = ["IRedbQueryable.Where", "String.Length"])]
public class E172_StringLength : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees with long last names (> 6 characters)
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.LastName.Length > 6)
            .Take(100);

        // Uncomment to see generated SQL (contains LENGTH or LEN):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.LastName.Length > 6)
            .CountAsync();

        sw.Stop();

        var names = results.Take(5)
            .Select(e => $"{e.Props.LastName} ({e.Props.LastName.Length} chars)");

        return Ok("E172", "String.Length Filter", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: LastName.Length > 6",
             $"Found: {totalCount} employees",
             $"Samples: {string.Join(", ", names)}"]);
    }
}
