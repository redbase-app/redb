using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates chaining multiple string functions in Where clause.
/// Finds employees in departments with long uppercase names.
/// Pro feature: server-side chained UPPER() + LENGTH() functions.
/// </summary>
[ExampleMeta("E155", "Where - String Chain", "Query",
    ExampleTier.Pro, 155, "String", "ToUpper", "Length", "Chain", RelatedApis = ["IRedbQueryable.Where", "String.ToUpper", "String.Length"])]
public class E155_StringChain : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees in departments with names longer than 5 characters
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Department.ToUpper().Length > 5)
            .Take(100);

        // Uncomment to see generated SQL (contains UPPER + LENGTH):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Department.ToUpper().Length > 5)
            .CountAsync();

        sw.Stop();

        var depts = results.Select(e => e.Props.Department).Distinct().Take(5);

        return Ok("E155", "Where - String Chain", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: Department.ToUpper().Length > 5",
             $"Found: {totalCount} employees",
             $"Departments: {string.Join(", ", depts)}"]);
    }
}
