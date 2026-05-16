using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees by a list of values using WhereIn.
/// Equivalent to SQL IN clause - much faster than multiple OR conditions.
/// </summary>
[ExampleMeta("E074", "WhereIn - Filter by List", "Query",
    ExampleTier.Free, 74, "WhereIn", "IN", "Filter", "List", RelatedApis = ["IRedbQueryable.WhereIn"])]
public class E074_WhereIn : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Filter by list of departments
        var targetDepartments = new[] { "Engineering", "Sales", "Marketing" };
        var query = redb.Query<EmployeeProps>()
            .WhereIn(e => e.Department, targetDepartments)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        // Execute query
        var results = await query.ToListAsync();
        
        // Count total matches (without loading all)
        var totalCount = await redb.Query<EmployeeProps>()
            .WhereIn(e => e.Department, targetDepartments)
            .CountAsync();

        sw.Stop();

        return Ok("E074", "WhereIn - Filter by List", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Loaded: {results.Count}, Total: {totalCount}", $"Depts: {string.Join(", ", targetDepartments)}"]);
    }
}
