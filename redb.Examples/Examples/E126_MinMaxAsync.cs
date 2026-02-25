using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Calculates min and max of numeric fields using MinAsync/MaxAsync.
/// Server-side aggregation without loading objects.
/// </summary>
[ExampleMeta("E126", "MinAsync/MaxAsync - Range", "Analytics",
    ExampleTier.Free, 126, "Min", "Max", "Aggregation", "Analytics", RelatedApis = ["IRedbQueryable.MinAsync", "IRedbQueryable.MaxAsync"])]
public class E126_MinMaxAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>();

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var minSalary = await query.MinAsync(e => e.Salary);
        var maxSalary = await query.MaxAsync(e => e.Salary);

        sw.Stop();

        return Ok("E126", "MinAsync/MaxAsync - Range", ExampleTier.Free, sw.ElapsedMilliseconds, 2,
            [$"MIN(Salary) = {minSalary:N0}", $"MAX(Salary) = {maxSalary:N0}"]);
    }
}
