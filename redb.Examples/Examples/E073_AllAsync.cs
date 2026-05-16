using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Check if ALL employees match a condition using AllAsync(predicate).
/// Returns true only if every record satisfies the predicate.
/// </summary>
[ExampleMeta("E073", "AllAsync - Check All Match", "Query",
    ExampleTier.Free, 73, "All", "Predicate", "Validation", RelatedApis = ["IRedbQueryable.AllAsync"])]
public class E073_AllAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Check if all employees have non-negative salary
        var allNonNegativeSalary = await redb.Query<EmployeeProps>()
            .AllAsync(e => e.Salary >= 0);

        // Check if all high earners (>100k) are over 25 years old
        var allHighEarnersOver25 = await redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 100000m)
            .AllAsync(e => e.Age > 25);

        // Negative check: are ALL employees millionaires? (expect false)
        var allMillionaires = await redb.Query<EmployeeProps>()
            .AllAsync(e => e.Salary > 1_000_000m);

        sw.Stop();

        // Count passed validations
        var checks = new[] { allNonNegativeSalary, allHighEarnersOver25 };
        var passedCount = checks.Count(c => c);

        return Ok("E073", "AllAsync - Check All Match", ExampleTier.Free, sw.ElapsedMilliseconds, passedCount,
            [$"All Salary >= 0: {allNonNegativeSalary}",
             $"All high earners (>100k) over 25: {allHighEarnersOver25}",
             $"All millionaires (>1M): {allMillionaires} (expected false)"]);
    }
}
