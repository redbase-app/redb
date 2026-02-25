using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates filtering by nullable fields.
/// Finds objects where a nullable field is null or has value.
/// </summary>
[ExampleMeta("E184", "Where - Nullable Field", "Query",
    ExampleTier.Free, 184, "Where", "Nullable", "Null", "HasValue", RelatedApis = ["IRedbQueryable.Where"])]
public class E184_WhereNullable : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees where EmployeeCode is null
        var withoutCode = await redb.Query<EmployeeProps>()
            .Where(e => e.EmployeeCode == null)
            .Take(5)
            .ToListAsync();

        var nullCount = await redb.Query<EmployeeProps>()
            .Where(e => e.EmployeeCode == null)
            .CountAsync();

        // Find employees where EmployeeCode is NOT null
        var withCode = await redb.Query<EmployeeProps>()
            .Where(e => e.EmployeeCode != null)
            .Take(5)
            .ToListAsync();

        var notNullCount = await redb.Query<EmployeeProps>()
            .Where(e => e.EmployeeCode != null)
            .CountAsync();

        sw.Stop();

        var sampleCode = withCode.FirstOrDefault()?.Props.EmployeeCode ?? "N/A";

        return Ok("E184", "Where - Nullable Field", ExampleTier.Free, sw.ElapsedMilliseconds, nullCount + notNullCount,
            [$"EmployeeCode == null: {nullCount}",
             $"EmployeeCode != null: {notNullCount}",
             $"Sample code: {sampleCode}"]);
    }
}
