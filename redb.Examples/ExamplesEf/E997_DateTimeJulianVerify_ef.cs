using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E997_DateTimeJulianVerify"/>: three employees with far-apart hire dates, three
/// date ranges that must each return exactly one of them as a whole object, and the round trip of a date with a
/// time of day. Leftovers of an earlier run are removed first, inside the measurement as in redb.
/// </summary>
[ExampleMeta("E997_ef", "EF: DateTime REAL Julian Verify", "Query", ExampleTier.Free, 1, "EF", "DateTime")]
public class E997_DateTimeJulianVerify_ef : EfExampleBase
{
    private const string Dept = "JULIAN-VERIFY";

    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        var lines = new List<string>();

        await using (var db = await contexts.CreateDbContextAsync())
            await db.objects.Where(o => o.EmployeeProps!.Department == Dept).ExecuteDeleteAsync();

        var dPast = new DateTime(2018, 03, 15, 00, 00, 00, DateTimeKind.Utc);
        var dMiddle = new DateTime(2024, 06, 15, 13, 45, 30, DateTimeKind.Utc);
        var dFuture = new DateTime(2030, 09, 20, 00, 00, 00, DateTimeKind.Utc);

        await using (var db = await contexts.CreateDbContextAsync())
        {
            foreach (var (dt, code) in new[] { (dPast, "PAST"), (dMiddle, "MID"), (dFuture, "FUT") })
            {
                db.objects.Add(EmployeeEfMapping.ToEntity(new RedbObject<Biz.EmployeeProps>
                {
                    name = $"JulianVerify-{code}",
                    Props = new Biz.EmployeeProps
                    {
                        FirstName = code, LastName = "Verify", Age = 30,
                        Position = "QA", Department = Dept, Salary = 1m,
                        HireDate = dt, EmployeeCode = $"JV-{code}"
                    }
                }));
            }
            // One save for the three, as redb saves them as one batch.
            await db.SaveChangesAsync();
        }

        List<RedbObject<Biz.EmployeeProps>> midRange, futureOnly, pastOnly;
        await using (var db = await contexts.CreateDbContextAsync())
        {
            var from2023 = new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var to2026 = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var from2029 = new DateTime(2029, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var to2020 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            midRange = await db.EmployeeProps.WithGraph()
                .Where(e => e.Department == Dept && e.HireDate >= from2023 && e.HireDate < to2026)
                .ToBusinessListAsync();
            futureOnly = await db.EmployeeProps.WithGraph()
                .Where(e => e.Department == Dept && e.HireDate >= from2029)
                .ToBusinessListAsync();
            pastOnly = await db.EmployeeProps.WithGraph()
                .Where(e => e.Department == Dept && e.HireDate < to2020)
                .ToBusinessListAsync();
        }

        var failures = new List<string>();
        void Expect(string label, List<RedbObject<Biz.EmployeeProps>> got, string wantCode)
        {
            if (got.Count == 1 && got[0].Props.EmployeeCode == $"JV-{wantCode}")
                lines.Add($"OK  {label}: 1 row = {wantCode}");
            else
                failures.Add($"{label}: expected [1x{wantCode}], got [{string.Join(",", got.Select(g => g.Props.EmployeeCode))}]");
        }
        Expect("[2023,2026)", midRange, "MID");
        Expect(">=2029", futureOnly, "FUT");
        Expect("<2020", pastOnly, "PAST");

        if (midRange.Count == 1)
        {
            var rt = midRange[0].Props.HireDate.ToUniversalTime();
            var drift = Math.Abs((rt - dMiddle).TotalMilliseconds);
            if (drift <= 2)
                lines.Add($"OK  round-trip: {rt:yyyy-MM-ddTHH:mm:ss.fff}Z (drift {drift:0.###}ms)");
            else
                failures.Add($"round-trip drift {drift:0.###}ms: got {rt:O}, want {dMiddle:O}");
        }

        sw.Stop();

        if (failures.Count > 0)
            return Fail("E997_ef", "EF: DateTime REAL Julian Verify", ExampleTier.Free, sw.ElapsedMilliseconds,
                string.Join(" | ", failures));

        return Ok("E997_ef", "EF: DateTime REAL Julian Verify", ExampleTier.Free, sw.ElapsedMilliseconds,
            midRange.Count + futureOnly.Count + pastOnly.Count, lines.ToArray());
    }
}
