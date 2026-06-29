using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Regression probe for SQLite REAL Julian-day datetime storage.
/// Seeds 3 employees with distinct, far-apart HireDates (incl. a fractional time
/// of day), then asserts that server-side range filters select EXACTLY the right
/// rows and that a saved datetime round-trips. A broken comparison (the old TEXT
/// lexical bug) would make ranges always-true/false and fail these assertions.
/// </summary>
[ExampleMeta("E997", "DateTime REAL Julian Verify", "Query",
    ExampleTier.Free, 1, "Where", "DateTime", "Regression")]
public class E997_DateTimeJulianVerify : ExampleBase
{
    private const string Dept = "JULIAN-VERIFY";

    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();
        var lines = new List<string>();

        // Idempotent: purge any leftovers from a previous run so the "exactly 1 per range"
        // asserts hold whether this runs once or as part of the full suite.
        foreach (var stale in await redb.Query<EmployeeProps>().Where(e => e.Department == Dept).ToListAsync())
            await redb.DeleteAsync(stale);

        // Distinct UTC instants, years apart; the 2024 one carries a fractional
        // time of day to exercise the non-integer part of the Julian day.
        var dPast   = new DateTime(2018, 03, 15, 00, 00, 00, DateTimeKind.Utc);
        var dMiddle = new DateTime(2024, 06, 15, 13, 45, 30, DateTimeKind.Utc);
        var dFuture = new DateTime(2030, 09, 20, 00, 00, 00, DateTimeKind.Utc);

        foreach (var (dt, code) in new[] { (dPast, "PAST"), (dMiddle, "MID"), (dFuture, "FUT") })
        {
            await redb.SaveAsync(new RedbObject<EmployeeProps>
            {
                name = $"JulianVerify-{code}",
                Props = new EmployeeProps
                {
                    FirstName = code, LastName = "Verify", Age = 30,
                    Position = "QA", Department = Dept, Salary = 1m,
                    HireDate = dt, EmployeeCode = $"JV-{code}"
                }
            });
        }

        // Range filters — each must return exactly its single seeded match.
        var midRange = await redb.Query<EmployeeProps>()
            .Where(e => e.Department == Dept && e.HireDate >= new DateTime(2023, 1, 1) && e.HireDate < new DateTime(2026, 1, 1))
            .ToListAsync();

        var futureOnly = await redb.Query<EmployeeProps>()
            .Where(e => e.Department == Dept && e.HireDate >= new DateTime(2029, 1, 1))
            .ToListAsync();

        var pastOnly = await redb.Query<EmployeeProps>()
            .Where(e => e.Department == Dept && e.HireDate < new DateTime(2020, 1, 1))
            .ToListAsync();

        var failures = new List<string>();
        void Expect(string label, List<RedbObject<EmployeeProps>> got, string wantCode)
        {
            if (got.Count == 1 && got[0].Props.EmployeeCode == $"JV-{wantCode}")
                lines.Add($"OK  {label}: 1 row = {wantCode}");
            else
                failures.Add($"{label}: expected [1×{wantCode}], got [{string.Join(",", got.Select(g => g.Props.EmployeeCode))}]");
        }

        Expect("[2023,2026)", midRange, "MID");
        Expect(">=2029", futureOnly, "FUT");
        Expect("<2020", pastOnly, "PAST");

        // Round-trip: the saved fractional UTC datetime must read back intact.
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
            return Fail("E997", "DateTime REAL Julian Verify", ExampleTier.Free, sw.ElapsedMilliseconds,
                string.Join(" | ", failures));

        return Ok("E997", "DateTime REAL Julian Verify", ExampleTier.Free, sw.ElapsedMilliseconds,
            midRange.Count + futureOnly.Count + pastOnly.Count, lines.ToArray());
    }
}
