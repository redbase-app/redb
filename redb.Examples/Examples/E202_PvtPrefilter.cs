using System.Diagnostics;
using System.Text;
using redb.Core;
using redb.Core.Query;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Dumps SQL for the cases the PVT prefilter is meant to handle, so the same run can be
/// compared with and without the cutting step. See <c>docs/PVT_PREFILTER_PLAN.md</c>.
///
/// The prefilter is opt-in. Run twice and diff:
///   <c>dotnet run --project redb.Examples -- E202</c>                     (off)
///   <c>REDB_PVT_PREFILTER=1 dotnet run --project redb.Examples -- E202</c> (on)
///
/// Each case also executes, and the example compares the returned id sets between the two
/// runs by writing them to the dump. The prefilter is a superset by construction, so the id
/// sets MUST be identical. A difference is a defect, not a tuning question.
///
/// Expected shapes when enabled:
///   top-level OR   -> disjunction spliced into the _values scan (Row form)
///   top-level AND  -> Row form too, when the single branch covers the whole pivot
///   unanalysable   -> no prefilter at all, SQL identical to the disabled run
/// </summary>
[ExampleMeta("E202", "PVT prefilter: SQL with and without the cutting step", "Query",
    ExampleTier.Pro, 200, "Sql", "Debug", "Prefilter", "Pro", "Pvt",
    RelatedApis = ["IRedbQueryable.ToSqlStringAsync"])]
public class E202_PvtPrefilter : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var enabled = Environment.GetEnvironmentVariable("REDB_PVT_PREFILTER") == "1";
        var log = new StringBuilder();
        var summary = new List<string>();
        var withPrefilter = 0;

        log.AppendLine($"-- REDB_PVT_PREFILTER = {(enabled ? "1 (enabled)" : "<unset> (disabled)")}");
        log.AppendLine();

        async Task CaseAsync(string label, string expectation, IRedbQueryable<EmployeeProps> query)
        {
            log.AppendLine("-----------------------------------------------------------------");
            log.AppendLine($"-- {label}");
            log.AppendLine($"-- expected when enabled: {expectation}");
            log.AppendLine("-----------------------------------------------------------------");

            string sql;
            try
            {
                sql = await query.ToSqlStringAsync();
            }
            catch (Exception ex)
            {
                log.AppendLine($"-- SQL BUILD FAIL: {ex.GetType().Name}: {ex.Message}");
                log.AppendLine();
                summary.Add($"{label}: SQL build failed");
                Console.WriteLine($"[BUILD-FAIL] {label}: {ex.Message}");
                return;
            }

            log.AppendLine(sql);

            Console.WriteLine();
            Console.WriteLine($"--- {label} (expected when enabled: {expectation}) ---");
            Console.WriteLine(sql);

            // "pf" is the alias the planner gives the prefilter scan. The Row form is the extra
            // parenthesised group spliced into the _values scan; note it may hold a SINGLE branch
            // (a merged range), so matching on " OR " would miss it.
            // Semi form was removed after measuring 3x worse than nothing; this stays as a tripwire.
            var hasSemi = sql.Contains("FROM _values pf");
            var hasRow = sql.Contains("AND ((v._id_structure");
            var form = hasSemi ? "Semi" : hasRow ? "Row" : "none";
            if (form != "none") withPrefilter++;

            string ids;
            try
            {
                var rows = await query.ToListAsync();
                ids = rows.Count == 0
                    ? "<empty>"
                    : string.Join(",", rows.Select(r => r.Id).OrderBy(x => x));
            }
            catch (Exception ex)
            {
                ids = $"EXEC FAIL {ex.GetType().Name}: {ex.Message.Split('\n')[0]}";
            }

            log.AppendLine($"-- prefilter form: {form}");
            log.AppendLine($"-- ids: {ids}");
            log.AppendLine();

            summary.Add($"{label}: {form}");
            Console.WriteLine($"[{form,4}] {label}");
        }

        // Top-level OR, both branches selective. The tsum case: one search string, several
        // fields. Row form, and with pg_trgm the LIKE finally reaches the column.
        await CaseAsync(
            "OR: two string fields, same needle",
            "Row",
            redb.Query<EmployeeProps>()
                .Where(e => e.Position.Contains("Design") || e.Department.Contains("Design"))
                .Take(100));

        // Top-level AND on a single field: two predicates merge into one selective range.
        // Splitting them into an OR would give a tautology, hence the merge.
        await CaseAsync(
            "AND: range on one field",
            "Row, single branch with both bounds merged",
            redb.Query<EmployeeProps>()
                .Where(e => e.Salary >= 60000m && e.Salary < 90000m)
                .Take(100));

        // Top-level AND across fields. Only the most selective conjunct is pushed; the weak
        // one (Position != "") stays above the aggregate where it belongs.
        await CaseAsync(
            "AND: three fields, one selective",
            "none (pivot not covered: Position and Age have no branch)",
            redb.Query<EmployeeProps>()
                .Where(e => e.Position != "" && e.Age >= 30 && e.Salary > 70000m)
                .Take(100));

        // One weak branch in an OR voids the group: a disjunction is never narrower than its
        // widest term, so the extra scan would buy nothing.
        await CaseAsync(
            "OR: one weak branch",
            "none (weakest branch below threshold)",
            redb.Query<EmployeeProps>()
                .Where(e => e.Position != "" || e.Salary > 70000m)
                .Take(100));

        // Null check is not expressible as a row predicate. Under AND it is simply dropped
        // and the other conjunct still gets pushed.
        await CaseAsync(
            "AND: null check plus selective field",
            "none (pivot not covered: EmployeeCode has no branch)",
            redb.Query<EmployeeProps>()
                .Where(e => e.EmployeeCode == null && e.Salary > 70000m)
                .Take(100));

        // Arrays are not single-row values. Under OR that voids the whole prefilter.
        await CaseAsync(
            "OR: array branch",
            "none (array is not a row predicate)",
            redb.Query<EmployeeProps>()
                .Where(e => e.Skills!.Contains("C#") || e.Salary > 70000m)
                .Take(100));

        // Sorting pulls Age into the pivot without putting it in the filter. The Row form
        // would null that column out, so the guard suppresses the prefilter entirely.
        //
        // ThenByRedb(Id) is not decoration: Age has thousands of ties, and Take(100) over a tie
        // returns whichever hundred the plan happens to produce. Without a unique tiebreak this
        // case reported a difference between two runs of the SAME configuration, which reads as
        // a prefilter defect and is not one. Id is a base field, so it adds nothing to the pivot
        // and the guard still fires on Age.
        await CaseAsync(
            "OR plus OrderBy on an uncovered field",
            "none (pivot coverage guard: Age comes from OrderBy)",
            redb.Query<EmployeeProps>()
                .Where(e => e.Position.Contains("Design") || e.Department.Contains("Design"))
                .OrderBy(e => e.Age)
                .ThenByRedb(o => o.Id)
                .Take(100));

        // ---- dates -------------------------------------------------------
        // Same algebra as numbers, different column (_datetimeoffset) and a different
        // parameter round-trip. Worth its own cases: a DateTime that survives C# to Npgsql
        // to the pivot but not into the prefilter branch would silently narrow the result.

        // Closed range on one field. Two predicates, one structure, one merged branch.
        await CaseAsync(
            "AND: date range on one field",
            "Row, single branch with both bounds merged",
            redb.Query<EmployeeProps>()
                .Where(e => e.HireDate >= new DateTime(2030, 1, 1) && e.HireDate < new DateTime(2031, 1, 1))
                .Take(100));

        // Open-ended comparison, single leaf at the root.
        await CaseAsync(
            "date: single greater-than",
            "Row, one branch",
            redb.Query<EmployeeProps>()
                .Where(e => e.HireDate > new DateTime(2048, 1, 1))
                .Take(100));

        // Mixed types under OR: a date branch and a string branch in one disjunction.
        await CaseAsync(
            "OR: date branch plus string branch",
            "Row, two branches on different columns",
            redb.Query<EmployeeProps>()
                .Where(e => e.HireDate > new DateTime(2050, 1, 1) || e.Position.Contains("Design"))
                .Take(100));

        // Inequality on a date is not selective enough to earn a scan.
        await CaseAsync(
            "date: not-equal",
            "none (NotEqual passes almost everything)",
            redb.Query<EmployeeProps>()
                .Where(e => e.HireDate != new DateTime(2030, 1, 1))
                .Take(100));

        string? outPath = null;
        try
        {
            var dir = Path.Combine(AppContext.BaseDirectory, "Output");
            Directory.CreateDirectory(dir);
            outPath = Path.Combine(dir, enabled ? "pvt_prefilter_on.txt" : "pvt_prefilter_off.txt");
            await File.WriteAllTextAsync(outPath, log.ToString());
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] failed to write dump file: {ex.Message}");
        }

        sw.Stop();

        var output = new List<string>(summary)
        {
            $"Prefilter: {(enabled ? "ENABLED" : "disabled")}, applied in {withPrefilter} of 11 cases",
            outPath != null ? $"Output: {outPath}" : "Output file: <not written>",
            "Diff pvt_prefilter_off.txt against pvt_prefilter_on.txt: id sets must match exactly"
        };

        return Ok("E202", "PVT prefilter: SQL with and without the cutting step", ExampleTier.Pro,
            sw.ElapsedMilliseconds, 11, output.ToArray());
    }
}
