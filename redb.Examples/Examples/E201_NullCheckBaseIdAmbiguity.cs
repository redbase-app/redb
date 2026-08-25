using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using redb.Core;
using redb.Core.Query;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Reproduces the ambiguous <c>_id</c> defect in the PVT CTE.
/// See <c>docs/BUG_PVT_NULLCHECK_AMBIGUOUS_ID.md</c>.
///
/// A props filter <c>== null</c> switches the PVT CTE into the two-table form
/// (<c>FROM _objects o_src LEFT JOIN _values v</c>), while the base-field filter
/// is compiled without a table alias. <c>_id</c> is the only column present in
/// BOTH <c>_objects</c> and <c>_values</c>, so the combination produces
/// <c>column reference "_id" is ambiguous</c>.
///
/// Four cases: two controls that must stay green, the repro, and a control that
/// pins the diagnosis to <c>_id</c> specifically (<c>_id_parent</c> exists only
/// in <c>_objects</c>, so it must stay green even in the two-table form).
///
/// Scope: verified against redb.Postgres.Pro. MSSql.Pro and SQLite.Pro generate
/// the same shape but are out of scope for now.
/// </summary>
[ExampleMeta("E201", "NullCheck + base Id: ambiguous _id in PVT CTE", "Query",
    ExampleTier.Pro, 200, "Sql", "Debug", "Bug", "Pro", "Pvt", "NullCheck",
    RelatedApis = ["IRedbQueryable.ToSqlStringAsync", "IRedbQueryable.WhereRedb"])]
public class E201_NullCheckBaseIdAmbiguity : ExampleBase
{
    /// <summary>Bare <c>_id</c>: not preceded by a dot or word char, not followed by a word char.</summary>
    private static readonly Regex BareId = new(@"(?<![\w.])_id(?![\w])", RegexOptions.Compiled);

    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var log = new StringBuilder();
        var summary = new List<string>();
        var suspects = 0;

        async Task CaseAsync(string label, string expectation, IRedbQueryable<EmployeeProps> query)
        {
            log.AppendLine("-----------------------------------------------------------------");
            log.AppendLine($"-- {label}");
            log.AppendLine($"-- expected: {expectation}");
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
                summary.Add($"{label}: SQL build failed ({ex.GetType().Name})");
                Console.WriteLine($"[BUILD-FAIL] {label}: {ex.Message}");
                return;
            }

            log.AppendLine(sql);
            log.AppendLine();

            Console.WriteLine();
            Console.WriteLine($"--- {label} (expected: {expectation}) ---");
            Console.WriteLine(sql);

            // Static verdict: the two-table CTE form together with an unqualified _id.
            var twoTableForm = sql.Contains("FROM _objects o_src");
            var hasBareId = BareId.IsMatch(sql);
            var verdict = twoTableForm && hasBareId ? "AMBIGUOUS" : "clean";
            if (verdict == "AMBIGUOUS") suspects++;

            // Dynamic verdict: does the database actually accept it.
            string execVerdict;
            try
            {
                var rows = await query.ToListAsync();
                execVerdict = $"executed, {rows.Count} rows";
            }
            catch (Exception ex)
            {
                execVerdict = $"{ex.GetType().Name}: {ex.Message.Split('\n')[0]}";
            }

            log.AppendLine($"-- static: two-table form={twoTableForm}, bare _id={hasBareId} => {verdict}");
            log.AppendLine($"-- runtime: {execVerdict}");
            log.AppendLine();

            summary.Add($"{label}: {verdict} | {execVerdict}");
            Console.WriteLine($"[{verdict,9}] {label} -> {execVerdict}");
        }

        // 1. Control. Props == null alone: two-table form, but no base filter, so nothing to qualify.
        await CaseAsync(
            "control: props == null only",
            "clean",
            redb.Query<EmployeeProps>()
                .Where(e => e.EmployeeCode == null)
                .Take(10));

        // 2. Control. Base Id alone: single-table subquery over _objects, name resolves in its own scope.
        await CaseAsync(
            "control: base Id only",
            "clean",
            redb.Query<EmployeeProps>()
                .WhereRedb(o => o.Id > 0)
                .Where(e => e.Age > 0)
                .Take(10));

        // 3. Repro. Both at once.
        await CaseAsync(
            "repro: props == null + base Id",
            "AMBIGUOUS",
            redb.Query<EmployeeProps>()
                .Where(e => e.EmployeeCode == null)
                .WhereRedb(o => o.Id > 0)
                .Take(10));

        // 4. Control. Same shape, but the base column exists only in _objects.
        //    Proves the defect is specific to _id and not to base-field pushdown in general.
        await CaseAsync(
            "control: props == null + base ParentId",
            "clean",
            redb.Query<EmployeeProps>()
                .Where(e => e.EmployeeCode == null)
                .WhereRedb(o => o.ParentId != null)
                .Take(10));

        string? outPath = null;
        try
        {
            var dir = Path.Combine(AppContext.BaseDirectory, "Output");
            Directory.CreateDirectory(dir);
            outPath = Path.Combine(dir, "nullcheck_baseid_ambiguity.txt");
            await File.WriteAllTextAsync(outPath, log.ToString());
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] failed to write dump file: {ex.Message}");
        }

        sw.Stop();

        var output = new List<string>(summary)
        {
            $"Suspect cases: {suspects} (expected 1: the repro)",
            outPath != null ? $"Output: {outPath}" : "Output file: <not written>",
            "Report: docs/BUG_PVT_NULLCHECK_AMBIGUOUS_ID.md"
        };

        return Ok("E201", "NullCheck + base Id: ambiguous _id in PVT CTE", ExampleTier.Pro,
            sw.ElapsedMilliseconds, 4, output.ToArray());
    }
}
