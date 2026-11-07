using System.Diagnostics;
using System.Text;
using redb.Core;
using redb.Core.Query;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Dumps SQL generated for a representative set of LINQ queries that mirror
/// the cases in <c>redb.Postgres/sql/v2-pvt/99_smoke_auto.sql</c>.
/// Designed so that running it against the Pro provider produces a SQL log
/// that can be compared side-by-side with the SQL produced by free v2-pvt
/// (<c>pvt_build_query_sql</c>).
///
/// The example does not execute any data queries. It only invokes
/// <see cref="RedbQueryableExtensions.ToSqlStringAsync{TProps}"/> for each
/// query and writes the resulting SQL to:
///   - the console (RAISE-style banner per case);
///   - a text file at <c>Output/smoke_sql_dump.txt</c> in the example output
///     directory, so it can be diffed with the v2-pvt INSPECT block.
/// </summary>
[ExampleMeta("E200", "SQL dump for smoke cases (Pro vs PVT)", "Query",
    ExampleTier.Pro, 200, "Sql", "Debug", "Smoke", "Pro", "Pvt",
    RelatedApis = ["IRedbQueryable.ToSqlStringAsync"])]
public class E200_SmokeSqlDump : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var log = new StringBuilder();
        int ok = 0, fail = 0;

        Task EmployeeAsync(string label, Func<IRedbQueryable<EmployeeProps>, IRedbQueryable<EmployeeProps>> build)
            => DumpAsync<EmployeeProps>(label, () => build(redb.Query<EmployeeProps>()).Take(100));

        Task PersonAsync(string label, Func<IRedbQueryable<PersonProps>, IRedbQueryable<PersonProps>> build)
            => DumpAsync<PersonProps>(label, () => build(redb.Query<PersonProps>()).Take(100));

        Task CityAsync(string label, Func<IRedbQueryable<CityProps>, IRedbQueryable<CityProps>> build)
            => DumpAsync<CityProps>(label, () => build(redb.Query<CityProps>()).Take(100));

        Task DepartmentAsync(string label, Func<IRedbQueryable<DepartmentProps>, IRedbQueryable<DepartmentProps>> build)
            => DumpAsync<DepartmentProps>(label, () => build(redb.Query<DepartmentProps>()).Take(100));

        async Task DumpAsync<TProps>(string label, Func<IRedbQueryable<TProps>> build) where TProps : class, new()
        {
            log.AppendLine("-----------------------------------------------------------------");
            log.AppendLine($"-- {label}");
            log.AppendLine("-----------------------------------------------------------------");
            IRedbQueryable<TProps>? q = null;
            try
            {
                q = build();
            }
            catch (Exception ex)
            {
                log.AppendLine($"-- BUILD FAIL: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine($"[BUILD-FAIL] {label}: {ex.Message}");
                fail++;
                log.AppendLine();
                return;
            }
            try
            {
                var sql = await q.ToSqlStringAsync();
                log.AppendLine(sql);
                Console.WriteLine($"[OK]   {label}");
                ok++;
            }
            catch (Exception ex)
            {
                log.AppendLine($"-- SQL FAIL: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine($"[FAIL] {label}: {ex.Message}");
                fail++;
            }
            log.AppendLine();
        }

        // ---------------- Employee: scalar predicates --------------------
        await EmployeeAsync("eq string",                q => q.Where(e => e.FirstName == "Ivan"));
        await EmployeeAsync("ne string",                q => q.Where(e => e.FirstName != "Ivan"));
        await EmployeeAsync("in string",                q => q.Where(e => new[] { "IT", "HR" }.Contains(e.Department)));
        await EmployeeAsync("nin string",               q => q.Where(e => !new[] { "Sales" }.Contains(e.Department)));
        await EmployeeAsync("range int",                q => q.Where(e => e.Age >= 18 && e.Age <= 65));
        await EmployeeAsync("numeric gt decimal",       q => q.Where(e => e.Salary > 1000m));
        await EmployeeAsync("numeric lt decimal",       q => q.Where(e => e.Salary < 1_000_000m));
        await EmployeeAsync("numeric range",            q => q.Where(e => e.Salary >= 0m && e.Salary <= 1_000_000m));
        await EmployeeAsync("null check",               q => q.Where(e => e.EmployeeCode == null));
        await EmployeeAsync("notNull check",            q => q.Where(e => e.EmployeeCode != null));
        await EmployeeAsync("startsWith",               q => q.Where(e => e.FirstName.StartsWith("A")));
        await EmployeeAsync("endsWith",                 q => q.Where(e => e.LastName.EndsWith("ov")));
        await EmployeeAsync("contains",                 q => q.Where(e => e.Position.Contains("eng")));
        await EmployeeAsync("containsIgnoreCase",       q => q.Where(e => e.Position.ToLower().Contains("eng")));
        await EmployeeAsync("startsWithIgnoreCase",     q => q.Where(e => e.FirstName.ToLower().StartsWith("a")));
        await EmployeeAsync("endsWithIgnoreCase",       q => q.Where(e => e.LastName.ToLower().EndsWith("ov")));
        await EmployeeAsync("$and+$or",                 q => q.Where(e => e.Age > 18 && (e.Department == "IT" || e.Department == "HR")));
        await EmployeeAsync("$or top-level",            q => q.Where(e => e.Department == "IT" || e.Department == "HR"));
        await EmployeeAsync("$not",                     q => q.Where(e => !(e.Age < 18)));
        await EmployeeAsync("$not + $and",              q => q.Where(e => !(e.Age < 18 && e.Department == "IT")));
        await EmployeeAsync("multi-field AND merge",    q => q.Where(e => e.Department == "IT" && e.Age > 18));
        await EmployeeAsync("date gt",                  q => q.Where(e => e.HireDate > new DateTime(2020, 1, 1)));
        await EmployeeAsync("date lte",                 q => q.Where(e => e.HireDate <= new DateTime(2030, 1, 1)));
        await EmployeeAsync("date range",               q => q.Where(e => e.HireDate >= new DateTime(2000, 1, 1) && e.HireDate < new DateTime(2030, 1, 1)));

        // ---------------- Employee: base-field pushdown (WhereRedb) ------
        await EmployeeAsync("base _id gt",              q => q.WhereRedb(o => o.Id > 0));
        await EmployeeAsync("base 0$:Name startsWith",  q => q.WhereRedb(o => o.Name.StartsWith("E")));
        await EmployeeAsync("pushdown _date_create gte",q => q.WhereRedb(o => o.DateCreate >= new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        await EmployeeAsync("pushdown base+props AND",  q => q.WhereRedb(o => o.Id > 0).Where(e => e.FirstName.StartsWith("A")));
        await EmployeeAsync("pushdown $or all-base",    q => q.WhereRedb(o => o.Id < 10 || o.Id > 100));
        await EmployeeAsync("pushdown $not base",       q => q.WhereRedb(o => !(o.Id < 0)));
        await EmployeeAsync("pushdown nested $and+$or", q => q.WhereRedb(o => o.Id > 0).Where(e => e.Department == "IT" || e.Department == "HR"));

        // ---------------- Employee: expression engine (B2) ---------------
        await EmployeeAsync("expr arithmetic gt",       q => q.Where(e => e.Age + 1 > 18));
        await EmployeeAsync("expr mul base pushdown",   q => q.WhereRedb(o => o.Id * 2 < 1_000_000));
        await EmployeeAsync("expr upper eq",            q => q.Where(e => e.FirstName.ToUpper() == "IVAN"));
        await EmployeeAsync("expr concat ilike",        q => q.Where(e => (e.FirstName + " " + e.LastName).ToLower().Contains("van")));
        await EmployeeAsync("expr coalesce eq",         q => q.Where(e => (e.EmployeeCode ?? "none") == "IT"));
        await EmployeeAsync("expr length gt",           q => q.Where(e => e.FirstName.Length > 0));
        await EmployeeAsync("expr abs sub",             q => q.Where(e => Math.Abs(e.Age - 30) < 1000));
        await EmployeeAsync("expr salary * 12",         q => q.Where(e => e.Salary * 12m > 1_000_000m));

        // ---------------- Employee: arrays -------------------------------
        await EmployeeAsync("array contains",           q => q.Where(e => e.Skills!.Contains("C#")));
        await EmployeeAsync("array length gt",          q => q.Where(e => e.Skills!.Length > 0));
        await EmployeeAsync("array length eq 3",        q => q.Where(e => e.Skills!.Length == 3));
        await EmployeeAsync("array any",                q => q.Where(e => e.Skills!.Any()));
        await EmployeeAsync("array not any",            q => q.Where(e => !e.Skills!.Any()));
        await EmployeeAsync("array int contains 5",     q => q.Where(e => e.SkillLevels!.Contains(5)));

        // ---------------- Employee: dictionaries -------------------------
        await EmployeeAsync("dict ContainsKey work",    q => q.Where(e => e.PhoneDirectory!.ContainsKey("work")));
        await EmployeeAsync("dict indexer eq",          q => q.Where(e => e.PhoneDirectory!["work"] == "+7-000-0000000"));
        await EmployeeAsync("dict nested .City",        q => q.Where(e => e.OfficeLocations!["Moscow"].City == "Moscow"));
        await EmployeeAsync("dict nested .Street!=null",q => q.Where(e => e.OfficeLocations!["Moscow"].Street != null));

        // ---------------- Person: ListItem accessors ---------------------
        await PersonAsync("Person eq Name",             q => q.Where(p => p.Name == "Alice"));
        await PersonAsync("Person Age range",           q => q.Where(p => p.Age >= 18 && p.Age <= 99));
        await PersonAsync("Person Email contains @",    q => q.Where(p => p.Email.Contains("@")));
        await PersonAsync("Person Status.Value eq",     q => q.Where(p => p.Status!.Value == "Active"));
        await PersonAsync("Person Roles[].Value any",   q => q.Where(p => p.Roles!.Any(r => r.Value == "admin")));

        // ---------------- City -------------------------------------------
        await CityAsync("City bool true",               q => q.Where(c => c.IsCapital == true));
        await CityAsync("City bool false",              q => q.Where(c => c.IsCapital == false));
        await CityAsync("City coords length eq 2",      q => q.Where(c => c.Coordinates.Length == 2));
        await CityAsync("City population gt",           q => q.Where(c => c.Population > 1000));
        await CityAsync("City region startsWith M",     q => q.Where(c => c.Region.StartsWith("M")));
        await CityAsync("City name in",                 q => q.Where(c => new[] { "Moscow", "Paris" }.Contains(c.Name)));

        // ---------------- Department -------------------------------------
        await DepartmentAsync("Dept eq bool",           q => q.Where(d => d.IsActive == true));
        await DepartmentAsync("Dept name startsWith",   q => q.Where(d => d.Name.StartsWith("IT")));
        await DepartmentAsync("Dept code contains '-'", q => q.Where(d => d.Code.Contains("-")));
        await DepartmentAsync("Dept budget >= 0",       q => q.Where(d => d.Budget >= 0m));
        await DepartmentAsync("Dept description != null", q => q.Where(d => d.Description != null));

        // ---------------- Write SQL log to disk --------------------------
        string? outPath = null;
        try
        {
            var dir = Path.Combine(AppContext.BaseDirectory, "Output");
            Directory.CreateDirectory(dir);
            outPath = Path.Combine(dir, "smoke_sql_dump.txt");
            await File.WriteAllTextAsync(outPath, log.ToString());
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] failed to write dump file: {ex.Message}");
        }

        sw.Stop();

        return Ok("E200", "SQL dump for smoke cases (Pro vs PVT)", ExampleTier.Pro, sw.ElapsedMilliseconds, ok + fail,
            new[]
            {
                $"Cases dumped: {ok} OK, {fail} FAIL",
                outPath != null ? $"Output: {outPath}" : "Output file: <not written>",
                "Compare with: redb.Postgres/sql/v2-pvt/99_smoke_auto.sql (INSPECT block)"
            });
    }
}
