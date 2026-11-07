using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;
using System.Text.RegularExpressions;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Audit suite for the PVT-vs-Pro parity matrix recorded in
/// <c>docs/FreePvtQuery/FREE-OVER-PRO.md</c>.
/// 
/// Runs against both PostgresFixture (Free) and PostgresProFixture (Pro),
/// so any regression on either side surfaces immediately.
/// 
/// Skip-fact tests document known gaps that must be flipped to active when fixed.
/// </summary>
public abstract class PvtAuditTestsBase
{
    protected readonly IRedbService Redb;
    protected readonly bool IsPro;

    protected PvtAuditTestsBase(IRedbService redb, bool isPro)
    {
        Redb = redb;
        IsPro = isPro;
    }

    // ────────────────────────────────────────────────────────────────────
    // §2.x #2.1 — ListItem accessors
    // ────────────────────────────────────────────────────────────────────

    private async Task<(List<long> Ids, RedbListItem Active, RedbListItem Blocked)> SeedPersonsWithStatusAsync(string listName)
    {
        var existing = await Redb.ListProvider.GetListByNameAsync(listName);
        if (existing != null)
        {
            var oldItems = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            if (oldItems.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(oldItems.Select(i => i.Id).ToList());
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }

        var list = RedbList.Create(listName, listName);
        list = await Redb.ListProvider.SaveListAsync(list);
        var statuses = await Redb.ListProvider.AddItemsAsync(list, ["Active", "Blocked"]);
        var active = statuses.First(s => s.Value == "Active");
        var blocked = statuses.First(s => s.Value == "Blocked");

        var persons = Enumerable.Range(0, 6).Select(i => new RedbObject<PersonProps>
        {
            name = $"AuditPerson_{i}",
            Props = new PersonProps
            {
                Name = $"Person{i}",
                Age = 20 + i,
                Email = $"p{i}@audit.test",
                Status = i % 2 == 0 ? active : blocked
            }
        }).ToList();
        var ids = await Redb.SaveAsync(persons);
        for (int i = 0; i < persons.Count; i++) persons[i].id = ids[i];

        return (ids, active, blocked);
    }

    [Fact]
    public async Task ListItem_StatusValue_FiltersByDisplayText()
    {
        var (_, _, _) = await SeedPersonsWithStatusAsync("AuditStatuses_ValueAccessor");

        var results = await Redb.Query<PersonProps>()
            .Where(p => p.Status!.Value == "Active")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.Status!.Value.Should().Be("Active"));
    }

    [Fact]
    public async Task ListItem_StatusId_FiltersByItemId()
    {
        var (_, active, _) = await SeedPersonsWithStatusAsync("AuditStatuses_IdAccessor");

        var results = await Redb.Query<PersonProps>()
            .Where(p => p.Status!.Id == active.Id)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.Status!.Id.Should().Be(active.Id));
    }

    [Fact]
    public async Task ListItem_RolesAnyValue_FiltersByDisplayText()
    {
        var existing = await Redb.ListProvider.GetListByNameAsync("AuditRoles");
        if (existing != null)
        {
            var oldItems = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            if (oldItems.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(oldItems.Select(i => i.Id).ToList());
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }

        var rolesList = RedbList.Create("AuditRoles", "AuditRoles");
        rolesList = await Redb.ListProvider.SaveListAsync(rolesList);
        var roles = await Redb.ListProvider.AddItemsAsync(rolesList, ["Admin", "User", "Viewer"]);
        var adminItem = roles.First(r => r.Value == "Admin");
        var userItem = roles.First(r => r.Value == "User");

        var rolePersons = Enumerable.Range(0, 4).Select(i => new RedbObject<PersonProps>
        {
            name = $"AuditRolePerson_{i}",
            Props = new PersonProps
            {
                Name = $"P{i}",
                Age = 30,
                Email = $"r{i}@audit.test",
                Roles = i % 2 == 0 ? [adminItem, userItem] : [userItem]
            }
        }).ToList();
        await Redb.SaveAsync(rolePersons);

        var results = await Redb.Query<PersonProps>()
            .Where(p => p.Roles!.Any(r => r.Value == "Admin"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.Roles!.Any(x => x.Value == "Admin").Should().BeTrue());
    }

    /// <summary>
    /// Direct <c>e =&gt; e.Status == redbListItem</c> must be rewritten by
    /// <c>FacetFilterBuilder</c> into a <c>.Id</c>-keyed facet (the field stores
    /// <c>_listitem</c> bigint, never display text). Verifies the C# provider
    /// already emits the correct ID-based payload — no PVT-side string lookup
    /// is needed.
    /// </summary>
    [Fact]
    public async Task ListItem_DirectInstanceComparison_RoutesThroughId()
    {
        var (_, active, _) = await SeedPersonsWithStatusAsync("AuditStatuses_DirectInstance");

        var results = await Redb.Query<PersonProps>()
            .Where(p => p.Status == active)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.Status!.Id.Should().Be(active.Id));
    }

    // ────────────────────────────────────────────────────────────────────
    // §3 H1 — Take(0) must return empty, not throw (CHANGELOG 2026-05-22)
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Take_Zero_ReturnsEmpty_WithoutThrowing()
    {
        await TestDataFactory.SeedEmployees(Redb, 10);

        var results = await Redb.Query<EmployeeProps>()
            .OrderBy(e => e.LastName)
            .Take(0)
            .ToListAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Take_Zero_ReturnsEmpty_OnTreeQuery()
    {
        await TestDataFactory.SeedEmployees(Redb, 6);

        var results = await Redb.TreeQuery<EmployeeProps>()
            .OrderBy(e => e.LastName)
            .Take(0)
            .ToListAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public void Take_Negative_StillThrows()
    {
        var act = () => Redb.Query<EmployeeProps>().Take(-1);
        act.Should().Throw<ArgumentException>();
    }

    // ────────────────────────────────────────────────────────────────────
    // §1 #2 / §3 #8 — DistinctBy on tree query (DISTINCT ON parity)
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DistinctBy_DedupsByKey()
    {
        await TestDataFactory.SeedEmployees(Redb, 20);

        var distinct = await Redb.Query<EmployeeProps>()
            .DistinctBy(e => e.Department)
            .ToListAsync();

        var rawCount = await Redb.Query<EmployeeProps>().CountAsync();
        distinct.Select(r => r.Props.Department).Distinct().Count()
            .Should().Be(distinct.Count, "DistinctBy must yield one row per Department");
        distinct.Count.Should().BeLessThanOrEqualTo(rawCount);
    }

    // ────────────────────────────────────────────────────────────────────
    // §2.x #2.2 — Dict Tuple Key (ValueTuple keys serialized via RedbKeySerializer
    // to Base64-JSON; parser and storage share the same encoding, so query path
    // matches storage path 1:1).
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DictTupleKey_PerformanceReviews_FiltersByCompositeKey()
    {
        // Seeder writes PerformanceReviews[(2024, "Q1")] = "Excellent" when i % 3 == 0,
        // otherwise "Good". With 12 employees that yields exactly 4 "Excellent" rows.
        await TestDataFactory.SeedEmployees(Redb, 12);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.PerformanceReviews![ValueTuple.Create(2024, "Q1")] == "Excellent")
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.PerformanceReviews![(2024, "Q1")].Should().Be("Excellent"));
    }

    // ────────────────────────────────────────────────────────────────────
    // §2.x #2.3 — Object reference fields (RedbObject<T> property).
    // Covers null-check semantics (most common real-world use). Nested-field
    // JOIN (e.g. e.CurrentProject.ProjectId > 0) requires cross-scheme JOIN
    // infrastructure in PVT and remains a deferred probe below.
    // ────────────────────────────────────────────────────────────────────

    private async Task<(int WithProject, int WithoutProject, string Department)> SeedEmployeesWithProjectsAsync(string department)
    {
        const int total = 6;
        const int withProject = 4;

        // Phase 1: batch-save all projects to get their ids.
        var projects = Enumerable.Range(0, withProject).Select(i => new RedbObject<ProjectMetricsProps>
        {
            name = $"{department}_Project_{i:D2}",
            Props = new ProjectMetricsProps
            {
                ProjectId = 1000 + i,
                TasksCompleted = 5 + i,
                TasksTotal = 10 + i,
                Budget = 50_000d + i * 1000,
                TeamSize = 3 + i,
                Technologies = ["dotnet", "postgres"]
            }
        }).ToList();
        var projectIds = await Redb.SaveAsync(projects);
        for (int i = 0; i < projects.Count; i++) projects[i].id = projectIds[i];

        // Phase 2: build employees, wire their CurrentProject reference, batch-save.
        var employees = Enumerable.Range(0, total).Select(i =>
        {
            var emp = TestDataFactory.CreateEmployee(i, department: department);
            if (i < withProject) emp.Props.CurrentProject = projects[i];
            return emp;
        }).ToList();
        await Redb.SaveAsync(employees);

        return (withProject, total - withProject, department);
    }

    [Fact]
    public async Task ObjectRef_CurrentProject_NotNull_Filters()
    {
        var (withProject, _, dept) = await SeedEmployeesWithProjectsAsync("AuditObjRefNotNull");

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == dept && e.CurrentProject != null)
            .ToListAsync();

        results.Should().HaveCount(withProject);
        results.Should().AllSatisfy(r => r.Props.CurrentProject.Should().NotBeNull());
    }

    [Fact]
    public async Task ObjectRef_CurrentProject_IsNull_Filters()
    {
        var (_, withoutProject, dept) = await SeedEmployeesWithProjectsAsync("AuditObjRefIsNull");

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == dept && e.CurrentProject == null)
            .ToListAsync();

        results.Should().HaveCount(withoutProject);
        results.Should().AllSatisfy(r => r.Props.CurrentProject.Should().BeNull());
    }

    [Fact(Skip = "Audit probe §2.3 (deferred) — nested-field through _id_object_ref " +
                  "(e.CurrentProject!.Props.ProjectId > X) is NOT supported in either tier: " +
                  "Free throws `pvt_resolve_field_path: nested segment \"Props\" not found`, " +
                  "Pro throws `SchemeFieldResolver: Failed to resolve Props fields`. " +
                  "Requires cross-scheme JOIN infrastructure in PVT and SchemeFieldResolver in Pro.")]
    public async Task ObjectRef_CurrentProject_NestedField_Filters()
    {
        var (_, _, dept) = await SeedEmployeesWithProjectsAsync("RefNested_" + Guid.NewGuid().ToString("N").Substring(0, 6));
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department == dept && e.CurrentProject!.Props.ProjectId > 1001)
            .ToListAsync();
        results.Should().HaveCount(2);
    }

    // ────────────────────────────────────────────────────────────────────
    // §2.x #2.4 — Sql.Function whitelist.
    // The free PVT parser already routes Sql.Function<T>("NAME", args) through
    // CustomFunctionExpression → FacetFilterBuilder → JSON {"$name": [...]}.
    // pvt_build_scalar_expr accepts only the hardcoded set of $-keys and raises
    // for anything else — so the whitelist is enforced by construction.
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SqlFunction_Coalesce_Filters()
    {
        await TestDataFactory.SeedEmployees(Redb, 10);

        // Salary = 50000 + i*5000, so COALESCE(Salary, 0) > 80000 matches i in {7,8,9} → 3 rows.
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => Sql.Function<decimal>("COALESCE", e.Salary, 0m) > 80000m)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.Salary.Should().BeGreaterThan(80000m));
    }

    [Fact]
    public async Task SqlFunction_UnknownName_ThrowsWhitelistViolation()
    {
        await TestDataFactory.SeedEmployees(Redb, 3);

        var act = async () => await Redb.Query<EmployeeProps>()
            .Where(e => Sql.Function<int>("definitely_not_a_known_function_xyz", e.Age) > 0)
            .ToListAsync();

        // Unknown $-keys hit the ELSE-branch of pvt_build_scalar_expr and trigger
        // RAISE EXCEPTION (Pro path throws its own NotSupportedException up front).
        await act.Should().ThrowAsync<Exception>(
            "Sql.Function with non-whitelisted name must be rejected (no silent fallback)");
    }

    // ────────────────────────────────────────────────────────────────────
    // §1.x Phase 2.A — Coalesce operator (??)
    // Shared parser (BaseFilterExpressionParser) now emits CoalesceExpression
    // for ExpressionType.Coalesce. Free → {"$coalesce":[…]} (PVT 17_pvt_expr.sql),
    // Pro → COALESCE(a, b, …). Right-associative `a ?? b ?? c` is flattened
    // to a single n-ary node.
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Coalesce_NullableDouble_FallsBackToConstant()
    {
        await TestDataFactory.SeedEmployees(Redb, 10);

        // (Rating ?? 0.0) > 3.5
        // Pollution-resistant: assert every returned row satisfies the predicate
        // and that at least one row is returned (seeded data guarantees matches).
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => (e.Rating ?? 0.0) > 3.5)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => (r.Props.Rating ?? 0.0).Should().BeGreaterThan(3.5));

        // Cross-check: at least one row in the *full* dataset must violate the
        // predicate; otherwise the filter is trivially "always true" and we'd be
        // unable to distinguish a working filter from a no-op.
        var totalCount = await Redb.Query<EmployeeProps>().CountAsync();
        var failingCount = await Redb.Query<EmployeeProps>()
            .Where(e => (e.Rating ?? 0.0) <= 3.5)
            .CountAsync();
        failingCount.Should().BeGreaterThan(0,
            "the dataset must contain at least one row that fails the predicate, " +
            "otherwise this test cannot detect a no-op WHERE clause");
        results.Count.Should().BeLessThan(totalCount);
    }

    [Fact]
    public async Task Coalesce_Chained_FlattensToNAry()
    {
        await TestDataFactory.SeedEmployees(Redb, 10);

        // EmployeeCode is null for i%3==0 (≈ 1/3 of rows). FirstName is always set.
        // (EmployeeCode ?? FirstName).Length > 100 is always false on seeded data
        // (FirstName lengths are short), so a working filter returns zero rows.
        // If the filter were a no-op we'd get the full dataset back.
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => (e.EmployeeCode ?? e.FirstName).Length > 100)
            .ToListAsync();

        var totalCount = await Redb.Query<EmployeeProps>().CountAsync();
        totalCount.Should().BeGreaterThan(0);
        results.Should().BeEmpty(
            "predicate is universally false on seeded data — a non-empty result " +
            "indicates the chained `??` was not compiled into the WHERE clause");
    }

    // ────────────────────────────────────────────────────────────────────
    // §1.y Phase 2.B — Conditional ternary (?:)
    // Shared parser emits ConditionalValueExpression for C# ?:.
    // Free → {"$if":[cond, then, else]} (PVT 17_pvt_expr.sql),
    // Pro → (CASE WHEN cond THEN ifTrue ELSE ifFalse END).
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Ternary_PropertyVsConstant_Filters()
    {
        await TestDataFactory.SeedEmployees(Redb, 12);

        // (e.IsRemote ? 1 : 0) == 1  →  selects only remote employees.
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => (e.IsRemote ? 1 : 0) == 1)
            .ToListAsync();

        var totalCount = await Redb.Query<EmployeeProps>().CountAsync();
        var nonRemoteCount = await Redb.Query<EmployeeProps>()
            .Where(e => !e.IsRemote)
            .CountAsync();
        nonRemoteCount.Should().BeGreaterThan(0,
            "the dataset must contain at least one non-remote row to prove the " +
            "ternary actually narrowed the result set");

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Props.IsRemote.Should().BeTrue());
        results.Count.Should().BeLessThan(totalCount);
    }

    [Fact]
    public async Task Ternary_NestedScalarBranches_Filters()
    {
        await TestDataFactory.SeedEmployees(Redb, 12);

        // (e.IsRemote ? e.Salary : e.Salary * 2m) > 200000m
        // For remote rows the bare Salary is compared to 200k; for non-remote
        // rows the doubled Salary is compared. Seeded salaries are well below
        // 200k so the bare-Salary branch never matches, while the doubled-
        // Salary branch matches a subset of non-remote rows — proving the
        // CASE branch selector follows the test column.
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => (e.IsRemote ? e.Salary : e.Salary * 2m) > 200000m)
            .ToListAsync();

        var totalCount = await Redb.Query<EmployeeProps>().CountAsync();
        totalCount.Should().BeGreaterThan(0);

        results.Should().AllSatisfy(r =>
        {
            var effective = r.Props.IsRemote ? r.Props.Salary : r.Props.Salary * 2m;
            effective.Should().BeGreaterThan(200000m);
        });
        results.Count.Should().BeLessThan(totalCount);
    }

    // ────────────────────────────────────────────────────────────────────
    // §1.z Phase 2.C — Multi-arg string functions
    // (Substring / Replace / IndexOf / PadLeft).
    // Shared parser emits MultiArgFunctionCallExpression; parser also
    // applies index translation (Substring start+1, IndexOf POSITION-1)
    // so SQL output matches C# semantics on both tiers.
    // Free → {"$substring":[...]}, {"$replace":[...]}, {"$indexof":[...]},
    //        {"$padleft":[...]}, {"$padright":[...]} (17_pvt_expr.sql).
    // Pro → SUBSTRING / REPLACE / POSITION / LPAD / RPAD.
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task StringFns_SubstringAndIndexOf_FiltersByEmployeeCode()
    {
        await TestDataFactory.SeedEmployees(Redb, 15);

        // EmployeeCode is null for index%3==0 (~1/3 of rows). The remaining
        // codes look like "EMP-0001", so Substring(0,3)=="EMP" and the
        // dash sits at position 3 (0-based) — IndexOf("-")==3.
        var query = Redb.Query<EmployeeProps>();
        var totalCount = await query.CountAsync();

        var hits = await query
            .Where(e => e.EmployeeCode!.Substring(0, 3) == "EMP"
                     && e.EmployeeCode!.IndexOf("-") == 3)
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().AllSatisfy(r =>
        {
            r.Props.EmployeeCode.Should().NotBeNull();
            r.Props.EmployeeCode!.Substring(0, 3).Should().Be("EMP");
            r.Props.EmployeeCode!.IndexOf("-").Should().Be(3);
        });
        hits.Count.Should().BeLessThan(totalCount,
            "rows with null EmployeeCode must be dropped by the props JOIN");
    }

    [Fact]
    public async Task StringFns_ReplaceAndPadLeft_FiltersByLastName()
    {
        await TestDataFactory.SeedEmployees(Redb, 12);

        // LastName values are "Last000".."Last011" (7 chars). PadLeft(10) pads
        // with 3 spaces on the left -> "   Last000". PG `LPAD(s, n)` truncates
        // when s is longer than n (C# PadLeft does not), so width MUST exceed
        // input length to keep C# and PG semantics aligned.
        // Replace("Last", "ZZZ") composes a second multi-arg call into the
        // same predicate.
        var query = Redb.Query<EmployeeProps>();

        var hits = await query
            .Where(e => e.LastName.PadLeft(10) == "   Last000"
                     && e.LastName.Replace("Last", "ZZZ") == "ZZZ000")
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().AllSatisfy(r => r.Props.LastName.Should().Be("Last000"));
    }

    [Fact]
    public async Task StringFns_PadLeft_DoesNotTruncate_LikeCSharp()
    {
        // Contract test for Phase 2.C.1: when the requested width is SMALLER
        // than the input string, C# PadLeft is a no-op (returns the original
        // string unchanged). Native PG LPAD/RPAD truncate in that case, so
        // both Free PVT ($padleft) and Pro emitter wrap the requested width
        // in GREATEST(length(s), n). Without that wrap this filter would
        // match zero rows because LPAD('Last000', 5) = 'Last0' != 'Last000'.
        await TestDataFactory.SeedEmployees(Redb, 5);

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => e.LastName.PadLeft(5) == "Last000")
            .ToListAsync();

        hits.Should().NotBeEmpty(
            "PG LPAD must not truncate when target width < input length");
        hits.Should().AllSatisfy(r => r.Props.LastName.Should().Be("Last000"));
    }

    // ────────────────────────────────────────────────────────────────────
    // §1.aa Phase 2.D — Math methods (Sqrt/Sign/Exp/Log/Log10/Pow + Round 2-arg)
    //         Free → {"$sqrt":[…]} / {"$power":[…]} etc. (17_pvt_expr.sql),
    //         Pro  → SQRT()/POWER()/LN()/LOG(base,value) native PG.
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task MathFns_SqrtAndExp_FiltersByRating()
    {
        // Single-arg path: PropertyFunction.Sqrt, .Exp via FunctionCallExpression.
        // Rating values for 20 employees follow `i % 4 == 0 ? null : 3.0 + (i % 5) * 0.5`,
        // so the only Rating that satisfies BOTH (Sqrt(r) > 2 -> r > 4) AND
        // (Exp(r) > 100 -> r > ln(100) ≈ 4.605) is r = 5.0.
        // r = 5.0 happens at i % 5 == 4, excluding i % 4 == 0 → i ∈ {9, 14, 19}.
        await TestDataFactory.SeedEmployees(Redb, 20);

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => e.Rating!.Value > 0
                     && Math.Sqrt(e.Rating!.Value) > 2.0
                     && Math.Exp(e.Rating!.Value) > 100.0)
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().AllSatisfy(r =>
        {
            r.Props.Rating.Should().NotBeNull();
            r.Props.Rating!.Value.Should().BeGreaterThan(4.605, "Exp(r) > 100 implies r > ln(100)");
        });
    }

    [Fact]
    public async Task MathFns_PowAndLog_FiltersByRating()
    {
        // Multi-arg path: PropertyFunction.Pow ($power) and .Log (1-arg natural log $ln).
        // Pow(r, 2) > 20 -> r > sqrt(20) ≈ 4.472.
        // Log(r) > 1.5  -> r > exp(1.5) ≈ 4.482.
        // Both reduce to r ∈ {4.5, 5.0} in the seeded distribution.
        await TestDataFactory.SeedEmployees(Redb, 20);

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => e.Rating!.Value > 0
                     && Math.Pow(e.Rating!.Value, 2.0) > 20.0
                     && Math.Log(e.Rating!.Value) > 1.5)
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().AllSatisfy(r =>
        {
            r.Props.Rating.Should().NotBeNull();
            r.Props.Rating!.Value.Should().BeGreaterThan(4.48,
                "Log(r) > 1.5 implies r > exp(1.5)");
        });
    }

    // ────────────────────────────────────────────────────────────────────
    // §1.ab Phase 2.E — DateTime methods (AddX, DayOfWeek, DayOfYear)
    //         Free → {"$dateadd":["unit",date,n]} / {"$dayofyear":[date]} (17_pvt_expr.sql),
    //         Pro  → (date + n * INTERVAL '1 unit') / EXTRACT(DOY FROM date) native PG.
    //         HireDate is seeded as `new DateTime(2020,1,1,…UTC).AddMonths(index)`
    //         (TestDataFactory.cs:59) so dates for 20 employees span 2020-01 … 2021-08.
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DateFns_AddYears_FiltersByHireDate()
    {
        // AddYears via MultiArgFunctionCallExpression → $dateadd / INTERVAL '1 year'.
        // HireDate.AddYears(10) >= 2031-01-01 ⇒ HireDate >= 2021-01-01 ⇒ index >= 12.
        await TestDataFactory.SeedEmployees(Redb, 20);
        var threshold = new DateTime(2031, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => e.HireDate.AddYears(10) >= threshold)
            .ToListAsync();

        hits.Should().NotBeEmpty(
            "HireDate seeded for >=12 months past 2020-01 satisfies the +10y threshold");
        hits.Should().AllSatisfy(r =>
            r.Props.HireDate.AddYears(10).Should().BeOnOrAfter(threshold));
    }

    [Fact]
    public async Task DateFns_DayOfYearAndAddMonths_FiltersByHireDate()
    {
        // DayOfYear via FunctionCallExpression ($dayofyear / EXTRACT(DOY FROM ...)).
        // AddMonths via MultiArgFunctionCallExpression ($dateadd / INTERVAL '1 month').
        // Seeded HireDates fall on the 1st-of-month so DOY=1 picks January rows only.
        await TestDataFactory.SeedEmployees(Redb, 20);

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => e.HireDate.DayOfYear == 1
                     && e.HireDate.AddMonths(12).Year >= 2022)
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().AllSatisfy(r =>
        {
            r.Props.HireDate.DayOfYear.Should().Be(1);
            r.Props.HireDate.AddMonths(12).Year.Should().BeGreaterThanOrEqualTo(2022);
        });
    }

    // ─────────────────────────────────────────────────────────────────
    // §1.ac Phase 2.F — Regex methods (IsMatch / Replace)
    //         Free → {"$expr":{"$regex":[...]}} / {"$regexreplace":[...,"g"]} (17_pvt_expr.sql)
    //         Pro  → PG '~' / '~*' / REGEXP_REPLACE(... , 'g')
    //         LastName seeded as "Last000".."Last019"; FirstName as "Employee000".."Employee019".
    // ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task RegexFns_IsMatch_FiltersByLastName()
    {
        // Regex.IsMatch routes through ComparisonExpression with RegexMatch operator.
        // Pattern '^Last00[0-2]$' matches exactly Last000/Last001/Last002 → 3 rows.
        await TestDataFactory.SeedEmployees(Redb, 20);
        var pattern = "^Last00[0-2]$";

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => Regex.IsMatch(e.LastName, pattern))
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().AllSatisfy(r =>
            Regex.IsMatch(r.Props.LastName, pattern).Should().BeTrue());
    }

    [Fact]
    public async Task RegexFns_Replace_FiltersByTransformedFirstName()
    {
        // Regex.Replace routes through MultiArgFunctionCallExpression(RegexReplace).
        // Strip digits from FirstName and compare against the literal stem 'Employee'.
        // All 20 seeded employees have FirstName like 'Employee00X', so all match.
        await TestDataFactory.SeedEmployees(Redb, 20);

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => Regex.Replace(e.FirstName, "[0-9]+", "") == "Employee")
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().AllSatisfy(r =>
            Regex.Replace(r.Props.FirstName, "[0-9]+", "").Should().Be("Employee"));
    }

    // ────────────────────────────────────────────────────────────────────
    // §2 #7 — Property functions (lock at LINQ level)
    // ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PropertyFunction_StringLength_Filters()
    {
        await TestDataFactory.SeedEmployees(Redb, 15);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.LastName.Length > 6)
            .Take(20)
            .ToListAsync();

        results.Should().AllSatisfy(r =>
            r.Props.LastName.Length.Should().BeGreaterThan(6));
    }

    [Fact]
    public async Task PropertyFunction_ArrayCount_Filters()
    {
        await TestDataFactory.SeedEmployees(Redb, 15);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Skills!.Length >= 3)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.Skills!.Length.Should().BeGreaterThanOrEqualTo(3));
    }

    [Fact]
    public async Task PropertyFunction_DictContainsKey_Filters()
    {
        await TestDataFactory.SeedEmployees(Redb, 10);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.PhoneDirectory!.ContainsKey("desk"))
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.PhoneDirectory!.Should().ContainKey("desk"));
    }
}
