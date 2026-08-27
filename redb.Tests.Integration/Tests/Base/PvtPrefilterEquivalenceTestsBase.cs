using redb.Core;
using redb.Core.Query;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Differential tests for the PVT prefilter (<c>RedbServiceConfiguration.EnablePvtPrefilter</c>).
///
/// The prefilter narrows the object set BEFORE the pivot aggregate runs. It is built as a superset:
/// it may let extra objects through, it may never lose one, and the authoritative filter stays above
/// GROUP BY. So for every query the result must be identical with the prefilter on and off.
///
/// Each test runs the same query twice against the same rows, flipping the flag in between, and
/// compares the two answers. A divergence is a prefilter bug, never a reason to relax the assertion.
/// The row form is destructive by nature: rows failing its predicate never reach the aggregate, so a
/// pivot column left uncovered collapses to NULL and its object silently disappears. Catching exactly
/// that is what this suite is for, which is why several cases deliberately put a field in ORDER BY or
/// DISTINCT BY that the filter never mentions.
/// 
/// Every ordered query ends with ThenByRedb(o => o.Id) before Take. The seed data repeats across
/// tests in a collection, so Age, Salary and HireDate all carry ties, and a Take over a partial
/// order returns an arbitrary slice that differs run to run. Without the tiebreak the assertions
/// compare noise: MSSql alone produced five such false failures in one full run.
/// </summary>
public abstract class PvtPrefilterEquivalenceTestsBase
{
    protected readonly IRedbService Redb;
    private bool _seeded;

    protected PvtPrefilterEquivalenceTestsBase(IRedbService redb) => Redb = redb;

    private async Task SeedAsync()
    {
        if (_seeded) return;
        await TestDataFactory.SeedEmployees(Redb, 40);
        _seeded = true;
    }

    /// <summary>
    /// Runs the same delegate twice, prefilter off then on, and restores the previous setting.
    /// </summary>
    private async Task<(T Off, T On)> BothAsync<T>(Func<Task<T>> run)
    {
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await run();

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var on = await run();

            return (off, on);
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    private async Task AssertSameAsync(Func<IRedbQueryable<EmployeeProps>> build, string because)
    {
        await SeedAsync();

        var (off, on) = await BothAsync(async () =>
        {
            var rows = await build().ToListAsync();
            return rows.Select(r => r.id).ToList();
        });

        on.Should().Equal(off, because);
    }

    // ──────────────────────────────────────────────────────────────────
    //  The flag must actually reach the query provider
    // ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Guards the whole suite against a false green. If the configuration object the service exposes
    /// were a different instance from the one the query provider reads, every test below would compare
    /// two identical runs and pass without testing anything.
    /// </summary>
    [Fact]
    public async Task Flag_ChangesGeneratedSql()
    {
        await SeedAsync();

        var (off, on) = await BothAsync(() => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .ToSqlStringAsync());

        on.Should().NotBe(off,
            "an OR over two selective string fields is the canonical shape the prefilter handles; " +
            "identical SQL means EnablePvtPrefilter never reached the query provider");
    }

    /// <summary>
    /// The planner writes its decision into the SQL preview as comments. Refusals are the interesting
    /// half: before this existed, finding out why a production query got no prefilter meant patching
    /// the API to log the configuration flag and waiting for a deploy.
    ///
    /// The comments live in <c>GetSqlPreviewAsync</c> only. They must never reach the executed
    /// statement: a comment that varies per query is a distinct plan-cache key in both PostgreSQL and
    /// SQL Server, which would trade a diagnostic for a cache that never hits.
    /// </summary>
    [Fact]
    public async Task PlannerTrace_ExplainsTheDecision()
    {
        await SeedAsync();

        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);

            var applied = await Redb.Query<EmployeeProps>()
                .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
                .OrderByRedb(o => o.Id)
                .Take(100)
                .ToSqlStringAsync();
            applied.Should().Contain("-- PVT prefilter:",
                "the planner decision belongs in the preview, next to the parameter block");
            applied.Should().Contain("branch:",
                "an applied plan lists the branches it emitted");

            // Age enters the pivot through the filter but gets no branch of its own, so the row form
            // would null its column out. The trace has to name that, not just stay silent.
            var declined = await Redb.Query<EmployeeProps>()
                .Where(e => e.Position.Contains("Developer") && e.Age > 30)
                .OrderByRedb(o => o.Id)
                .Take(100)
                .ToSqlStringAsync();
            declined.Should().Contain("PivotNotCovered",
                "a refusal must say which guard stopped it");
            declined.Should().Contain("Age",
                "and which pivot column was left uncovered");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    //  Shapes the planner accepts
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public Task Or_TwoStringFields_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .OrderBy(e => e.Age).ThenBy(e => e.Salary).ThenByRedb(o => o.Id).Take(100),
        "an OR over two covered structures is the shape the row form is built for");

    [Fact]
    public Task Or_SameFieldTwice_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Engineering" || e.Department == "Marketing")
            .OrderBy(e => e.Salary).ThenByRedb(o => o.Id).Take(100),
        "both branches sit on one structure, which merges into a single covered branch");

    [Fact]
    public Task Or_ThreeFieldsMixedTypes_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Senior") || e.Age > 40 || e.Salary > 150000m)
            .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100),
        "branches over different storage columns must all render correctly");

    [Fact]
    public Task Range_SingleNumericField_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Salary >= 60000m && e.Salary < 120000m)
            .OrderBy(e => e.Salary).ThenByRedb(o => o.Id).Take(100),
        "two half-lines on one structure merge into one selective range");

    [Fact]
    public Task Range_DateTimeField_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.HireDate >= new DateTime(2020, 6, 1, 0, 0, 0, DateTimeKind.Utc)
                     && e.HireDate < new DateTime(2021, 6, 1, 0, 0, 0, DateTimeKind.Utc))
            .OrderBy(e => e.HireDate).ThenByRedb(o => o.Id).Take(100),
        "date ranges are the case the partial index on the datetime column exists for");

    [Fact]
    public Task Equality_SingleStringField_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Engineering")
            .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100),
        "a single leaf behaves like a one-branch disjunction");

    [Fact]
    public Task StartsWith_SingleStringField_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.StartsWith("Senior"))
            .OrderBy(e => e.Salary).ThenByRedb(o => o.Id).Take(100),
        "StartsWith renders as a LIKE with a trailing wildcard only");

    // ──────────────────────────────────────────────────────────────────
    //  Pivot columns the filter never mentions — the destructive case
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public Task Or_OrderByUnfilteredField_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .OrderBy(e => e.HireDate).ThenBy(e => e.Salary).ThenByRedb(o => o.Id).Take(100),
        "HireDate enters the pivot through ORDER BY; if the prefilter dropped its rows the column " +
        "would collapse to NULL and the ordering would change");

    [Fact]
    public Task Or_DistinctByUnfilteredField_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .DistinctBy(e => e.Department)
            .OrderBy(e => e.Department).ThenByRedb(o => o.Id).Take(100),
        "DISTINCT BY adds a pivot column the filter never names");

    [Fact]
    public Task Range_OrderByOtherField_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Salary >= 60000m && e.Salary < 120000m)
            .OrderBy(e => e.Position).ThenBy(e => e.Salary).ThenByRedb(o => o.Id).Take(100),
        "a covered filter plus an uncovered ordering column is where coverage must refuse the prefilter");

    // ──────────────────────────────────────────────────────────────────
    //  Shapes the planner must refuse
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public Task And_AcrossDifferentFields_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") && e.Department.Contains("Engineering"))
            .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100),
        "a row belongs to one structure, so a cross-field conjunction is not expressible at row level");

    /// <summary>
    /// The shape that loses objects rather than merely reordering them. A nested disjunction under a
    /// conjunction would give the planner a multi-branch candidate whose sibling conjunct reads a
    /// column the prefilter has already nulled out: an employee whose Position matches and whose
    /// Department matches the outer term but not the inner one loses its Department row, so the
    /// surviving filter reads NULL and drops the employee outright.
    /// </summary>
    [Fact]
    public Task NestedOr_UnderAnd_OnCoveredStructure_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => (e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
                        && e.Department.Contains("a"))
            .OrderByRedb(o => o.Id).Take(100),
        "the sibling conjunct reads Department, which a Position-only match would have nulled out");

    /// <summary>
    /// Same trap without any ordering or distinct to hide behind: nothing outside the filter reads the
    /// pivot here, so only the conjunction itself can expose the nulled column.
    /// </summary>
    [Fact]
    public Task NestedOr_UnderAnd_NoOrdering_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => (e.Position.Contains("Senior") || e.Department.Contains("Engineering"))
                        && e.Position.Contains("e"))
            .OrderByRedb(o => o.Id).Take(100),
        "the sibling conjunct reads Position, which a Department-only match would have nulled out");

    /// <summary>
    /// A pure disjunction with nothing reading the pivot outside the filter. This is the shape the
    /// prefilter exists for and the one guard 2 must keep allowing, so a regression that disables the
    /// prefilter wholesale would still pass here while Flag_ChangesGeneratedSql catches it.
    /// </summary>
    [Fact]
    public Task Or_NoPropsOrdering_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .OrderByRedb(o => o.Id).Take(100),
        "ordering by the object id keeps every pivot column readable by the filter alone");

    [Fact]
    public Task NullCheck_WithOr_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.EmployeeCode == null || e.Department.Contains("Engineering"))
            .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100),
        "a null check needs absent and present-but-not-matching to stay distinct, which dropping rows blurs");

    [Fact]
    public Task ArrayContains_WithOr_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Skills!.Contains("C#") || e.Department.Contains("Marketing"))
            .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100),
        "array membership is not a row predicate; the planner must give up on the whole disjunction");

    [Fact]
    public Task NestedObjectField_WithOr_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.HomeAddress!.City == "New York" || e.Department.Contains("Marketing"))
            .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100),
        "nested fields reach the pivot through their own aliases");

    [Fact]
    public Task Dictionary_WithOr_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.PhoneDirectory!.ContainsKey("desk") || e.Department.Contains("Marketing"))
            .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100),
        "dictionary access is not a row predicate either");

    [Fact]
    public Task BaseFieldFilter_WithOr_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .WhereRedb(o => o.Id > 0)
            .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100),
        "a filter on the object itself lands beside the pivot and must not collide with it");

    // ──────────────────────────────────────────────────────────────────
    //  Paths other than ToListAsync
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Count_WithOr_SameResults()
    {
        await SeedAsync();

        var (off, on) = await BothAsync(() => Redb.Query<EmployeeProps>()
            .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
            .CountAsync());

        on.Should().Be(off, "counting goes through its own SQL builder");
    }

    [Fact]
    public async Task Tree_WithOr_SameResults()
    {
        var root = TestDataFactory.CreateTreeNode("PrefilterRoot", "PF-ROOT", 1_000_000m);
        root.id = await Redb.SaveAsync(root);

        for (var i = 0; i < 6; i++)
        {
            var child = TestDataFactory.CreateTreeNode(
                i % 2 == 0 ? $"Engineering {i}" : $"Marketing {i}",
                $"PF-{i}",
                100_000m + i * 10_000m);
            child.id = await Redb.CreateChildAsync(child, root);

            var grand = TestDataFactory.CreateTreeNode($"Team {i}", $"PF-{i}-T", 10_000m + i);
            await Redb.CreateChildAsync(grand, child);
        }

        var rootObj = await Redb.LoadAsync<TreeNodeProps>(root.id);

        var (off, on) = await BothAsync(async () =>
        {
            var rows = await Redb.TreeQuery<TreeNodeProps>(rootObj)
                .Where(n => n.Name.Contains("Engineering") || n.Code.Contains("PF-1"))
                .OrderBy(n => n.Budget)
                .ToListAsync();
            return rows.Select(r => r.id).ToList();
        });

        on.Should().Equal(off, "the tree provider builds its own pivot over the recursive CTE");
    }

    /// <summary>
    /// The delete path is the one caller whose only scheme filter lives inside the pivot CTE, so it is
    /// where a wrongly narrowed pivot would destroy data rather than merely hide it. Each run works on
    /// its own freshly saved batch, so the two are independent and directly comparable.
    /// </summary>
    [Fact]
    public async Task Delete_WithOr_DeletesTheSameRows()
    {
        async Task<(int Deleted, int Survived)> RunAsync(string marker)
        {
            var batch = Enumerable.Range(0, 12)
                .Select(i => TestDataFactory.CreateEmployee(
                    i, department: i % 2 == 0 ? marker : marker + "-keep"))
                .ToList();
            var ids = await Redb.SaveAsync(batch);
            var idSet = ids.ToHashSet();

            var deleted = await Redb.Query<EmployeeProps>()
                .Where(e => e.Department == marker)
                .WhereRedb(o => idSet.Contains(o.Id))
                .DeleteAsync();

            var survivors = await Redb.Query<EmployeeProps>()
                .WhereRedb(o => idSet.Contains(o.Id))
                .Take(100)
                .ToListAsync();

            return (deleted, survivors.Count);
        }

        var (off, on) = await BothAsync(() => RunAsync(Guid.NewGuid().ToString("N")[..8]));

        on.Should().Be(off, "DeleteAsync keeps its own scheme check; the prefilter must not widen it");
    }
}
