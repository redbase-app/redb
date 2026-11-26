using redb.Core;
using redb.Core.Query;
using redb.Core.Query.Aggregation;
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

    /// <summary>
    /// A page the leading form takes (a condition on the sort field that rejects NULL) ordered then by the object id
    /// (ThenByRedb). The streams branch of the form sorts by the id as well and must carry that column: on PostgreSQL and
    /// SQLite it did not, and the page failed with "column pvt._id does not exist". Pages with and without Skip, ascending
    /// and descending, must equal the ones with the prefilter off.
    /// </summary>
    [Fact]
    public async Task LeadingForm_ThenByObjectId_SameAnswers()
    {
        await SeedAsync();
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<EmployeeProps> BySalary() => Redb.Query<EmployeeProps>()
                .Where(e => e.Salary > 0m).OrderBy(e => e.Salary).ThenByRedb(o => o.Id);
            IRedbQueryable<EmployeeProps> ByAgeDesc() => Redb.Query<EmployeeProps>()
                .Where(e => e.Age >= 20).OrderByDescending(e => e.Age).ThenByRedb(o => o.Id);
            async Task<string> AnswersAsync() => string.Join("; ", new[]
            {
                string.Join(",", (await BySalary().Take(10).ToListAsync()).Select(r => r.Id)),
                string.Join(",", (await BySalary().Skip(7).Take(10).ToListAsync()).Select(r => r.Id)),
                string.Join(",", (await ByAgeDesc().Take(10).ToListAsync()).Select(r => r.Id)),
                string.Join(",", (await ByAgeDesc().Skip(25).Take(10).ToListAsync()).Select(r => r.Id)),
            });

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Should().Be(off, "the leading form changes the plan, not the page");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// A page with no filter, sorted descending by a field a quarter of the objects lack (Rating), is led by the index
    /// of values of that field: the objects without a value come last. Pages inside the values, one that crosses into
    /// the objects without a value and one past every object (the walk does not fill them, the other branch answers),
    /// with ThenBy and without; without a tiebreak only the sequence of sort values is compared.
    /// </summary>
    [Fact]
    public async Task LeadingForm_UnfilteredDescending_SameAnswers()
    {
        await SeedAsync();
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<EmployeeProps> ByRating() => Redb.Query<EmployeeProps>()
                .OrderByDescending(e => e.Rating).ThenByRedb(o => o.Id);
            IRedbQueryable<EmployeeProps> ByRatingThenAge() => Redb.Query<EmployeeProps>()
                .OrderByDescending(e => e.Rating).ThenBy(e => e.Age).ThenByRedb(o => o.Id);
            IRedbQueryable<EmployeeProps> BySalary() => Redb.Query<EmployeeProps>()
                .OrderByDescending(e => e.Salary);
            async Task<string> AnswersAsync()
            {
                var rated = await Redb.Query<EmployeeProps>().Where(e => e.Rating != null).CountAsync();
                var total = await Redb.Query<EmployeeProps>().CountAsync();
                return string.Join("; ", new[]
                {
                    string.Join(",", (await ByRating().Take(10).ToListAsync()).Select(r => r.Id)),
                    string.Join(",", (await ByRating().Skip(7).Take(10).ToListAsync()).Select(r => r.Id)),
                    string.Join(",", (await ByRating().Skip(rated - 4).Take(10).ToListAsync()).Select(r => r.Id)),
                    string.Join(",", (await ByRating().Skip(total + 5).Take(10).ToListAsync()).Select(r => r.Id)),
                    string.Join(",", (await ByRatingThenAge().Skip(3).Take(10).ToListAsync()).Select(r => r.Id)),
                    string.Join(",", (await ByRatingThenAge().Skip(rated - 2).Take(5).ToListAsync()).Select(r => r.Id)),
                    string.Join(",", (await BySalary().Take(10).ToListAsync()).Select(r => r.Props.Salary)),
                    string.Join(",", (await BySalary().Skip(12).Take(10).ToListAsync()).Select(r => r.Props.Salary)),
                });
            }

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Should().Be(off, "the walk of the index changes the plan, not the page");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

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

            // The prefilter lives in the aggregate form. A plain OrderByRedb(o => o.Id) now goes inside
            // the streams form, so the ordering is an expression over Id, which still keeps the query
            // on the aggregate form; the order is the same.
            var applied = await Redb.Query<EmployeeProps>()
                .Where(e => e.Position.Contains("Developer") || e.Department.Contains("Engineering"))
                .OrderByRedb(o => o.Id + 0)
                .Take(100)
                .ToSqlStringAsync();
            applied.Should().Contain("-- PVT prefilter:",
                "the planner decision belongs in the preview, next to the parameter block");
            applied.Should().Contain("branch:",
                "an applied plan lists the branches it emitted");

            // Age enters the pivot through ORDER BY, so no branch can cover it and the row form would
            // null its column out. The trace has to name that, not just stay silent.
            //
            // Deliberately ordered by a Props field rather than filtered by one: since every conjunct
            // became a branch, a second FILTER field is covered like the first and no longer declines.
            var declined = await Redb.Query<EmployeeProps>()
                .Where(e => e.Position.Contains("Developer"))
                .OrderBy(e => e.Age)
                .ThenByRedb(o => o.Id + 0)
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

    /// <summary>
    /// "a || b || c" parses as OrElse(OrElse(a, b), c). The planner read only the direct operands of
    /// the outer OR, met the inner OR as an unknown leaf and declined the whole disjunction with
    /// NoAnalyzableLeaf, although each leaf was expressible. Two leaves got the prefilter, three
    /// did not. The ordering is an expression over Id for the reason given in PlannerTrace_ExplainsTheDecision.
    /// </summary>
    [Fact]
    public async Task Or_ThreeLeaves_GetsOneBranchEach()
    {
        await SeedAsync();

        IRedbQueryable<EmployeeProps> Build() => Redb.Query<EmployeeProps>()
            .WhereRedb(o => o.Id > 0)
            .Where(e => e.Position.Contains("Developer")
                     || e.Department.Contains("Engineering")
                     || e.LastName.Contains("son"))
            .OrderByRedb(o => o.Id + 0)
            .Take(100);

        var (_, sql) = await BothAsync(() => Build().ToSqlStringAsync());
        sql.Should().Contain("-- PVT prefilter: Row form, 3 branch(es)",
            "every leaf of a nested OR is reachable, so each becomes a branch");

        var (off, on) = await BothAsync(async () => (await Build().ToListAsync()).Select(r => r.id).ToList());
        on.Should().Equal(off, "the prefilter is a superset and must not change the answer");
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
    //  Membership in a constant set
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public Task In_OnStringField_SameResults() => AssertSameAsync(
        () =>
        {
            var wanted = new[] { "Engineering", "Marketing", "Sales" };
            return Redb.Query<EmployeeProps>()
                .Where(e => wanted.Contains(e.Department))
                .OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(100);
        },
        "membership is the most selective shape after equality and must reach the value scan");

    [Fact]
    public Task In_OnNumericField_SameResults() => AssertSameAsync(
        () =>
        {
            var wanted = new[] { 26, 30, 34, 41 };
            return Redb.Query<EmployeeProps>()
                .Where(e => wanted.Contains(e.Age))
                .OrderBy(e => e.Salary).ThenByRedb(o => o.Id).Take(100);
        },
        "the numeric column has its own partial index, so the branch should be index-served");

    /// <summary>
    /// The shape production tripped over: an empty set. It is legitimately empty, but a branch
    /// rendered from it would be a contradiction, and inside a disjunction it would quietly take the
    /// other branches with it. The planner declines instead, and the answer must still be right.
    /// </summary>
    [Fact]
    public Task In_EmptySet_SameResults() => AssertSameAsync(
        () =>
        {
            var wanted = Array.Empty<string>();
            return Redb.Query<EmployeeProps>()
                .Where(e => wanted.Contains(e.Department))
                .OrderByRedb(o => o.Id).Take(100);
        },
        "an empty set matches nothing and must do so with the prefilter on as well as off");

    /// <summary>
    /// Three conditions, because `a &amp;&amp; b &amp;&amp; c` parses as a tree rather than as one node with
    /// three operands. Before the planner flattened nested conjunctions the middle node was not a
    /// leaf, the third condition was lost, and coverage then refused the whole plan.
    /// </summary>
    [Fact]
    public Task And_ThreeFields_SameResults() => AssertSameAsync(
        () => Redb.Query<EmployeeProps>()
            .Where(e => e.Department == "Engineering" && e.Age > 25 && e.Salary > 60000m)
            .OrderByRedb(o => o.Id).Take(100),
        "every conjunct becomes a branch, and a conjunct nested by the parser must not be lost");

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

    /// <summary>
    /// An entry of a dictionary addressed by its key (PhoneDirectory["desk"]) is one row per object: the streams join
    /// it by the key instead of grouping every row of the dictionary in the scheme (E060, 100 000 employees, a page of
    /// 100: 270-300 ms -> 90-100 ms). Asserted on the SQL; the answers must not change.
    /// </summary>
    [Fact]
    public async Task DictionaryEntryByKey_ReadByStreams_SameAnswers()
    {
        await TestDataFactory.SeedEmployees(Redb, 40);
        // An employee without the dictionary has no key in it: both forms must count it under !ContainsKey.
        var withoutDirectory = TestDataFactory.CreateEmployee(9201);
        withoutDirectory.Props.PhoneDirectory = null;
        await Redb.SaveAsync(withoutDirectory);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<EmployeeProps> HasDesk() => Redb.Query<EmployeeProps>()
                .Where(e => e.PhoneDirectory!.ContainsKey("desk"))
                .OrderByRedb(o => o.Id).Take(10);
            IRedbQueryable<EmployeeProps> DeskIs() => Redb.Query<EmployeeProps>()
                .Where(e => e.PhoneDirectory!["desk"] == "555-1003");

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            (await HasDesk().ToSqlStringAsync()).Should().Contain("-- PVT form: Streams,",
                "a dictionary entry by its key maps to one row per object");

            async Task<(List<long> Page, int Count, int Missing)> AnswersAsync() => (
                (await HasDesk().ToListAsync()).Select(o => o.Id).ToList(),
                await DeskIs().CountAsync(),
                await Redb.Query<EmployeeProps>().Where(e => !e.PhoneDirectory!.ContainsKey("absent-key")).CountAsync());

            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Page.Should().Equal(off.Page);
            on.Page.Should().NotBeEmpty();
            on.Count.Should().Be(off.Count);
            on.Missing.Should().Be(off.Missing);
            on.Missing.Should().Be(await Redb.Query<EmployeeProps>().CountAsync(), "no employee has the key absent-key");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// SQL Server and SQLite: MIN and MAX without any condition take the first entry of the index of values of each field in
    /// its order, the trash subtracted; a batch with SUM or AVG, a filter or WhereRedb keeps the streams form. The employees
    /// holding the highest salary and the lowest age are moved to the trash, so the first entries of those indexes belong to
    /// trashed objects. Answers must equal the ones with the prefilter off for a decimal, an integer, a nullable double, a
    /// date, a nested-class field, base fields with COUNT(*), and an empty scheme.
    /// </summary>
    protected async Task UnfilteredExtremes_FirstEntryOfTheIndex_SameAnswersAsync()
    {
        await TestDataFactory.SeedEmployees(Redb, 30);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            IRedbQueryable<EmployeeProps> All() => Redb.Query<EmployeeProps>();
            var richest = (await All().OrderByDescending(e => e.Salary).ThenByRedb(o => o.Id).Take(1).ToListAsync()).Single().Id;
            var youngest = (await All().OrderBy(e => e.Age).ThenByRedb(o => o.Id).Take(1).ToListAsync()).Single().Id;
            await Redb.SoftDeleteAsync(new[] { richest, youngest });
            await Redb.SyncSchemeAsync<UnfilteredAggregateEmptyProps>();

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            const string firstEntry = "ORDER BY v._numeric DESC";
            (await All().ToAggregateSqlStringAsync(x => new { Mn = Agg.Min(x.Props.Salary), Mx = Agg.Max(x.Props.Salary), C = Agg.Count() }))
                .Should().Contain(firstEntry, "no condition: the extreme is the first entry of the index");
            (await All().ToAggregateSqlStringAsync(x => new { Mx = Agg.Max(x.Props.Salary), S = Agg.Sum(x.Props.Age) }))
                .Should().NotContain(firstEntry, "a SUM in the batch keeps the streams form");
            (await All().Where(e => e.Age > 0).ToAggregateSqlStringAsync(x => new { Mx = Agg.Max(x.Props.Salary) }))
                .Should().NotContain(firstEntry, "a filter keeps the streams form");
            (await All().WhereRedb(o => o.Id > 0).ToAggregateSqlStringAsync(x => new { Mx = Agg.Max(x.Props.Salary) }))
                .Should().NotContain(firstEntry, "WhereRedb keeps the streams form");

            async Task<string> AnswersAsync() => System.Text.Json.JsonSerializer.Serialize(new object[]
            {
                await All().AggregateAsync(x => new { Mn = Agg.Min(x.Props.Salary), Mx = Agg.Max(x.Props.Salary), C = Agg.Count() }),
                await All().AggregateAsync(x => new { Mn = Agg.Min(x.Props.Age), Mx = Agg.Max(x.Props.Age) }),
                // Rating is a double, null on every fourth employee.
                await All().AggregateAsync(x => new { Mn = Agg.Min(x.Props.Rating), Mx = Agg.Max(x.Props.Rating) }),
                await All().AggregateAsync(x => new { Mn = Agg.Min(x.Props.HireDate), Mx = Agg.Max(x.Props.HireDate) }),
                await All().AggregateAsync(x => new { Low = Agg.Min(x.Props.HomeAddress!.Building!.Floor), Top = Agg.Max(x.Props.HomeAddress!.Building!.Floor) }),
                await All().AggregateAsync(x => new { FirstId = Agg.Min(x.Id), LastId = Agg.Max(x.Id), C = Agg.Count(), Mx = Agg.Max(x.Props.Salary) }),
                await All().AggregateAsync(x => new { Mx = Agg.Max(x.Props.Salary), S = Agg.Sum(x.Props.Age) }),
                await Redb.Query<UnfilteredAggregateEmptyProps>().AggregateAsync(x => new
                {
                    MnN = Agg.Min(x.Props.Number), MxA = Agg.Max(x.Props.Amount), MnR = Agg.Min(x.Props.Ratio), C = Agg.Count()
                }),
            });

            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Should().Be(off, "reading the first entry of the index changes the plan, not the answer");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    /// <summary>
    /// SQL Server and SQLite: AllAsync over the whole scheme with a predicate on one plain field first looks for a
    /// counterexample among the first 1 000 objects, then compares the objects of the scheme with the values of the field
    /// that satisfy the predicate. 1 100 objects of a scheme of their own put the counterexamples past the probe, so the
    /// counts decide: another name at the end, an object without a note, the same objects moved to the trash. A Where or a
    /// WhereRedb before AllAsync, a number field and an IN list are compared too. Every answer must equal the one with the
    /// prefilter off.
    /// </summary>
    protected async Task AllWithoutFilter_ProbeThenCounts_SameAnswersAsync()
    {
        var objects = Enumerable.Range(0, 1100).Select(i => (redb.Core.Models.Contracts.IRedbObject<AllTextProbeProps>)
            new redb.Core.Models.Entities.RedbObject<AllTextProbeProps>
            {
                name = $"all-text-{i}",
                // The last object holds another name; the one before it has no note.
                Props = new AllTextProbeProps { Name = i == 1099 ? "b" : "a", Note = i == 1098 ? null : "n" }
            }).ToList();
        var ids = await Redb.AddNewObjectsAsync(objects);
        await TestDataFactory.SeedEmployees(Redb, 20);
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            IRedbQueryable<AllTextProbeProps> Q() => Redb.Query<AllTextProbeProps>();
            IRedbQueryable<EmployeeProps> E() => Redb.Query<EmployeeProps>();
            async Task<string> AnswersAsync() => string.Join(",",
                await Q().AllAsync(p => p.Name == "a"),
                await Q().AllAsync(p => p.Name != "zzz"),
                await Q().AllAsync(p => p.Name.StartsWith("a")),
                await Q().AllAsync(p => p.Note != null),
                await Q().AllAsync(p => p.Note == "n"),
                await Q().AllAsync(p => new[] { "a", "b" }.Contains(p.Name)),
                await Q().Where(p => p.Note == "n").AllAsync(p => p.Name == "a"),
                await Q().WhereRedb(o => o.Id < ids[1099]).AllAsync(p => p.Name == "a"),
                await E().AllAsync(e => e.Age >= 0),
                await E().AllAsync(e => e.Salary > 1_000_000m),
                await E().AllAsync(e => e.Rating != null),
                // Every other narrowing a query carries keeps its own answer: DistinctBy compares keys, not objects.
                await Q().DistinctBy(p => p.Note).AllAsync(p => p.Name == "a"),
                await Q().Distinct().AllAsync(p => p.Name == "a"),
                await Q().Take(5).AllAsync(p => p.Name == "a"),
                await Q().Skip(1090).AllAsync(p => p.Name == "a"),
                await Q().WhereInRedb(o => o.Id, ids.Take(10)).AllAsync(p => p.Name == "a"));

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var on = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await AnswersAsync();
            on.Should().Be(off, "the probe and the counts change the plan, not the answer");
            on.Should().StartWith("False,True,False,False,False,True,False,True",
                "one late other name, one object without a note; Where keeps the object with another name");

            // The objects past the probe moved to the trash: their values stay until the purge and must not count.
            await Redb.SoftDeleteAsync(new[] { ids[1098], ids[1099] });
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var trashedOn = await AnswersAsync();
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var trashedOff = await AnswersAsync();
            trashedOn.Should().Be(trashedOff, "the trash is subtracted from the values counted");
            trashedOn.Should().StartWith("True,True,True,True,True,True,True,True", "the objects left all hold the name and the note");
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }
}
