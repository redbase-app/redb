using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Regression suite for bugs surfaced by <c>redb.Examples</c> cases
/// E110, E111, E142, E150 and E200.
///
/// All tests are written so that fixing the corresponding bug makes the
/// test pass; a failing test is a real regression that has to be fixed
/// either in core (parser / projection extractor) or in the provider
/// (PVT SQL / Pro inline SQL).
///
/// Targets:
///   * E110 — <c>WhereHasAncestor</c> over a single scheme tree;
///   * E111 — <c>WhereHasDescendant</c> over a single scheme tree;
///   * E142 — <c>GroupByArray</c> with an outer <c>Where</c> filter
///            (free PG used to <c>RAISE EXCEPTION</c>, now compiled
///            via <c>pvt_build_query_sql</c> → IN-list);
///   * E150 — array aggregation via <c>Agg.Sum(arr.Select(s => s))</c>
///            and the indexed variant <c>Agg.Sum(arr[0])</c>;
///   * E200 — string concatenation inside <c>ToLower().Contains(...)</c>,
///            currently broken in <see cref="redb.Core.Query.Parsing.BaseFilterExpressionParser"/>.
/// </summary>
public abstract class BugRegressionTestsBase
{
    protected readonly IRedbService Redb;

    protected BugRegressionTestsBase(IRedbService redb) => Redb = redb;

    private async Task SeedEmployeesAsync() => await TestDataFactory.SeedEmployees(Redb, 20);

    /// <summary>
    /// Tree:  Company(1_000_000) -> Engineering(500_000) -> Backend(200_000)
    ///                                                    -> Frontend(150_000)
    ///                          -> Marketing(300_000)
    /// </summary>
    private async Task<(long root, long eng, long backend, long frontend, long marketing)> SeedTreeAsync()
    {
        var root = TestDataFactory.CreateTreeNode("Company", "BUG-ROOT", 1_000_000m);
        root.id = await Redb.SaveAsync(root);

        var eng = TestDataFactory.CreateTreeNode("Engineering", "BUG-ENG", 500_000m);
        eng.id = await Redb.CreateChildAsync(eng, root);

        var backend = TestDataFactory.CreateTreeNode("Backend", "BUG-BE", 200_000m);
        backend.id = await Redb.CreateChildAsync(backend, eng);

        var frontend = TestDataFactory.CreateTreeNode("Frontend", "BUG-FE", 150_000m);
        frontend.id = await Redb.CreateChildAsync(frontend, eng);

        var marketing = TestDataFactory.CreateTreeNode("Marketing", "BUG-MKT", 300_000m);
        marketing.id = await Redb.CreateChildAsync(marketing, root);

        return (root.id, eng.id, backend.id, frontend.id, marketing.id);
    }

    // ──────────────────────────────────────────────────────────────────
    //  E110 — WhereHasAncestor
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task E110_WhereHasAncestor_ReturnsDescendantsOfMatchingAncestor()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        // Only the root (1_000_000) has Budget > 500_000.
        // Its descendants are: eng, be, fe, mkt — 4 nodes (root itself excluded).
        var ids = await Redb.TreeQuery<TreeNodeProps>()
            .WhereHasAncestor<TreeNodeProps>(anc => anc.Budget > 500_000m)
            .ToListAsync();

        var seeded = new[] { root, eng, be, fe, mkt };
        var hits = ids.Where(o => seeded.Contains(o.id)).Select(o => o.id).ToHashSet();

        hits.Should().NotContain(root, "root has no ancestor with Budget > 500k");
        hits.Should().Contain(new[] { eng, be, fe, mkt }, "all nodes under root must inherit it");
    }

    // ──────────────────────────────────────────────────────────────────
    //  E111 — WhereHasDescendant
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task E111_WhereHasDescendant_ReturnsAncestorsOfMatchingDescendant()
    {
        var (root, eng, be, fe, mkt) = await SeedTreeAsync();

        // All non-root nodes (eng, be, fe, mkt) have Budget > 100_000.
        // Ancestors of any of them are: root and eng (root is ancestor of all four;
        // eng is ancestor of be & fe).
        var ids = await Redb.TreeQuery<TreeNodeProps>()
            .WhereHasDescendant<TreeNodeProps>(desc => desc.Budget > 100_000m)
            .ToListAsync();

        var seeded = new[] { root, eng, be, fe, mkt };
        var hits = ids.Where(o => seeded.Contains(o.id)).Select(o => o.id).ToHashSet();

        hits.Should().Contain(new[] { root, eng }, "root and eng have qualifying descendants");
        hits.Should().NotContain(new[] { be, fe, mkt }, "leaves have no descendants");
    }

    // ──────────────────────────────────────────────────────────────────
    //  E142 — GroupByArray with outer Where (filter on parent object)
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task E142_GroupByArray_WithOuterWhere_ReturnsFilteredGroups()
    {
        await SeedEmployeesAsync();

        // Outer filter: only employees that actually have a Contacts array
        // populated. Pre-fix this raised
        //   P0001: pvt_build_array_groupby_sql: outer p_filter is not yet supported
        // After fix the filter compiles via pvt_build_query_sql and is applied
        // before unnest.
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Contacts != null)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty("outer filter is satisfied by every seeded employee");
        results.Select(r => r.Type).Should().Contain("email");
        results.Select(r => r.Type).Should().Contain("phone");
        results.Should().AllSatisfy(r =>
        {
            r.Type.Should().NotBeNullOrEmpty();
            r.Count.Should().BeGreaterThan(0);
        });
    }

    [Fact]
    public async Task E142_GroupByArray_WithOuterWhere_FilterDropsAll()
    {
        await SeedEmployeesAsync();

        // Outer filter that matches nothing — the SQL must still compile and
        // return an empty set rather than raising or returning all rows.
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName == "__NoSuchEmployee__")
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().BeEmpty("outer filter matches no parent objects");
    }

    // ──────────────────────────────────────────────────────────────────
    //  E142.b — GroupByArray with HAVING on aggregated element column.
    //  Each seeded employee has exactly 2 contacts (email + phone), so the
    //  total count per Type group is 20 (one per employee). A HAVING
    //  > 0 keeps both, HAVING > 10 keeps both, HAVING > 25 keeps none.
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task E142b_GroupByArray_HavingCount_KeepsAllGroups()
    {
        await SeedEmployeesAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Count(g) > 10)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Select(r => r.Type).Should().Contain(new[] { "email", "phone" });
        results.Should().AllSatisfy(r => r.Count.Should().BeGreaterOrEqualTo(20));
    }

    [Fact]
    public async Task E142b_GroupByArray_HavingCount_DropsAllGroups()
    {
        await SeedEmployeesAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Count(g) > 1_000_000)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().BeEmpty();
    }

    // ──────────────────────────────────────────────────────────────────
    //  E142.c — GroupByArray with outer Where on a Props field
    //  (FirstName == seeded value). Expected: 2 groups (email + phone),
    //  each with Count == 1 because only one employee matched the filter.
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task E142c_GroupByArray_WithOuterPropsWhere_GroupsMatchEmployees()
    {
        await SeedEmployeesAsync();

        // Tests share the same DB; account for accumulated rows by computing
        // the matched-employee count dynamically.
        var matched = (await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName == "Employee005")
            .ToListAsync()).Count;
        matched.Should().BeGreaterThan(0);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName == "Employee005")
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().HaveCount(2);
        results.Select(r => r.Type).Should().BeEquivalentTo(new[] { "email", "phone" });
        results.Should().AllSatisfy(r => r.Count.Should().Be(matched),
            "each contact-type group must contain exactly one row per matched employee");
    }

    // ──────────────────────────────────────────────────────────────────
    //  E142.d — GroupByArray with multi-aggregation selector
    //  (Count + Sum/Avg over a numeric element field is not possible
    //  on Contacts; we use Skills array which contains strings only,
    //  so we stick to Count + key, and additionally validate that an
    //  outer Where on base "id" combined with HAVING compiles & runs.)
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task E142d_GroupByArray_OuterWhereAndHaving_Combined()
    {
        await SeedEmployeesAsync();

        var matched = (await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote == true)
            .ToListAsync()).Count;
        matched.Should().BeGreaterThan(0);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote == true)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Count(g) > 0)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Select(r => r.Type).Should().Contain(new[] { "email", "phone" });
        results.Should().AllSatisfy(r => r.Count.Should().Be(matched),
            "outer Where filter must be applied before array aggregation");
    }

    // ──────────────────────────────────────────────────────────────────
    //  E150 — Array aggregation (regression lock: already works)
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task E150_ArrayAggregation_SumAverage_AllElements()
    {
        await SeedEmployeesAsync();

        // Aggregate ALL elements of SkillLevels[] (unnested).
        var all = await Redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                Total = Agg.Sum(x.Props.SkillLevels.Select(s => s)),
                Avg   = Agg.Average(x.Props.SkillLevels.Select(s => s)),
                Count = Agg.Count()
            });

        all.Count.Should().BeGreaterThan(0);
        all.Total.Should().BeGreaterThan(0);
        all.Avg.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task E150_ArrayAggregation_SumAverage_SpecificIndex()
    {
        await SeedEmployeesAsync();

        // Aggregate only the first element of SkillLevels[] (indexed access).
        var first = await Redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                SumFirst = Agg.Sum(x.Props.SkillLevels[0]),
                AvgFirst = Agg.Average(x.Props.SkillLevels[0])
            });

        first.SumFirst.Should().BeGreaterThan(0);
        first.AvgFirst.Should().BeGreaterThan(0);
    }

    // ──────────────────────────────────────────────────────────────────
    //  E200 — string concatenation inside ToLower().Contains(...)
    //  Currently broken in BaseFilterExpressionParser.ExtractProperty:
    //    "Expression must be a property access, got MethodBinaryExpression"
    //  The test asserts the expression builds AND returns at least one match
    //  against the seeded data.
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task E200_StringConcat_ToLowerContains_ParsesAndExecutes()
    {
        await SeedEmployeesAsync();

        // Seeded names look like "Employee000 Last000"..; lower-case contains "employee" always matches.
        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => (e.FirstName + " " + e.LastName).ToLower().Contains("employee"))
            .ToListAsync();

        hits.Should().NotBeEmpty(
            "string concatenation inside ToLower().Contains(...) must compile to SQL `||` + ILIKE");
    }
}
