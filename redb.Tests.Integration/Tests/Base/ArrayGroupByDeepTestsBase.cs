using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Deep-coverage integration tests for <c>GroupByArray</c>.
///
/// Complements <see cref="GroupByHavingTestsBase"/> (HAVING basics) and the
/// E142* slice of <see cref="BugRegressionTestsBase"/> by exercising:
///   * composite array keys (anonymous { Type, IsVerified });
///   * outer Where combined with nested AND/OR;
///   * outer Where with string operations (Contains / StartsWith) on parent props;
///   * outer WhereIn on base.id;
///   * boolean array keys.
///
/// Targets the same dialect set as <see cref="GroupByHavingTestsBase"/>:
/// free PG, Pro PG, Pro MSSql. Free MSSql has no array-groupby + HAVING path.
/// </summary>
public abstract class ArrayGroupByDeepTestsBase
{
    protected readonly IRedbService Redb;

    protected ArrayGroupByDeepTestsBase(IRedbService redb) => Redb = redb;

    private async Task SeedAsync() => await TestDataFactory.SeedEmployees(Redb, 20);

    // ──────────────────────────────────────────────────────────────────
    //  Composite array key (anonymous { Type, IsVerified })
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_CompositeKey_TypeAndVerified_FourGroupsMax()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => new { c.Type, c.IsVerified })
            .SelectAsync(g => new
            {
                g.Key.Type,
                g.Key.IsVerified,
                Count = Agg.Count(g)
            });

        // Seed: every employee has exactly two contacts —
        //   { Type=email, IsVerified=true } and { Type=phone, IsVerified=false }.
        // So there must be exactly 2 distinct composite groups, each non-empty.
        results.Should().HaveCount(2);
        results.Should().Contain(r => r.Type == "email" && r.IsVerified);
        results.Should().Contain(r => r.Type == "phone" && !r.IsVerified);
        results.Should().AllSatisfy(r => r.Count.Should().BeGreaterThan(0));
    }

    [Fact]
    public async Task ArrayGroupBy_CompositeKey_Having_PositiveThreshold_Survives()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => new { c.Type, c.IsVerified })
            .Having(g => Agg.Count(g) > 0)
            .SelectAsync(g => new
            {
                g.Key.Type,
                g.Key.IsVerified,
                C = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Select(r => r.Type).Distinct().Should().BeEquivalentTo(new[] { "email", "phone" });
    }

    [Fact]
    public async Task ArrayGroupBy_CompositeKey_Having_ImpossibleThreshold_DropsAll()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => new { c.Type, c.IsVerified })
            .Having(g => Agg.Count(g) > 1_000_000)
            .SelectAsync(g => new { g.Key.Type, C = Agg.Count(g) });

        results.Should().BeEmpty();
    }

    // ──────────────────────────────────────────────────────────────────
    //  Outer Where with nested AND/OR over parent props
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_OuterWhere_NestedAndOr_Combined()
    {
        await SeedAsync();

        // Seed has 20 employees, departments cycled through 5 deps, IsRemote = (i%2==0).
        // (Department == "Engineering" && IsRemote) OR (Department == "Sales" && !IsRemote)
        // produces a non-empty subset; assert non-empty groups + each group count > 0.
        var matched = (await Redb.Query<EmployeeProps>()
            .Where(e => (e.Department == "Engineering" && e.IsRemote)
                     || (e.Department == "Sales" && !e.IsRemote))
            .ToListAsync()).Count;
        matched.Should().BeGreaterThan(0, "seed must produce some matches for the compound filter");

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => (e.Department == "Engineering" && e.IsRemote)
                     || (e.Department == "Sales" && !e.IsRemote))
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.C.Should().Be(matched,
            "every contact type must produce exactly matched-employee rows after array unnest"));
    }

    // ──────────────────────────────────────────────────────────────────
    //  Outer Where with string ops (Contains / StartsWith) on parent props
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_OuterWhere_StringContains_OnDepartment()
    {
        await SeedAsync();

        var matched = (await Redb.Query<EmployeeProps>()
            .Where(e => e.Department.Contains("ing"))   // Engineering, Marketing
            .ToListAsync()).Count;
        matched.Should().BeGreaterThan(0);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Department.Contains("ing"))
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().NotBeEmpty();
        results.Select(r => r.Type).Should().BeEquivalentTo(new[] { "email", "phone" });
        results.Should().AllSatisfy(r => r.C.Should().Be(matched));
    }

    [Fact]
    public async Task ArrayGroupBy_OuterWhere_StringStartsWith_OnFirstName()
    {
        await SeedAsync();

        var matched = (await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.StartsWith("Employee00"))
            .ToListAsync()).Count;
        matched.Should().BeGreaterThan(0);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.StartsWith("Employee00"))
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.C.Should().Be(matched));
    }

    // ──────────────────────────────────────────────────────────────────
    //  Outer Where narrows to a single employee — exact contact-type cardinality
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_OuterWhere_UniqueFirstName_ExactCardinality()
    {
        await SeedAsync();

        // Seeders are cumulative across tests in the suite, so "Employee001"
        // may exist N≥1 times. Compute expected cardinality from data.
        var matched = (await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName == "Employee001")
            .ToListAsync()).Count;
        matched.Should().BeGreaterThan(0);

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.FirstName == "Employee001")
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().HaveCount(2, "each matched employee has exactly 2 distinct contact types (email + phone)");
        results.Should().AllSatisfy(r => r.C.Should().Be(matched));
    }

    // ──────────────────────────────────────────────────────────────────
    //  Boolean array key (IsVerified on its own)
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_BoolKey_IsVerified_TwoGroups()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.IsVerified)
            .SelectAsync(g => new { Verified = g.Key, C = Agg.Count(g) });

        // Seed has both verified=true (email) and verified=false (phone) — 2 distinct.
        results.Should().HaveCount(2);
        results.Select(r => r.Verified).Should().BeEquivalentTo(new[] { true, false });
        results.Should().AllSatisfy(r => r.C.Should().BeGreaterThan(0));
    }

    // ──────────────────────────────────────────────────────────────────
    //  Empty / null arrays — outer filter must exclude employees w/o array
    // ──────────────────────────────────────────────────────────────────
    [Fact]
    public async Task ArrayGroupBy_OuterWhere_ContactsNotNull_StillProducesGroups()
    {
        await SeedAsync();

        // All seeded employees have Contacts; this is essentially a smoke regression
        // for the IS NOT NULL outer filter, which used to be rejected by free PG.
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Contacts != null)
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new { Type = g.Key, C = Agg.Count(g) });

        results.Should().NotBeEmpty();
        results.Select(r => r.Type).Should().BeEquivalentTo(new[] { "email", "phone" });
    }
}
