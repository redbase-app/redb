using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// HAVING-clause regression suite for GroupBy and GroupByArray queries.
///
/// Runs on all six hosts (PostgreSQL, SQL Server, SQLite; Free and Pro). SQL Server Free joined on
/// 2026-09-24: until then it had no HAVING at all - the provider passed NULL and every group came back -
/// and the missing wrapper is why nobody saw it.
///
/// The seeded fixture is shared across tests; cleanup is performed once at
/// fixture init. To stay stable against accumulated rows the assertions use
/// thresholds (<c>&gt; 0</c>, <c>&gt; 1_000_000</c>) rather than exact counts.
/// </summary>
public abstract class GroupByHavingTestsBase
{
    protected readonly IRedbService Redb;

    protected GroupByHavingTestsBase(IRedbService redb) => Redb = redb;

    private async Task SeedAsync() => await TestDataFactory.SeedEmployees(Redb, 20);

    // ──────────────────────────────────────────────────────────────────
    //  Regular GroupBy + HAVING
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GroupBy_Having_CountGt_PassThrough()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .Having(g => Agg.Count(g) > 0)
            .SelectAsync(g => new
            {
                Department = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Department.Should().NotBeNullOrEmpty();
            r.Count.Should().BeGreaterThan(0);
        });
    }


    [Fact]
    public async Task Having_Branching_DoesNotContaminateSiblingQueries()
    {
        // G-4 (ревью 2026-09-03): Having мутировал this - ветвление заражало сестринскую
        // ветку обоими предикатами.
        await SeedAsync();
        var grouped = Redb.Query<EmployeeProps>().GroupBy(e => e.Department);
        var strict = grouped.Having(g => Agg.Count(g) > 1_000_000);
        var loose = grouped.Having(g => Agg.Count(g) > 0);

        var looseRows = await loose.SelectAsync(g => new { Department = g.Key, Cnt = Agg.Count(g) });
        looseRows.Should().NotBeEmpty("предикат сестринской ветки не должен протекать в эту");

        var strictRows = await strict.SelectAsync(g => new { Department = g.Key, Cnt = Agg.Count(g) });
        strictRows.Should().BeEmpty();
    }

    [Fact]
    public async Task Having_NullConstant_IsLoud()
    {
        // G-5 (ревью 2026-09-03): null-константа превращалась в СТРОКУ "null".
        await SeedAsync();
        long? nothing = null;
        var act = async () => await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .Having(g => Agg.Count(g) == nothing)
            .SelectAsync(g => new { g.Key });

        await act.Should().ThrowAsync<NotSupportedException>(
            "сравнение с текстом \"null\" - тихо неверный результат");
    }

    [Fact]
    public async Task GroupBy_Having_CountGt_FiltersAll()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .Having(g => Agg.Count(g) > 1_000_000)
            .SelectAsync(g => new
            {
                Department = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task GroupBy_Having_SumSalary_Threshold()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .Having(g => Agg.Sum(g, e => e.Salary) > 0m)
            .SelectAsync(g => new
            {
                Department = g.Key,
                Total = Agg.Sum(g, e => e.Salary)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r => r.Total.Should().BeGreaterThan(0m));
    }

    [Fact]
    public async Task GroupBy_Having_AndCombinator()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .Having(g => Agg.Count(g) > 0 && Agg.Average(g, e => e.Age) > 0.0)
            .SelectAsync(g => new
            {
                Department = g.Key,
                C = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GroupBy_Having_OrCombinator()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .Having(g => Agg.Count(g) > 1_000_000 || Agg.Count(g) > 0)
            .SelectAsync(g => new
            {
                Department = g.Key,
                C = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GroupBy_Having_NotCombinator()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .Having(g => !(Agg.Count(g) > 1_000_000))
            .SelectAsync(g => new
            {
                Department = g.Key,
                C = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
    }

    // ──────────────────────────────────────────────────────────────────
    //  Array GroupBy + HAVING (Phase 2.G.3)
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GroupByArray_Having_CountGt_PassThrough()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Count(g) > 0)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Type.Should().NotBeNullOrEmpty();
            r.Count.Should().BeGreaterThan(0);
        });
        results.Select(r => r.Type).Should()
            .Contain("email", "TestContact.Type=email group must survive HAVING");
        results.Select(r => r.Type).Should()
            .Contain("phone", "TestContact.Type=phone group must survive HAVING");
    }

    [Fact]
    public async Task GroupByArray_Having_CountGt_FiltersAll()
    {
        await SeedAsync();

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

    [Fact]
    public async Task GroupByArray_Having_OnAnItemField_FiltersTheGroups()
    {
        // The HAVING aggregates an item field the select does not read. SQL Server Free wrote it as
        // MAX([Value]) - a column the query does not have - and failed.
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Max(g, c => c.Value) == "no-such-contact-value")
            .SelectAsync(g => new { Type = g.Key, Count = Agg.Count(g) });

        results.Should().BeEmpty("no contact has that value, so no group's maximum equals it");
    }

    [Fact]
    public async Task GroupByArray_Having_OnAnItemField_KeepsTheMatchingGroups()
    {
        // The same field in the select and in the HAVING: both read one joined column.
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Max(g, c => c.Value) != "no-such-contact-value")
            .SelectAsync(g => new { Type = g.Key, MaxValue = Agg.Max(g, c => c.Value) });

        results.Select(r => r.Type).Should().Contain(new[] { "email", "phone" });
        results.Should().AllSatisfy(r => r.MaxValue.Should().NotBeNullOrEmpty());
    }

    [Fact]
    public async Task GroupByArray_Having_AndCombinator()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Count(g) > 0 && Agg.Count(g) < 1_000_000)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GroupByArray_Having_OrCombinator()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => Agg.Count(g) > 1_000_000 || Agg.Count(g) > 0)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GroupByArray_Having_NotCombinator()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .Having(g => !(Agg.Count(g) > 1_000_000))
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
    }

    // ──────────────────────────────────────────────────────────────────
    //  Array GroupBy WITHOUT HAVING (regression for PVT/inline migration)
    //
    //  Verifies that array GroupBy aggregation works when no HAVING clause
    //  is provided. Historically this path bypassed PVT/inline SQL and went
    //  to the legacy `aggregate_array_grouped` SQL function. After the
    //  migration both branches (HAVING + no-HAVING) share the same code
    //  path; these tests guard against regressions in that change.
    // ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task GroupByArray_NoHaving_CountOnly()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Type.Should().NotBeNullOrEmpty();
            r.Count.Should().BeGreaterThan(0);
        });
        results.Select(r => r.Type).Should()
            .Contain("email", "TestContact.Type=email group must be present without HAVING");
        results.Select(r => r.Type).Should()
            .Contain("phone", "TestContact.Type=phone group must be present without HAVING");
    }

    [Fact]
    public async Task GroupByArray_NoHaving_MultipleAggregates()
    {
        await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupByArray(e => e.Contacts!, c => c.Type)
            .SelectAsync(g => new
            {
                Type = g.Key,
                Count = Agg.Count(g),
                MinValue = Agg.Min(g, c => c.Value),
                MaxValue = Agg.Max(g, c => c.Value)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Type.Should().NotBeNullOrEmpty();
            r.Count.Should().BeGreaterThan(0);
        });
    }
}
