using redb.Core;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Dictionary querying tests — ContainsKey, indexer, nested.
/// </summary>
public abstract class DictionaryTestsBase
{
    protected readonly IRedbService Redb;

    protected DictionaryTestsBase(IRedbService redb) => Redb = redb;

    private async Task<List<long>> SeedAsync()
    {
        return await TestDataFactory.SeedEmployees(Redb, 20);
    }

    [Fact]
    public async Task DictContainsKey_Filters()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.PhoneDirectory!.ContainsKey("desk"))
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.PhoneDirectory.Should().ContainKey("desk"));
    }

    [Fact]
    public async Task DictIndexer_ValueComparison()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.BonusByYear![2023] > 5000m)
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.BonusByYear![2023].Should().BeGreaterThan(5000m));
    }

    [Fact]
    public async Task DictNestedClassProperty_Filters()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.OfficeLocations!["HQ"].City == "New York")
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.OfficeLocations!["HQ"].City.Should().Be("New York"));
    }

    [Fact]
    public async Task DictTupleKey_Filters()
    {
        var ids = await SeedAsync();

        var reviewKey = (Year: 2024, Quarter: "Q1");
        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.PerformanceReviews![reviewKey] == "Excellent")
            .Take(20)
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.PerformanceReviews![(2024, "Q1")].Should().Be("Excellent"));
    }
}
