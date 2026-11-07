using redb.Core;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Array querying tests — e.Skills.Contains("C#"), etc.
/// </summary>
public abstract class ArrayTestsBase
{
    protected readonly IRedbService Redb;

    protected ArrayTestsBase(IRedbService redb) => Redb = redb;

    private async Task<List<long>> SeedAsync()
    {
        return await TestDataFactory.SeedEmployees(Redb, 20);
    }

    [Fact]
    public async Task ArrayContains_SingleValue_Filters()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null && e.Skills.Contains("C#"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
            r.Props.Skills.Should().Contain("C#"));
    }

    [Fact]
    public async Task ArrayContains_Or_MatchesEither()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null &&
                (e.Skills.Contains("C#") || e.Skills.Contains("Python")))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            var skills = r.Props.Skills!;
            (skills.Contains("C#") || skills.Contains("Python")).Should().BeTrue();
        });
    }

    [Fact]
    public async Task ArrayContains_Not_ExcludesValue()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null && e.Skills.Contains("C#") && !e.Skills.Contains("Python"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Props.Skills.Should().Contain("C#");
            r.Props.Skills.Should().NotContain("Python");
        });
    }

    [Fact]
    public async Task ArrayContains_And_MustHaveAll()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null &&
                e.Skills.Contains("C#") && e.Skills.Contains("SQL"))
            .ToListAsync();

        results.Should().AllSatisfy(r =>
        {
            r.Props.Skills.Should().Contain("C#");
            r.Props.Skills.Should().Contain("SQL");
        });
    }

    [Fact]
    public async Task ArrayContains_WithScalarFilter_Combined()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.Age > 30 && e.Skills != null && e.Skills.Contains("C#"))
            .ToListAsync();

        results.Should().AllSatisfy(r =>
        {
            r.Props.Age.Should().BeGreaterThan(30);
            r.Props.Skills.Should().Contain("C#");
        });
    }
}
