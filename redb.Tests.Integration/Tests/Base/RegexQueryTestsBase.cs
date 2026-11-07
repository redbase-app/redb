using System.Text.RegularExpressions;
using redb.Core;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Regex.IsMatch and Regex.Replace in a LINQ filter run in the database (plan docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md
/// §11). A filter the provider drops would return every employee, so each test asserts that only matching rows came back.
/// LastName is seeded as "Last000".."Last019".
/// </summary>
public abstract class RegexQueryTestsBase
{
    protected readonly IRedbService Redb;

    protected RegexQueryTestsBase(IRedbService redb) => Redb = redb;

    [Fact]
    public async Task IsMatch_FiltersInTheDatabase()
    {
        await TestDataFactory.SeedEmployees(Redb, 20);
        const string pattern = "^Last00[0-2]$";

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => Regex.IsMatch(e.LastName, pattern))
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().OnlyContain(r => Regex.IsMatch(r.Props.LastName, pattern));
    }

    [Fact]
    public async Task IsMatchIgnoringCase_FiltersInTheDatabase()
    {
        await TestDataFactory.SeedEmployees(Redb, 20);
        const string pattern = "^last00[0-2]$";

        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => Regex.IsMatch(e.LastName, pattern, RegexOptions.IgnoreCase))
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().OnlyContain(r => Regex.IsMatch(r.Props.LastName, pattern, RegexOptions.IgnoreCase));
    }

    [Fact]
    public async Task Replace_FiltersInTheDatabase()
    {
        await TestDataFactory.SeedEmployees(Redb, 20);

        // Only "Last001" becomes "X1".
        var hits = await Redb.Query<EmployeeProps>()
            .Where(e => Regex.Replace(e.LastName, "^Last00", "X") == "X1")
            .ToListAsync();

        hits.Should().NotBeEmpty();
        hits.Should().OnlyContain(r => r.Props.LastName == "Last001");
    }
}
