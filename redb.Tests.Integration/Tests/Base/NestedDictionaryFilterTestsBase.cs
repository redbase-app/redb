using System.Linq.Expressions;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Filters on a field of a class stored as a dictionary value (<c>DepartmentHistory["2024"].Name</c>,
/// <c>OfficeLocations["HQ"].City</c>), compared with the same LINQ over the seeded objects in memory. The entry of a
/// dictionary has children of several structures under one parent row: scalars (Name, HeadCount), an array
/// (Projects), a nested class (Building). The engines join the children of the entry by parent and structure; each
/// case here has children of other structures next to the one it reads, so a join that picked the wrong child or lost
/// the entry would change the answer. A condition on the array of an entry (Projects) is refused by every engine and is
/// not part of this set.
/// </summary>
public abstract class NestedDictionaryFilterTestsBase
{
    protected readonly IRedbService Redb;

    protected NestedDictionaryFilterTestsBase(IRedbService redb) => Redb = redb;

    private async Task<(string Tag, List<EmployeeProps> Props)> SeedAsync()
    {
        await Redb.SyncSchemeAsync<EmployeeProps>();
        var tag = "nd-" + Guid.NewGuid().ToString("N")[..8];
        var props = new List<EmployeeProps>();
        for (var i = 0; i < 6; i++)
        {
            var p = TestDataFactory.CreateEmployee(i + 1, firstName: $"{tag}-{i}", department: tag).Props;
            p.DepartmentHistory = new Dictionary<string, TestDepartment>
            {
                ["2024"] = new() { Name = i % 2 == 0 ? $"{tag}-even" : $"{tag}-odd", HeadCount = i * 10, Projects = i % 3 == 0 ? ["p1", "p2"] : ["p3"] },
                ["2023"] = new() { Name = $"{tag}-even", HeadCount = 100 + i, Projects = ["p1"] }
            };
            p.OfficeLocations = new Dictionary<string, TestAddress>
            {
                ["HQ"] = new() { City = i < 4 ? $"{tag}-city" : $"{tag}-other", Street = $"s{i}", Building = new TestBuildingInfo { Floor = i, Name = $"b{i}" } },
                ["Branch"] = new() { City = $"{tag}-city", Street = "branch" }
            };
            await Redb.SaveAsync(new RedbObject<EmployeeProps> { name = $"{tag}-{i}", Props = p });
            props.Add(p);
        }
        return (tag, props);
    }

    private async Task MatchesLinqAsync(string tag, List<EmployeeProps> seeded, Expression<Func<EmployeeProps, bool>> filter, int expectedCount)
    {
        var expected = seeded.Where(filter.Compile()).Select(p => p.FirstName).ToList();
        expected.Should().HaveCount(expectedCount, "precondition");

        var query = Redb.Query<EmployeeProps>().Where(e => e.Department == tag).Where(filter);
        (await query.ToListAsync()).Select(o => o.Props.FirstName).Should().BeEquivalentTo(expected);
        (await query.CountAsync()).Should().Be(expectedCount);
    }

    [Fact]
    public async Task ScalarOfEntry_NextToChildrenOfOtherStructures()
    {
        var (tag, seeded) = await SeedAsync();
        var even = tag + "-even";
        await MatchesLinqAsync(tag, seeded, e => e.DepartmentHistory!["2024"].Name == even, 3);
    }

    [Fact]
    public async Task TwoScalarsOfOneEntry()
    {
        var (tag, seeded) = await SeedAsync();
        var even = tag + "-even";
        await MatchesLinqAsync(tag, seeded, e => e.DepartmentHistory!["2024"].Name == even && e.DepartmentHistory!["2024"].HeadCount > 10, 2);
    }

    [Fact]
    public async Task ScalarsOfTwoEntriesOfOneDictionary()
    {
        var (tag, seeded) = await SeedAsync();
        var odd = tag + "-odd";
        await MatchesLinqAsync(tag, seeded, e => e.DepartmentHistory!["2024"].Name == odd && e.DepartmentHistory!["2023"].HeadCount > 102, 2);
    }

    [Fact]
    public async Task FieldOfNestedClassValue_NextToNestedClassAndOtherEntry()
    {
        var (tag, seeded) = await SeedAsync();
        var city = tag + "-city";
        await MatchesLinqAsync(tag, seeded, e => e.OfficeLocations!["HQ"].City == city, 4);
    }
}
