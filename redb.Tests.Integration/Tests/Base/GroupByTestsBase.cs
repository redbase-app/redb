using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

public abstract class GroupByTestsBase
{
    protected readonly IRedbService Redb;

    protected GroupByTestsBase(IRedbService redb) => Redb = redb;

    private async Task<List<long>> SeedAsync()
    {
        return await TestDataFactory.SeedEmployees(Redb, 20);
    }

    [Fact]
    public async Task GroupBy_SingleField_GroupsCorrectly()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .SelectAsync(g => new
            {
                Department = g.Key,
                Count = Agg.Count(g),
                TotalSalary = Agg.Sum(g, e => e.Salary)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Department.Should().NotBeEmpty();
            r.Count.Should().BeGreaterThan(0);
            r.TotalSalary.Should().BeGreaterThan(0m);
        });

        var totalCount = results.Sum(r => r.Count);
        totalCount.Should().BeGreaterThanOrEqualTo(20);
    }

    [Fact]
    public async Task GroupBy_WithFilter_GroupsFiltered()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .Where(e => e.IsRemote)
            .GroupBy(e => e.Department)
            .SelectAsync(g => new
            {
                Department = g.Key,
                AvgSalary = Agg.Average(g, e => e.Salary),
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.AvgSalary.Should().BeGreaterThan(0.0);
            r.Count.Should().BeGreaterThan(0);
        });
    }

    [Fact]
    public async Task GroupBy_WithAggregations_MultipleAggs()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .SelectAsync(g => new
            {
                Department = g.Key,
                MinSalary = Agg.Min(g, e => e.Salary),
                MaxSalary = Agg.Max(g, e => e.Salary),
                AvgAge = Agg.Average(g, e => e.Age),
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.MinSalary.Should().BeLessThanOrEqualTo(r.MaxSalary);
            r.AvgAge.Should().BeGreaterThan(0);
            r.Count.Should().BeGreaterThan(0);
        });
    }

    [Fact]
    public async Task GroupBy_BoolField_GroupsByBoolean()
    {
        var ids = await SeedAsync();

        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.IsRemote)
            .SelectAsync(g => new
            {
                IsRemote = g.Key,
                Count = Agg.Count(g),
                AvgSalary = Agg.Average(g, e => e.Salary)
            });

        results.Should().HaveCountGreaterThanOrEqualTo(1);
        results.Should().HaveCountLessThanOrEqualTo(2);
        results.Sum(r => r.Count).Should().BeGreaterThanOrEqualTo(20);
    }

    [Fact]
    public async Task GroupBy_AliasedKey_ReturnsCorrectValues()
    {
        var ids = await SeedAsync();

        // Key: anonymous property name "Dept" differs from field name "Department"
        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.Department)
            .SelectAsync(g => new
            {
                Dept = g.Key,
                Count = Agg.Count(g)
            });

        results.Should().NotBeEmpty();
        results.Should().AllSatisfy(r =>
        {
            r.Dept.Should().NotBeNullOrEmpty("GroupBy key must resolve even when alias differs from field name");
            r.Count.Should().BeGreaterThan(0);
        });

        results.Sum(r => r.Count).Should().BeGreaterThanOrEqualTo(20);
    }

    [Fact]
    public async Task GroupBy_BoolKey_AliasedKey_Works()
    {
        var ids = await SeedAsync();

        // Key: "Remote" differs from field "IsRemote"
        var results = await Redb.Query<EmployeeProps>()
            .GroupBy(e => e.IsRemote)
            .SelectAsync(g => new
            {
                Remote = g.Key,
                Total = Agg.Count(g)
            });

        results.Should().HaveCountGreaterThanOrEqualTo(1);
        results.Should().HaveCountLessThanOrEqualTo(2);
        results.Sum(r => r.Total).Should().BeGreaterThanOrEqualTo(20);
    }

    #region ListItem GroupBy

    private async Task<(List<long> Ids, RedbListItem Active, RedbListItem Blocked)> SeedPersonsWithStatusAsync()
    {
        // Create list with items
        var existing = await Redb.ListProvider.GetListByNameAsync("GroupByTestStatuses");
        if (existing != null)
        {
            var oldItems = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            if (oldItems.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(oldItems.Select(i => i.Id).ToList());
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }

        var list = RedbList.Create("GroupByTestStatuses", "GroupByTestStatuses");
        list = await Redb.ListProvider.SaveListAsync(list);
        var statuses = await Redb.ListProvider.AddItemsAsync(list, ["Active", "Blocked"]);
        var active = statuses.First(s => s.Value == "Active");
        var blocked = statuses.First(s => s.Value == "Blocked");

        var ids = new List<long>();
        for (int i = 0; i < 10; i++)
        {
            var person = new RedbObject<PersonProps>
            {
                name = $"GroupByPerson_{i}",
                Props = new PersonProps
                {
                    Name = $"Person{i}",
                    Age = 20 + i,
                    Email = $"p{i}@test.com",
                    Status = i % 3 == 0 ? blocked : active
                }
            };
            person.id = await Redb.SaveAsync(person);
            ids.Add(person.id);
        }

        return (ids, active, blocked);
    }

    private async Task CleanupPersonsWithStatusAsync(List<long> ids)
    {
        foreach (var id in ids)
            await Redb.DeleteAsync(id);

        var existing = await Redb.ListProvider.GetListByNameAsync("GroupByTestStatuses");
        if (existing != null)
        {
            var items = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            if (items.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(items.Select(i => i.Id).ToList());
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }
    }

    [Fact]
    public async Task GroupBy_ListItemField_KeyId_ReturnsNonZero()
    {
        var (ids, active, blocked) = await SeedPersonsWithStatusAsync();
        try
        {
            var results = await Redb.Query<PersonProps>()
                .GroupBy(p => p.Status)
                .SelectAsync(g => new
                {
                    StatusId = g.Key!.Id,
                    Count = Agg.Count(g)
                });

            results.Should().NotBeEmpty();
            results.Should().HaveCountGreaterThanOrEqualTo(2, "should have Active and Blocked groups");
            results.Should().AllSatisfy(r =>
            {
                r.StatusId.Should().BeGreaterThan(0, "ListItem Id must not be 0 — was the bug before fix");
                r.Count.Should().BeGreaterThan(0);
            });

            // Verify the Ids match our seeded list items
            var allStatusIds = results.Select(r => r.StatusId).ToList();
            allStatusIds.Should().Contain(active.Id);
            allStatusIds.Should().Contain(blocked.Id);

            results.Sum(r => r.Count).Should().Be(10);
        }
        finally
        {
            await CleanupPersonsWithStatusAsync(ids);
        }
    }

    [Fact]
    public async Task GroupBy_ListItemField_KeyId_AliasedProperty_Works()
    {
        var (ids, active, blocked) = await SeedPersonsWithStatusAsync();
        try
        {
            // Anonymous property "Id" differs from what we name it ("StatusCode")
            var results = await Redb.Query<PersonProps>()
                .GroupBy(p => p.Status)
                .SelectAsync(g => new
                {
                    StatusCode = g.Key!.Id,
                    Total = Agg.Count(g)
                });

            results.Should().NotBeEmpty();
            results.Should().AllSatisfy(r =>
            {
                r.StatusCode.Should().BeGreaterThan(0);
                r.Total.Should().BeGreaterThan(0);
            });

            results.Sum(r => r.Total).Should().Be(10);
        }
        finally
        {
            await CleanupPersonsWithStatusAsync(ids);
        }
    }

    #endregion
}
