using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// RedbList (lookup/dictionary) CRUD and query tests.
/// </summary>
public abstract class ListTestsBase
{
    protected readonly IRedbService Redb;
    protected virtual bool IsPro => false;

    protected ListTestsBase(IRedbService redb) => Redb = redb;

    private async Task<RedbList> CreateTestListAsync(string name = "TestStatuses")
    {
        // Cleanup if exists
        var existing = await Redb.ListProvider.GetListByNameAsync(name);
        if (existing != null)
        {
            var items = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            var itemIds = items.Select(i => i.Id).ToList();
            if (itemIds.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(itemIds);
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }

        var list = RedbList.Create(name, $"{name} alias");
        return await Redb.ListProvider.SaveListAsync(list);
    }

    private async Task CleanupListAsync(string name)
    {
        var existing = await Redb.ListProvider.GetListByNameAsync(name);
        if (existing != null)
        {
            var items = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            var itemIds = items.Select(i => i.Id).ToList();
            if (itemIds.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(itemIds);
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }
    }

    [Fact]
    public async Task CreateList_Persists()
    {
        var saved = await CreateTestListAsync("ListTest_Create");

        saved.Id.Should().BeGreaterThan(0);
        saved.Name.Should().Be("ListTest_Create");

        await CleanupListAsync("ListTest_Create");
    }

    [Fact]
    public async Task GetListByName_ReturnsExisting()
    {
        var saved = await CreateTestListAsync("ListTest_GetByName");

        var found = await Redb.ListProvider.GetListByNameAsync("ListTest_GetByName");

        found.Should().NotBeNull();
        found!.Id.Should().Be(saved.Id);

        await CleanupListAsync("ListTest_GetByName");
    }

    [Fact]
    public async Task AddItems_PersistsItems()
    {
        var list = await CreateTestListAsync("ListTest_Items");

        var values = new[] { "Active", "Inactive", "Pending" };
        var aliases = new[] { "Active status", "Inactive status", "Pending review" };
        var added = await Redb.ListProvider.AddItemsAsync(list, values, aliases);

        added.Should().HaveCount(3);
        added.Select(i => i.Value).Should().BeEquivalentTo(values);

        await CleanupListAsync("ListTest_Items");
    }

    [Fact]
    public async Task GetListItems_ReturnsAll()
    {
        var list = await CreateTestListAsync("ListTest_GetItems");
        await Redb.ListProvider.AddItemsAsync(list, ["A", "B", "C"]);

        var items = await Redb.ListProvider.GetListItemsAsync(list.Id);

        items.Should().HaveCount(3);

        await CleanupListAsync("ListTest_GetItems");
    }

    [Fact]
    public async Task GetListWithItems_LoadsItemsToo()
    {
        var list = await CreateTestListAsync("ListTest_WithItems");
        await Redb.ListProvider.AddItemsAsync(list, ["X", "Y"]);

        var loaded = await Redb.ListProvider.GetListWithItemsAsync(list.Id);

        loaded.Should().NotBeNull();
        loaded!.Items.Should().HaveCount(2);

        await CleanupListAsync("ListTest_WithItems");
    }

    [Fact]
    public async Task GetListItemByValue_FindsMatch()
    {
        var list = await CreateTestListAsync("ListTest_ByValue");
        await Redb.ListProvider.AddItemsAsync(list, ["Alpha", "Beta"]);

        var item = await Redb.ListProvider.GetListItemByValueAsync(list.Id, "Alpha");

        item.Should().NotBeNull();
        item!.Value.Should().Be("Alpha");

        await CleanupListAsync("ListTest_ByValue");
    }

    [Fact]
    public async Task DeleteList_Removes()
    {
        var list = await CreateTestListAsync("ListTest_Delete");

        var deleted = await Redb.ListProvider.DeleteListAsync(list.Id);

        deleted.Should().BeTrue();
        var found = await Redb.ListProvider.GetListByNameAsync("ListTest_Delete");
        found.Should().BeNull();
    }

    [Fact]
    public async Task ListItemOnProps_SaveAndQuery()
    {
        // Setup: create a list with items
        var list = await CreateTestListAsync("ListTest_Props");
        var statuses = await Redb.ListProvider.AddItemsAsync(list, ["Active", "Blocked"]);
        var activeItem = statuses.First(s => s.Value == "Active");
        var blockedItem = statuses.First(s => s.Value == "Blocked");

        // Save a Person with Status = Active
        var person = new RedbObject<PersonProps>
        {
            name = "Test Person",
            Props = new PersonProps
            {
                Name = "John",
                Age = 30,
                Email = "john@test.com",
                Status = activeItem,
                Roles = [activeItem, blockedItem]
            }
        };
        person.id = await Redb.SaveAsync(person);

        // Load and verify
        var loaded = await Redb.LoadAsync<PersonProps>(person.id);
        loaded.Should().NotBeNull();
        loaded!.Props.Status.Should().NotBeNull();
        loaded.Props.Status!.Value.Should().Be("Active");
        loaded.Props.Roles.Should().HaveCount(2);
        await CleanupListAsync("ListTest_Props");
    }

    [Fact]
    public async Task ListItemArray_QueryAny()
    {
        // Setup
        var list = await CreateTestListAsync("ListTest_QueryAny");
        var items = await Redb.ListProvider.AddItemsAsync(list, ["Admin", "User", "Viewer"]);
        var adminItem = items.First(i => i.Value == "Admin");
        var userItem = items.First(i => i.Value == "User");

        var person = new RedbObject<PersonProps>
        {
            name = "Admin User",
            Props = new PersonProps
            {
                Name = "AdminPerson",
                Age = 35,
                Email = "admin@test.com",
                Roles = [adminItem, userItem]
            }
        };
        person.id = await Redb.SaveAsync(person);

        // Query: find persons with Admin role
        var results = await Redb.Query<PersonProps>()
            .Where(p => p.Roles!.Any(r => r.Value == "Admin"))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().Contain(r => r.id == person.id);
        await CleanupListAsync("ListTest_QueryAny");
    }

    [Fact]
    public async Task SyncListFromEnum_CreatesItems()
    {
        // Cleanup if exists
        await CleanupListAsync("TestPriority");

        var list = await Redb.ListProvider.SyncListFromEnumAsync<TestPriority>();

        list.Should().NotBeNull();
        list.Id.Should().BeGreaterThan(0);

        var items = await Redb.ListProvider.GetListItemsAsync(list.Id);
        items.Select(i => i.Value).Should().BeEquivalentTo(["Low", "Medium", "High", "Critical"]);

        await CleanupListAsync("TestPriority");
    }

    [Fact]
    public async Task ListItem_ClosureContains_FiltersCorrectly()
    {
        // Setup: create list with items and save persons with different statuses
        var list = await CreateTestListAsync("ListTest_ClosureContains");
        var statuses = await Redb.ListProvider.AddItemsAsync(list, ["Active", "Blocked", "Pending"]);
        var activeItem = statuses.First(s => s.Value == "Active");
        var blockedItem = statuses.First(s => s.Value == "Blocked");
        var pendingItem = statuses.First(s => s.Value == "Pending");

        var person1 = new RedbObject<PersonProps>
        {
            name = "Person Active",
            Props = new PersonProps { Name = "Alice", Age = 25, Email = "alice@test.com", Status = activeItem }
        };
        var person2 = new RedbObject<PersonProps>
        {
            name = "Person Blocked",
            Props = new PersonProps { Name = "Bob", Age = 30, Email = "bob@test.com", Status = blockedItem }
        };
        var person3 = new RedbObject<PersonProps>
        {
            name = "Person Pending",
            Props = new PersonProps { Name = "Carol", Age = 35, Email = "carol@test.com", Status = pendingItem }
        };

        person1.id = await Redb.SaveAsync(person1);
        person2.id = await Redb.SaveAsync(person2);
        person3.id = await Redb.SaveAsync(person3);

        // Query: find persons where Status IN (Active, Pending) — closure Contains pattern
        var selected = new List<RedbListItem> { activeItem, pendingItem };
        var results = await Redb.Query<PersonProps>()
            .Where(p => selected.Contains(p.Status!))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().Contain(r => r.id == person1.id, "Active person should be found");
        results.Should().Contain(r => r.id == person3.id, "Pending person should be found");
        results.Should().NotContain(r => r.id == person2.id, "Blocked person should NOT be found");

        await CleanupListAsync("ListTest_ClosureContains");
    }

    [Fact]
    public async Task ListItem_ArrayContains_FiltersCorrectly()
    {
        // Setup: create list with items and save person with Roles array
        var list = await CreateTestListAsync("ListTest_ArrayContains");
        var roles = await Redb.ListProvider.AddItemsAsync(list, ["Admin", "User", "Viewer"]);
        var adminItem = roles.First(r => r.Value == "Admin");
        var userItem = roles.First(r => r.Value == "User");
        var viewerItem = roles.First(r => r.Value == "Viewer");

        var person1 = new RedbObject<PersonProps>
        {
            name = "Admin User",
            Props = new PersonProps { Name = "Dave", Age = 40, Email = "dave@test.com", Roles = [adminItem, userItem] }
        };
        var person2 = new RedbObject<PersonProps>
        {
            name = "Viewer Only",
            Props = new PersonProps { Name = "Eve", Age = 28, Email = "eve@test.com", Roles = [viewerItem] }
        };

        person1.id = await Redb.SaveAsync(person1);
        person2.id = await Redb.SaveAsync(person2);

        // Query: x.Roles.Contains(adminItem) — array Contains pattern
        var results = await Redb.Query<PersonProps>()
            .Where(p => p.Roles!.Contains(adminItem))
            .ToListAsync();

        results.Should().NotBeEmpty();
        results.Should().Contain(r => r.id == person1.id, "Person with Admin role should be found");
        results.Should().NotContain(r => r.id == person2.id, "Viewer-only person should NOT be found");

        await CleanupListAsync("ListTest_ArrayContains");
    }

    [Fact]
    public async Task ListItem_OrderByValue_SortsAlphabetically()
    {
        // Setup: create list with items that have known alphabetical order
        var list = await CreateTestListAsync("ListTest_OrderByValue");
        // Values: Alpha < Beta < Gamma (alphabetical order)
        var statuses = await Redb.ListProvider.AddItemsAsync(list, ["Gamma", "Alpha", "Beta"]);
        var gammaItem = statuses.First(s => s.Value == "Gamma");
        var alphaItem = statuses.First(s => s.Value == "Alpha");
        var betaItem = statuses.First(s => s.Value == "Beta");

        var person1 = new RedbObject<PersonProps>
        {
            name = "Person Gamma",
            Props = new PersonProps { Name = "P1", Age = 20, Email = "p1@test.com", Status = gammaItem }
        };
        var person2 = new RedbObject<PersonProps>
        {
            name = "Person Alpha",
            Props = new PersonProps { Name = "P2", Age = 30, Email = "p2@test.com", Status = alphaItem }
        };
        var person3 = new RedbObject<PersonProps>
        {
            name = "Person Beta",
            Props = new PersonProps { Name = "P3", Age = 40, Email = "p3@test.com", Status = betaItem }
        };

        person1.id = await Redb.SaveAsync(person1);
        person2.id = await Redb.SaveAsync(person2);
        person3.id = await Redb.SaveAsync(person3);

        // Query: OrderBy Status.Value — should sort alphabetically by list item text
        var results = await Redb.Query<PersonProps>()
            .Where(p => p.Name == "P1" || p.Name == "P2" || p.Name == "P3")
            .OrderBy(p => p.Status!.Value)
            .ToListAsync();

        results.Should().HaveCount(3);

        if (IsPro)
        {
            // Pro: JOIN _list_items generates correct alphabetical sorting
            var values = results.Select(r => r.Props.Status!.Value).ToList();
            values.Should().BeEquivalentTo(["Alpha", "Beta", "Gamma"],
                opts => opts.WithStrictOrdering(),
                "OrderBy Status.Value should sort alphabetically by list item text");
        }

        await CleanupListAsync("ListTest_OrderByValue");
    }

    [Fact]
    public async Task ListItem_OrderByAlias_SortsAlphabetically()
    {
        // Setup: create list with items that have known aliases in reverse alphabetical order
        var list = await CreateTestListAsync("ListTest_OrderByAlias");
        var values  = new[] { "Status1", "Status2", "Status3" };
        var aliases = new[] { "Cherry",  "Apple",   "Banana"  };
        var statuses = await Redb.ListProvider.AddItemsAsync(list, values, aliases);
        var s1 = statuses.First(s => s.Alias == "Cherry");
        var s2 = statuses.First(s => s.Alias == "Apple");
        var s3 = statuses.First(s => s.Alias == "Banana");

        var person1 = new RedbObject<PersonProps>
        {
            name = "Person Cherry",
            Props = new PersonProps { Name = "A1", Age = 20, Email = "a1@test.com", Status = s1 }
        };
        var person2 = new RedbObject<PersonProps>
        {
            name = "Person Apple",
            Props = new PersonProps { Name = "A2", Age = 30, Email = "a2@test.com", Status = s2 }
        };
        var person3 = new RedbObject<PersonProps>
        {
            name = "Person Banana",
            Props = new PersonProps { Name = "A3", Age = 40, Email = "a3@test.com", Status = s3 }
        };

        person1.id = await Redb.SaveAsync(person1);
        person2.id = await Redb.SaveAsync(person2);
        person3.id = await Redb.SaveAsync(person3);

        // Query: OrderBy Status.Alias — should sort alphabetically by alias
        var results = await Redb.Query<PersonProps>()
            .Where(p => p.Name == "A1" || p.Name == "A2" || p.Name == "A3")
            .OrderBy(p => p.Status!.Alias)
            .ToListAsync();

        results.Should().HaveCount(3);

        if (IsPro)
        {
            // Pro: JOIN _list_items generates correct alphabetical sorting by alias
            var resultAliases = results.Select(r => r.Props.Status!.Alias).ToList();
            resultAliases.Should().BeEquivalentTo(["Apple", "Banana", "Cherry"],
                opts => opts.WithStrictOrdering(),
                "OrderBy Status.Alias should sort alphabetically by list item alias");
        }

        await CleanupListAsync("ListTest_OrderByAlias");
    }
}

public enum TestPriority
{
    Low,
    Medium,
    High,
    Critical
}
