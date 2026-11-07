using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

public abstract class CrudTestsBase
{
    protected readonly IRedbService Redb;

    protected CrudTestsBase(IRedbService redb) => Redb = redb;

    [Fact]
    public async Task Save_NewObject_ReturnsPositiveId()
    {
        var obj = TestDataFactory.CreateSimple("CRUD-Save", 42.50m);
        obj.id = await Redb.SaveAsync(obj);

        obj.id.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Load_AfterSave_ReturnsSameData()
    {
        var obj = TestDataFactory.CreateSimple("CRUD-Load", 123.45m);
        obj.id = await Redb.SaveAsync(obj);

        var loaded = await Redb.LoadAsync<SimpleProps>(obj.id);

        loaded.Should().NotBeNull();
        loaded!.Props.Title.Should().Be("CRUD-Load");
        loaded.Props.Price.Should().Be(123.45m);
        loaded.Props.Count.Should().Be(10);
        loaded.Props.IsActive.Should().BeTrue();
        loaded.Props.CreatedAt.Should().Be(new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc));
        loaded.Props.Code.Should().Be(obj.Props.Code);
        loaded.Props.Description.Should().Be("Test description");
    }

    [Fact]
    public async Task Update_ExistingObject_PersistsChanges()
    {
        var obj = TestDataFactory.CreateSimple("CRUD-Update", 100m);
        obj.id = await Redb.SaveAsync(obj);

        var loaded = await Redb.LoadAsync<SimpleProps>(obj.id);
        loaded!.Props.Title = "CRUD-Updated";
        loaded.Props.Price = 200m;
        loaded.Props.IsActive = false;
        await Redb.SaveAsync(loaded);

        var updated = await Redb.LoadAsync<SimpleProps>(obj.id);
        updated!.Props.Title.Should().Be("CRUD-Updated");
        updated.Props.Price.Should().Be(200m);
        updated.Props.IsActive.Should().BeFalse();
    }

    [Fact]
    public async Task Delete_SingleObject_Removes()
    {
        var obj = TestDataFactory.CreateSimple("CRUD-Delete");
        obj.id = await Redb.SaveAsync(obj);

        var result = await Redb.DeleteAsync(obj.id);

        result.Should().BeTrue();
        var loaded = await Redb.LoadAsync<SimpleProps>(obj.id);
        loaded.Should().BeNull();
    }

    [Fact]
    public async Task Delete_NonExistent_ReturnsFalse()
    {
        var result = await Redb.DeleteAsync(999_999_999L);
        result.Should().BeFalse();
    }

    [Fact]
    public async Task Save_WithNullableFields_HandlesNulls()
    {
        var obj = new RedbObject<SimpleProps>
        {
            name = "CRUD-Nulls",
            Props = new SimpleProps
            {
                Title = "Nulls",
                Count = 0,
                Price = 0m,
                CreatedAt = DateTime.UtcNow,
                IsActive = false,
                Code = Guid.Empty,
                Description = null,
                OptionalNumber = null
            }
        };
        obj.id = await Redb.SaveAsync(obj);

        var loaded = await Redb.LoadAsync<SimpleProps>(obj.id);
        loaded!.Props.Description.Should().BeNull();
        loaded.Props.OptionalNumber.Should().BeNull();
    }

    [Fact]
    public async Task Save_WithNestedObject_RoundTrips()
    {
        var emp = TestDataFactory.CreateEmployee(0, city: "Berlin");
        emp.id = await Redb.SaveAsync(emp);

        var loaded = await Redb.LoadAsync<EmployeeProps>(emp.id);
        loaded!.Props.HomeAddress.Should().NotBeNull();
        loaded.Props.HomeAddress!.City.Should().Be("Berlin");
        loaded.Props.HomeAddress.Street.Should().NotBeEmpty();
        loaded.Props.HomeAddress.Building.Should().NotBeNull();
        loaded.Props.HomeAddress.Building!.Floor.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Save_WithDeepNested_RoundTrips()
    {
        var emp = TestDataFactory.CreateEmployee(0);
        emp.id = await Redb.SaveAsync(emp);

        var loaded = await Redb.LoadAsync<EmployeeProps>(emp.id);
        loaded!.Props.HomeAddress!.Building!.Name.Should().NotBeEmpty();
        loaded.Props.HomeAddress.Building.Amenities.Should().Contain("Parking");
    }

    [Fact]
    public async Task Save_WithContacts_ArrayOfObjectsRoundTrips()
    {
        var emp = TestDataFactory.CreateEmployee(0);
        emp.id = await Redb.SaveAsync(emp);

        var loaded = await Redb.LoadAsync<EmployeeProps>(emp.id);
        loaded!.Props.Contacts.Should().HaveCount(2);
        loaded.Props.Contacts![0].Type.Should().Be("email");
        loaded.Props.Contacts[0].IsVerified.Should().BeTrue();
        loaded.Props.Contacts[1].Type.Should().Be("phone");
    }

    [Fact]
    public async Task BulkSave_MultipleObjects_AllPersisted()
    {
        var objects = Enumerable.Range(0, 10)
            .Select(i => TestDataFactory.CreateSimple($"Bulk-{i}", i * 10m))
            .Cast<RedbObject<SimpleProps>>()
            .ToList();

        var ids = await Redb.SaveAsync(objects);
        for (int i = 0; i < objects.Count; i++) objects[i].id = ids[i];

        var count = await Redb.Query<SimpleProps>()
            .Where(s => s.Title.Contains("Bulk-"))
            .CountAsync();

        count.Should().BeGreaterThanOrEqualTo(10);
    }

    [Fact]
    public async Task Delete_Batch_RemovesAll()
    {
        var objects = Enumerable.Range(0, 5)
            .Select(i => TestDataFactory.CreateSimple($"BatchDel-{i}"))
            .Cast<RedbObject<SimpleProps>>()
            .ToList();
        var ids = await Redb.SaveAsync(objects);

        var deleted = await Redb.DeleteAsync(ids);

        deleted.Should().Be(5);
        foreach (var id in ids)
        {
            var loaded = await Redb.LoadAsync<SimpleProps>(id);
            loaded.Should().BeNull();
        }
    }

    [Fact]
    public async Task Save_NonGenericRedbObject_BaseFieldsOnly()
    {
        var obj = new RedbObject
        {
            name = "NonGeneric-Test",
            value_string = "hello",
            value_long = 42,
            value_bool = true
        };
        obj.id = await Redb.SaveAsync(obj);

        obj.id.Should().BeGreaterThan(0);
    }
}
