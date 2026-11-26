using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The props depth boundary of the Pro loader. With <c>WithPropsDepth(1)</c> every reference of the page comes back as
/// a stub: the referenced object's base fields, no props, a loader for the first access. The loader read the values of
/// the referenced objects (and their list items and the types) before its depth check and then dropped them, so a page
/// of stubs paid for a page of loaded references. A reference at the boundary costs the read of its base fields only.
/// </summary>
public abstract class PropsDepthBoundaryTestsBase
{
    protected readonly IRedbService Redb;

    protected PropsDepthBoundaryTestsBase(IRedbService redb) => Redb = redb;

    private async Task<long> SaveNodeAsync(string label, long? nextId) => await Redb.SaveAsync(new RedbObject<LazyNodeProps>
    {
        name = label,
        Props = new LazyNodeProps { Label = label, Next = nextId is { } id ? new RedbObject<LazyNodeProps> { id = id } : null }
    });

    private Task<List<RedbObject<LazyNodeProps>>> PageAsync(long id, int? propsDepth)
    {
        var query = Redb.Query<LazyNodeProps>().WhereRedb(o => o.Id == id);
        return (propsDepth is { } depth ? query.WithPropsDepth(depth) : query).ToListAsync();
    }

    [Fact]
    public async Task AtDepthOne_ReferenceIsAStub_AndItsValuesAreNotRead()
    {
        await Redb.SyncSchemeAsync<LazyNodeProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];
        var leafId = await SaveNodeAsync($"depth-leaf-{tag}", null);
        var withReference = await SaveNodeAsync($"depth-with-{tag}", leafId);
        var withoutReference = await SaveNodeAsync($"depth-without-{tag}", null);

        // Once each first: the scheme and type caches are filled before anything is counted.
        await PageAsync(withReference, 1);
        await PageAsync(withoutReference, 1);

        List<RedbObject<LazyNodeProps>>? page = null;
        var withCommands = await Helpers.CommandCounter.CountWithoutScalarsAsync(async () => page = await PageAsync(withReference, 1));
        var withoutCommands = await Helpers.CommandCounter.CountWithoutScalarsAsync(() => PageAsync(withoutReference, 1));

        var stub = page!.Single().Props.Next;
        stub.Should().NotBeNull("the reference is handed out at the boundary");
        stub!.IsPropsLoaded.Should().BeFalse("props depth 1 leaves the reference a stub");
        stub.GetPropsDirectly().Should().BeNull("a stub carries no props");
        stub.id.Should().Be(leafId);
        stub.name.Should().Be($"depth-leaf-{tag}", "the stub carries the base fields of the referenced object");
        (withCommands - withoutCommands).Should().Be(1,
            "a reference at the boundary costs the read of its base fields, not of its values, list items and types");

        stub.Props!.Label.Should().Be($"depth-leaf-{tag}", "the stub still loads its props on first access");
    }

    [Fact]
    public async Task AtDefaultDepth_ReferenceIsLoaded()
    {
        await Redb.SyncSchemeAsync<LazyNodeProps>();
        var tag = Guid.NewGuid().ToString("N")[..8];
        var leafId = await SaveNodeAsync($"depth-leaf-{tag}", null);
        var withReference = await SaveNodeAsync($"depth-with-{tag}", leafId);

        var reference = (await PageAsync(withReference, null)).Single().Props.Next;

        reference.Should().NotBeNull();
        reference!.IsPropsLoaded.Should().BeTrue("below the boundary the referenced object is materialised");
        reference.GetPropsDirectly()!.Label.Should().Be($"depth-leaf-{tag}");
    }
}
