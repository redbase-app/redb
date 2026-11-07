using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// An object without Props - a scheme with no properties, or an object saved with <c>Props = null</c> - is a loaded
/// object like any other: it lives in its base fields (Identity report REPORT-CORE-UNLOADED-REFERENCE-PROPSLESS-SCHEME,
/// 2026-09-17). Hydration never called the Props setter for it, so the "loaded" flag stayed down and the save's
/// refusal of unloaded references (fbf4e9f1) took every such object for a reference. What redb materializes as a
/// root is loaded by definition; a reference is a stub redb attached a lazy loader to.
/// </summary>
public abstract class ValueOnlyObjectSaveTestsBase
{
    protected readonly IRedbService Redb;

    protected ValueOnlyObjectSaveTestsBase(IRedbService redb) => Redb = redb;

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    [Fact]
    public async Task AnObjectOfASchemeWithoutProperties_LoadsAsLoaded_AndSavesAgain()
    {
        await Redb.SyncSchemeAsync<ValueOnlyProps>();
        var tag = NewTag();
        var id = await Redb.SaveAsync(new RedbObject<ValueOnlyProps> { name = $"flag-{tag}", Props = new ValueOnlyProps() });

        var loaded = (await Redb.LoadAsync<ValueOnlyProps>(id))!;
        loaded.IsPropsLoaded.Should().BeTrue("what redb materialized as a root is loaded, Props or no Props");

        loaded.name = $"flag-{tag}-renamed";
        await Redb.SaveAsync(loaded);

        (await Redb.LoadAsync<ValueOnlyProps>(id))!.name.Should().Be($"flag-{tag}-renamed");
    }

    [Fact]
    public async Task AnObjectOfASchemeWithoutProperties_FromAQuery_SavesAgain()
    {
        await Redb.SyncSchemeAsync<ValueOnlyProps>();
        var tag = NewTag();
        var id = await Redb.SaveAsync(new RedbObject<ValueOnlyProps> { name = $"queried-{tag}", Props = new ValueOnlyProps() });

        var queried = (await Redb.Query<ValueOnlyProps>().ToListAsync()).Single(o => o.id == id);
        queried.IsPropsLoaded.Should().BeTrue();

        queried.name = $"queried-{tag}-renamed";
        await Redb.SaveAsync(queried);

        (await Redb.LoadAsync<ValueOnlyProps>(id))!.name.Should().Be($"queried-{tag}-renamed");
    }

    [Fact]
    public async Task AnObjectSavedWithNullProps_LoadsAsLoaded_AndSavesAgain()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var tag = NewTag();
        var id = await Redb.SaveAsync(new RedbObject<SimpleProps> { name = $"nullprops-{tag}", Props = null! });

        var loaded = (await Redb.LoadAsync<SimpleProps>(id))!;
        loaded.IsPropsLoaded.Should().BeTrue("Props may be null on a loaded object");

        loaded.name = $"nullprops-{tag}-renamed";
        await Redb.SaveAsync(loaded);

        (await Redb.LoadAsync<SimpleProps>(id))!.name.Should().Be($"nullprops-{tag}-renamed");
    }
}
