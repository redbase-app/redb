using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Core.Utils;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Reflective walkers must treat RedbListItem as a LEAF (production stand, 2026-09-09): the
/// item's lazy Object getter LOADS on read, and the loader-install walk read every property of
/// every business object - including Object - so materializing anything with list-item fields
/// fired a synchronous phantom load per item (~150/s on the stand, with not a single explicit
/// .Object anywhere in the application code).
/// </summary>
public class LazyWalkersListItemLeafTests
{
    private sealed class PropsWithItem
    {
        public string Label { get; set; } = "";
        public RedbListItem? Status { get; set; }
    }

    private static (RedbObject<PropsWithItem> root, RedbListItem item, Counter loads) Make()
    {
        var loads = new Counter();
        var item = new RedbListItem { Id = 5, IdList = 1, Value = "status", IdObject = 42 };
        item.AttachObjectLoader(id =>
        {
            loads.Value++;
            return Task.FromResult<IRedbObject?>(new RedbObject { id = id, name = "phantom" });
        });
        var root = new RedbObject<PropsWithItem>
        {
            id = 1,
            scheme_id = 100,
            Props = new PropsWithItem { Label = "root", Status = item },
        };
        return (root, item, loads);
    }

    private sealed class Counter { public int Value; }

    [Fact]
    public void LoaderInstall_DoesNotWakeTheItemsObject()
    {
        var (root, item, loads) = Make();

        LazyReferenceInstaller.Install(root, new NoopLoader());

        loads.Value.Should().Be(0,
            "installing lazy loaders is a wiring walk - it must never trigger a database load");
        item.IsObjectLoaded.Should().BeFalse();
    }

    [Fact]
    public void MarkUnloaded_DoesNotWakeTheItemsObject()
    {
        var (_, item, loads) = Make();
        var (root, _, _) = Make();

        LazyReferenceInstaller.MarkUnloadedWherePropsAreNull(root);

        loads.Value.Should().Be(0);
        item.IsObjectLoaded.Should().BeFalse();
    }

    private sealed class NoopLoader : ILazyPropsLoader
    {
        public TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new() => null;
        public Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId, CancellationToken cancellationToken = default) where TProps : class, new() => Task.FromResult<TProps?>(null);
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new() => Task.CompletedTask;
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, CancellationToken cancellationToken = default) where TProps : class, new() => Task.CompletedTask;
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new() => Task.CompletedTask;
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new() => Task.CompletedTask;
    }
}
