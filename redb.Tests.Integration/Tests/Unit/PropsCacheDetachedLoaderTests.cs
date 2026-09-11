using Microsoft.Extensions.DependencyInjection;
using redb.Core.Caching;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// When the props cache switches a graph's lazy loaders to <see cref="DetachedLazyPropsLoader"/>
/// (tsum production incident, 2026-09-09): the detached loader opens a fresh DI scope, and with it
/// a pooled connection, on EVERY lazy access. Doing that on <c>Set</c> hijacked the very instance
/// the writing request was still working with - the writer's own reference walk started renting a
/// connection per touch, and a parallel tick drained the whole pool. The hand-off point is the
/// hand-OUT: only a graph served to another scope (<c>Get</c>/<c>GetWithoutHashValidation</c>/
/// <c>FilterNeedToLoad</c>) must be detached; the writer keeps its own scoped loaders.
/// </summary>
public class PropsCacheDetachedLoaderTests
{
    private static GlobalPropsCache FreshDomainCache(out string domain)
    {
        domain = "detached-" + Guid.NewGuid().ToString("N");
        var cache = new GlobalPropsCache(domain);
        cache.Initialize(new MemoryRedbObjectCache(), new ThrowingScopeFactory());
        return cache;
    }

    private static (RedbObject<LazyNodeProps> root, RedbObject<LazyNodeProps> stub) RootWithStub(long id)
    {
        var stub = new RedbObject<LazyNodeProps>
        {
            id = id + 1000,
            scheme_id = 100,
            hash = Guid.NewGuid(),
            _lazyLoader = new MarkerLoader(),
        };
        var root = new RedbObject<LazyNodeProps>
        {
            id = id,
            scheme_id = 100,
            Props = new LazyNodeProps { Label = "root", Next = stub },
        };
        root.RecomputeHash();
        return (root, stub);
    }

    [Fact]
    public void Set_DoesNotHijackTheWritersGraph()
    {
        var cache = FreshDomainCache(out _);
        var (root, stub) = RootWithStub(1);

        cache.Set(root);

        var wrapped = stub._lazyLoader.Should().BeOfType<ScopeFallbackLazyPropsLoader>(
            "the writer keeps its own scoped loader (wrapped with a dead-scope fallback) - "
            + "detaching on Set would make its every reference touch rent a fresh scope and connection").Subject;
        wrapped.Scoped.Should().BeSameAs(root.Props.Next!._lazyLoader is ScopeFallbackLazyPropsLoader f ? f.Scoped : null);
        wrapped.Scoped.Should().BeOfType<MarkerLoader>("the writer's original loader is what the wrapper delegates to");
    }

    [Fact]
    public void Get_HandsOutADetachedGraph()
    {
        var cache = FreshDomainCache(out _);
        var (root, stub) = RootWithStub(2);
        cache.Set(root);

        var served = cache.Get<LazyNodeProps>(2, root.hash!.Value);

        served.Should().NotBeNull();
        stub._lazyLoader.Should().BeOfType<DetachedLazyPropsLoader>(
            "a graph served from the cache is shared - it must not carry the writer's scoped loader");
    }

    [Fact]
    public void GetWithoutHashValidation_HandsOutADetachedGraph()
    {
        var cache = FreshDomainCache(out _);
        var (root, stub) = RootWithStub(3);
        cache.Set(root);

        var served = cache.GetWithoutHashValidation<LazyNodeProps>(3);

        served.Should().NotBeNull();
        stub._lazyLoader.Should().BeOfType<DetachedLazyPropsLoader>();
    }

    [Fact]
    public void FilterNeedToLoad_HandsOutDetachedGraphs()
    {
        var cache = FreshDomainCache(out _);
        var (root, stub) = RootWithStub(4);
        cache.Set(root);

        var missing = cache.FilterNeedToLoad(
            new List<(long, Guid)> { (4, root.hash!.Value) }, out Dictionary<long, RedbObject<LazyNodeProps>> fromCache);

        missing.Should().BeEmpty();
        fromCache.Should().ContainKey(4);
        stub._lazyLoader.Should().BeOfType<DetachedLazyPropsLoader>(
            "the bulk hand-out shares the graph exactly like Get does");
    }

    private sealed class ThrowingScopeFactory : IServiceScopeFactory
    {
        public IServiceScope CreateScope()
            => throw new NotSupportedException("no scope is expected to be opened by these tests");
    }

    private sealed class MarkerLoader : ILazyPropsLoader
    {
        public TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new()
            => throw new NotSupportedException("marker only");
        public Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new NotSupportedException("marker only");
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new NotSupportedException("marker only");
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new NotSupportedException("marker only");
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new NotSupportedException("marker only");
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new NotSupportedException("marker only");
    }
}
