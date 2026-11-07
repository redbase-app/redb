using redb.Core;
using redb.Core.Caching;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Cross-cache E2E over the live service: the domain metadata cache (schemes), the process-global
/// ClrSchemeTypeIndex, and the list cache. Verifies caching, invalidation and the per-domain isolation
/// invariant documented in docs/CACHING_ARCHITECTURE.md. (PropsCache is covered separately by unit
/// tests since the fixtures run with EnablePropsCache=false.)
/// </summary>
public abstract class CacheBehaviorTestsBase
{
    protected readonly IRedbService Redb;

    protected CacheBehaviorTestsBase(IRedbService redb) => Redb = redb;

    // ===== GlobalMetadataCache =====

    [Fact]
    public async Task MetadataCache_AfterSync_SchemeIsCached()
    {
        await Redb.SyncSchemeAsync<NamingByTypeProps>();

        // GetSchemeFromCache is a pure in-memory lookup (no DB) — non-null means it is cached.
        Redb.GetSchemeFromCache<NamingByTypeProps>().Should().NotBeNull();
    }

    [Fact]
    public async Task MetadataCache_Invalidate_RemovesScheme()
    {
        var scheme = await Redb.SyncSchemeAsync<NamingByTypeProps>();
        Redb.GetSchemeFromCache<NamingByTypeProps>().Should().NotBeNull();

        Redb.Cache.InvalidateScheme(scheme.Name);

        Redb.GetSchemeFromCache<NamingByTypeProps>().Should().BeNull("invalidation must drop the cached scheme");
    }

    [Fact]
    public async Task MetadataCache_ByName_CountsAHit()
    {
        var scheme = await Redb.SyncSchemeAsync<NamingByTypeProps>();
        var name = scheme.Name;

        // Prime the SchemeByName projection, then a second lookup must register a hit.
        await Redb.GetSchemeByNameAsync(name);
        var before = Redb.Cache.GetStatistics().SchemeHits;
        var again = await Redb.GetSchemeByNameAsync(name);

        again.Should().NotBeNull();
        Redb.Cache.GetStatistics().SchemeHits.Should().BeGreaterThan(before);
    }

    [Fact]
    public void MetadataCache_IsEnabledInFixtures()
    {
        Redb.Cache.IsEnabled.Should().BeTrue();
    }

    // ===== ClrSchemeTypeIndex (process-global name → Type) =====

    [Fact]
    public async Task ClrSchemeTypeIndex_ResolvesSyncedNameToType()
    {
        var scheme = await Redb.SyncSchemeAsync<NamingByTypeProps>();

        // The name → Type index is a code fact shared across all cache domains.
        ClrSchemeTypeIndex.Resolve(scheme.Name).Should().Be(typeof(NamingByTypeProps));
    }

    [Fact]
    public async Task ClrSchemeTypeIndex_ResolvesAliasToo()
    {
        await Redb.SyncSchemeAsync<NamingByTypeProps>();

        // NamingByTypeProps carries an alias — it must resolve to the same type as the scheme name.
        ClrSchemeTypeIndex.Resolve("Именование: по типу").Should().Be(typeof(NamingByTypeProps));
    }

    // ===== GlobalListCache =====

    [Fact]
    public void ListCache_EnabledByDefault_AndTogglesReported()
    {
        Redb.ListCache.IsEnabled.Should().BeTrue("fixtures leave EnableListCache at its default (true)");

        Redb.ListCache.SetTtl(TimeSpan.FromMinutes(3));   // must not throw
        Redb.ListCache.SetEnabled(false);
        Redb.ListCache.IsEnabled.Should().BeFalse();
        Redb.ListCache.SetEnabled(true);
        Redb.ListCache.IsEnabled.Should().BeTrue();
    }

    [Fact]
    public void ListCache_Clear_DoesNotThrow_AndStaysEnabled()
    {
        Redb.ListCache.Clear();
        Redb.ListCache.IsEnabled.Should().BeTrue();
    }
}
