using redb.Core.Caching;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// PropsCache (the whole-object cache behind <see cref="GlobalPropsCache"/>) unit behaviour: keyed by
/// objectId, validated by object hash, type-safe on read, honours TTL. The fixtures run with
/// EnablePropsCache=false, so this exercises <see cref="MemoryRedbObjectCache"/> directly. See
/// docs/CACHING_ARCHITECTURE.md §1.2.
/// </summary>
public class PropsCacheTests
{
    private static RedbObject<SimpleProps> MakeObject(long id, Guid hash, string title) => new()
    {
        id = id,
        name = title,
        scheme_id = 1000,
        hash = hash,
        Props = new SimpleProps { Title = title }
    };

    [Fact]
    public void Get_WithMatchingHash_ReturnsObject()
    {
        var cache = new MemoryRedbObjectCache();
        var hash = Guid.NewGuid();
        cache.Set(MakeObject(1, hash, "hit"));

        var got = cache.Get<SimpleProps>(1, hash);

        got.Should().NotBeNull();
        got!.Props.Title.Should().Be("hit");
    }

    [Fact]
    public void Get_WithChangedHash_ReturnsNull()
    {
        var cache = new MemoryRedbObjectCache();
        cache.Set(MakeObject(2, Guid.NewGuid(), "stale"));

        // Different hash = the object changed in the DB → cache must miss so fresh data is loaded.
        cache.Get<SimpleProps>(2, Guid.NewGuid()).Should().BeNull();
    }

    [Fact]
    public void GetWithoutHashValidation_ReturnsObject_IgnoringHash()
    {
        var cache = new MemoryRedbObjectCache();
        cache.Set(MakeObject(3, Guid.NewGuid(), "trusted"));

        // SkipHashValidationOnCacheCheck path: trust the cache, no hash compare.
        var got = cache.GetWithoutHashValidation<SimpleProps>(3);

        got.Should().NotBeNull();
        got!.Props.Title.Should().Be("trusted");
    }

    [Fact]
    public void Get_WrongType_ReturnsNull_NotGarbage()
    {
        var cache = new MemoryRedbObjectCache();
        var hash = Guid.NewGuid();
        cache.Set(MakeObject(4, hash, "typed"));

        // Same objectId, different TProps: `entry.RedbObject as RedbObject<TProps>` yields null rather
        // than a mistyped object — the caller reloads instead of getting garbage.
        cache.Get<EmployeeProps>(4, hash).Should().BeNull();
    }

    [Fact]
    public void Remove_EvictsEntry()
    {
        var cache = new MemoryRedbObjectCache();
        var hash = Guid.NewGuid();
        cache.Set(MakeObject(5, hash, "gone"));
        cache.Get<SimpleProps>(5, hash).Should().NotBeNull();

        cache.Remove(5);

        cache.Get<SimpleProps>(5, hash).Should().BeNull();
    }

    [Fact]
    public void Ttl_Expired_ReturnsNull()
    {
        var cache = new MemoryRedbObjectCache(ttl: TimeSpan.Zero);
        var hash = Guid.NewGuid();
        cache.Set(MakeObject(6, hash, "expired"));

        // TTL zero → the entry is already stale on the next read.
        cache.Get<SimpleProps>(6, hash).Should().BeNull();
    }

    [Fact]
    public void Stats_TrackHitsAndMisses()
    {
        var cache = new MemoryRedbObjectCache();
        var hash = Guid.NewGuid();
        cache.Set(MakeObject(7, hash, "s"));

        cache.Get<SimpleProps>(7, hash);           // hit
        cache.Get<SimpleProps>(999, Guid.NewGuid()); // miss

        var stats = cache.GetStats();
        stats.HitCount.Should().BeGreaterThan(0);
        stats.MissCount.Should().BeGreaterThan(0);
    }
}
