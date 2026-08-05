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
    // Real object: hash = ComputeHash(Props), like production (Save writes it, materialization
    // reproduces it). The cache's live-hash dirty-check relies on this invariant — a fabricated random
    // hash would (correctly) be seen as a dirty snapshot and missed.
    private static RedbObject<SimpleProps> MakeObject(long id, string title)
    {
        var obj = new RedbObject<SimpleProps>
        {
            id = id,
            name = title,
            scheme_id = 1000,
            Props = new SimpleProps { Title = title }
        };
        obj.RecomputeHash();   // hash now matches Props
        return obj;
    }

    [Fact]
    public void Get_WithMatchingHash_ReturnsObject()
    {
        var cache = new MemoryRedbObjectCache();
        var obj = MakeObject(1, "hit");
        cache.Set(obj);

        var got = cache.Get<SimpleProps>(1, obj.hash!.Value);

        got.Should().NotBeNull();
        got!.Props.Title.Should().Be("hit");
    }

    [Fact]
    public void Get_WithChangedHash_ReturnsNull()
    {
        var cache = new MemoryRedbObjectCache();
        cache.Set(MakeObject(2, "stale"));

        // Different hash = the object changed in the DB → cache must miss so fresh data is loaded.
        cache.Get<SimpleProps>(2, Guid.NewGuid()).Should().BeNull();
    }

    [Fact]
    public void GetWithoutHashValidation_ReturnsObject_IgnoringHash()
    {
        var cache = new MemoryRedbObjectCache();
        cache.Set(MakeObject(3, "trusted"));

        // SkipHashValidationOnCacheCheck path: trust the cache, no DB-hash compare (dirty-check still applies).
        var got = cache.GetWithoutHashValidation<SimpleProps>(3);

        got.Should().NotBeNull();
        got!.Props.Title.Should().Be("trusted");
    }

    [Fact]
    public void Get_WrongType_ReturnsNull_NotGarbage()
    {
        var cache = new MemoryRedbObjectCache();
        var obj = MakeObject(4, "typed");
        cache.Set(obj);

        // Same objectId, different TProps: `entry.RedbObject as RedbObject<TProps>` yields null rather
        // than a mistyped object — the caller reloads instead of getting garbage.
        cache.Get<EmployeeProps>(4, obj.hash!.Value).Should().BeNull();
    }

    [Fact]
    public void Remove_EvictsEntry()
    {
        var cache = new MemoryRedbObjectCache();
        var obj = MakeObject(5, "gone");
        cache.Set(obj);
        cache.Get<SimpleProps>(5, obj.hash!.Value).Should().NotBeNull();

        cache.Remove(5);

        cache.Get<SimpleProps>(5, obj.hash!.Value).Should().BeNull();
    }

    [Fact]
    public void Ttl_Expired_ReturnsNull()
    {
        var cache = new MemoryRedbObjectCache(ttl: TimeSpan.Zero);
        var obj = MakeObject(6, "expired");
        cache.Set(obj);

        // TTL zero → the entry is already stale on the next read.
        cache.Get<SimpleProps>(6, obj.hash!.Value).Should().BeNull();
    }

    [Fact]
    public void DirtySnapshot_MutatedAfterSet_ReturnsNull()
    {
        var cache = new MemoryRedbObjectCache();
        var obj = MakeObject(8, "clean");
        cache.Set(obj);

        // Mutate the SAME object in place after caching (aliasing) WITHOUT re-Set — the cache holds a
        // reference, so its snapshot is now dirty. The live-hash check must detect it and miss.
        obj.Props.Title = "mutated-in-place";

        cache.Get<SimpleProps>(8, obj.hash!.Value).Should().BeNull(
            "a cached object mutated in place after Set is a dirty snapshot — must miss, not serve stale");
    }

    [Fact]
    public void Stats_TrackHitsAndMisses()
    {
        var cache = new MemoryRedbObjectCache();
        var obj = MakeObject(7, "s");
        cache.Set(obj);

        cache.Get<SimpleProps>(7, obj.hash!.Value);   // hit
        cache.Get<SimpleProps>(999, Guid.NewGuid());  // miss

        var stats = cache.GetStats();
        stats.HitCount.Should().BeGreaterThan(0);
        stats.MissCount.Should().BeGreaterThan(0);
    }
}
