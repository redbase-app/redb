using redb.Core.Caching;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The props cache under load (production, 2026-09-15): an insert into a full cache sorted every entry under the
/// write lock, and hit validation - the object hash and the walk of its loaded graph - ran under one process-wide
/// lock, so concurrent readers queued behind each other with their connections open. Eviction is now least
/// recently used, a tenth of the limit at a time, and validation runs outside the lock. A re-cached object starts
/// a new lifetime.
/// </summary>
public class PropsCacheEvictionTests
{
    private static RedbObject<SimpleProps> MakeObject(long id)
    {
        var obj = new RedbObject<SimpleProps>
        {
            id = id,
            name = $"evict-{id}",
            scheme_id = 1000,
            Props = new SimpleProps { Title = $"evict-{id}" }
        };
        obj.RecomputeHash();
        return obj;
    }

    [Fact]
    public void Overflow_EvictsATenthOfTheLimitAtOnce()
    {
        var cache = new MemoryRedbObjectCache(maxSize: 100);
        for (long id = 1; id <= 100; id++)
            cache.Set(MakeObject(id));
        cache.GetStats().TotalEntries.Should().Be(100);

        cache.Set(MakeObject(101));

        cache.GetStats().TotalEntries.Should().Be(91,
            "an insert into a full cache evicts a tenth of the limit, so the next inserts do not pay an eviction each");
    }

    [Fact]
    public void Overflow_EvictsTheLeastRecentlyUsed_NotTheOldest()
    {
        var cache = new MemoryRedbObjectCache(maxSize: 10);
        var objects = Enumerable.Range(1, 10).Select(i => MakeObject(i)).ToList();
        foreach (var obj in objects)
            cache.Set(obj);

        // The oldest entry is read, so it becomes the most recently used one.
        cache.Get<SimpleProps>(1, objects[0].hash!.Value).Should().NotBeNull();

        cache.Set(MakeObject(11));

        cache.Get<SimpleProps>(1, objects[0].hash!.Value).Should().NotBeNull("the entry just read is the last one to go");
        cache.Get<SimpleProps>(2, objects[1].hash!.Value).Should().BeNull("the least recently used entry is evicted");
    }

    [Fact]
    public void ReSet_AfterTheTtl_StartsANewLifetime()
    {
        var cache = new MemoryRedbObjectCache(ttl: TimeSpan.FromMilliseconds(200));
        var obj = MakeObject(1);
        cache.Set(obj);
        Thread.Sleep(300);

        // A periodic job loads and re-caches the same objects on every pass.
        cache.Set(obj);

        cache.Get<SimpleProps>(1, obj.hash!.Value).Should().NotBeNull(
            "a re-cached object is current again; keeping its first lifetime made it miss for ever once one TTL had passed");
    }

    [Fact]
    public void ExpiredEntry_IsDroppedOnRead()
    {
        var cache = new MemoryRedbObjectCache(ttl: TimeSpan.FromMilliseconds(100));
        var obj = MakeObject(1);
        cache.Set(obj);
        Thread.Sleep(200);

        cache.Get<SimpleProps>(1, obj.hash!.Value).Should().BeNull();

        cache.GetStats().TotalEntries.Should().Be(0, "an expired entry frees its slot instead of holding it until eviction");
    }

    /// <summary>Props whose getter can be slowed down and counts the readers inside it at the same time.</summary>
    public sealed class ParallelProbeProps
    {
        public static volatile int DelayMs;
        private static int _inFlight;
        private static int _maxInFlight;

        public static int MaxInFlight => _maxInFlight;

        private string _title = string.Empty;

        public string Title
        {
            get
            {
                if (DelayMs > 0)
                {
                    var now = Interlocked.Increment(ref _inFlight);
                    int seen;
                    while (now > (seen = _maxInFlight) && Interlocked.CompareExchange(ref _maxInFlight, now, seen) != seen) { }
                    Thread.Sleep(DelayMs);
                    Interlocked.Decrement(ref _inFlight);
                }
                return _title;
            }
            set => _title = value;
        }
    }

    [Fact]
    public async Task HitsOfDifferentObjects_ValidateAtTheSameTime()
    {
        var cache = new MemoryRedbObjectCache();
        var first = new RedbObject<ParallelProbeProps> { id = 1, name = "first", scheme_id = 1000, Props = new ParallelProbeProps { Title = "first" } };
        var second = new RedbObject<ParallelProbeProps> { id = 2, name = "second", scheme_id = 1000, Props = new ParallelProbeProps { Title = "second" } };
        first.RecomputeHash();
        second.RecomputeHash();
        cache.Set(first);
        cache.Set(second);

        ParallelProbeProps.DelayMs = 300;
        RedbObject<ParallelProbeProps>?[] hits;
        try
        {
            using var start = new Barrier(2);
            hits = await Task.WhenAll(
                Task.Run(() => { start.SignalAndWait(); return cache.Get<ParallelProbeProps>(1, first.hash!.Value); }),
                Task.Run(() => { start.SignalAndWait(); return cache.Get<ParallelProbeProps>(2, second.hash!.Value); }));
        }
        finally
        {
            ParallelProbeProps.DelayMs = 0;
        }

        hits.Should().OnlyContain(h => h != null);
        ParallelProbeProps.MaxInFlight.Should().Be(2,
            "two hits validate their objects at the same time instead of queueing behind one cache lock");
    }
}
