using redb.Core.Caching;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The props cache names its own trouble (production, 2026-09-15: a cache too small for the working set and a
/// slow hit check degraded a process ×5-10 without a single log line). Each warning is written at most once per
/// 10-second window.
/// </summary>
public class PropsCacheWarningTests
{
    private static RedbObject<SimpleProps> MakeObject(long id)
    {
        var obj = new RedbObject<SimpleProps>
        {
            id = id,
            name = $"warn-{id}",
            scheme_id = 1000,
            Props = new SimpleProps { Title = $"warn-{id}" }
        };
        obj.RecomputeHash();
        return obj;
    }

    [Fact]
    public void WorkingSetAboveTheLimit_WarnsOnce_NamingTheSetting()
    {
        var capture = new CapturingLoggerProvider();
        var cache = new MemoryRedbObjectCache(maxSize: 10, logger: capture.CreateLogger("props-cache"));

        for (long id = 1; id <= 60; id++)
            cache.Set(MakeObject(id));

        capture.Warnings.Should().ContainSingle(m => m.Contains("PropsCacheMaxSize"),
            "evicting most of the cache within seconds means the objects in use do not fit it");
    }

    /// <summary>Props whose getter can be slowed down: every validation of a cached hit reads it.</summary>
    public sealed class SlowProbeProps
    {
        public static volatile int DelayMs;

        private string _title = string.Empty;

        public string Title
        {
            get
            {
                if (DelayMs > 0)
                    Thread.Sleep(DelayMs);
                return _title;
            }
            set => _title = value;
        }
    }

    [Fact]
    public void SlowHitValidation_WarnsOnce()
    {
        var capture = new CapturingLoggerProvider();
        var cache = new MemoryRedbObjectCache(logger: capture.CreateLogger("props-cache"));
        var obj = new RedbObject<SlowProbeProps> { id = 7, name = "slow", scheme_id = 1000, Props = new SlowProbeProps { Title = "slow" } };
        obj.RecomputeHash();
        cache.Set(obj);

        SlowProbeProps.DelayMs = 60;
        try
        {
            cache.Get<SlowProbeProps>(7, obj.hash!.Value).Should().NotBeNull();
            cache.Get<SlowProbeProps>(7, obj.hash!.Value).Should().NotBeNull();
        }
        finally
        {
            SlowProbeProps.DelayMs = 0;
        }

        capture.Warnings.Should().ContainSingle(m => m.Contains("validation") && m.Contains("7"),
            "a hit whose check takes this long makes every cache hit that slow");
    }

    [Fact]
    public void CurrentEntriesServedNone_WarnsOnce()
    {
        var capture = new CapturingLoggerProvider();
        var cache = new MemoryRedbObjectCache(logger: capture.CreateLogger("props-cache"));
        var obj = MakeObject(1);
        cache.Set(obj);

        // The stored hash still matches, but the cached instance no longer hashes to it - what a read model that
        // does not reproduce the saved graph looks like on every lookup.
        obj.Props.Title = "changed in place";
        for (var i = 0; i < 150; i++)
            cache.Get<SimpleProps>(1, obj.hash!.Value).Should().BeNull();

        capture.Warnings.Should().ContainSingle(m => m.Contains("served none"));
    }

    [Fact]
    public void HealthyCache_StaysSilent()
    {
        var capture = new CapturingLoggerProvider();
        var cache = new MemoryRedbObjectCache(maxSize: 1000, logger: capture.CreateLogger("props-cache"));
        var objects = Enumerable.Range(1, 200).Select(i => MakeObject(i)).ToList();
        foreach (var obj in objects)
            cache.Set(obj);

        for (var pass = 0; pass < 3; pass++)
            foreach (var obj in objects)
                cache.Get<SimpleProps>(obj.id, obj.hash!.Value).Should().NotBeNull();

        capture.Warnings.Should().BeEmpty();
    }
}
