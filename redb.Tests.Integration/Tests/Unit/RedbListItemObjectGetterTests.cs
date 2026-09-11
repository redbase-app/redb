using System.Diagnostics;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The sync <see cref="RedbListItem.Object"/> getter after the tsum freeze (2026-09-09,
/// reproduced locally): items are shared through the static list cache, and the old shape -
/// a lock held ACROSS the load plus a Task.Run needing a second pool thread per touch -
/// serialized every toucher process-wide behind the first one and seized a busy pool without
/// a single exception. The getter now loads outside any lock and without Task.Run: concurrent
/// touchers proceed independently, the first published result wins, a racing duplicate load
/// is harmless.
/// </summary>
public class RedbListItemObjectGetterTests
{
    private static RedbListItem SlowItem(TimeSpan loadTime, Action onLoad)
    {
        var item = new RedbListItem { Id = 7, IdList = 1, Value = "probe", IdObject = 42 };
        item.AttachObjectLoader(async id =>
        {
            onLoad();
            await Task.Delay(loadTime);
            return (IRedbObject?)new RedbObject { id = id, name = "loaded-" + id };
        });
        return item;
    }

    [Fact]
    public async Task ConcurrentTouches_DoNotConvoyBehindOneLock()
    {
        var loads = 0;
        var item = SlowItem(TimeSpan.FromMilliseconds(300), () => Interlocked.Increment(ref loads));

        var sw = Stopwatch.StartNew();
        var touches = await Task.WhenAll(Enumerable.Range(0, 4)
            .Select(_ => Task.Run(() => item.Object)));
        sw.Stop();

        touches.Should().OnlyContain(o => o != null && o.Name == "loaded-42",
            "every toucher gets the loaded object");
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(900),
            "touchers must load in parallel - the old lock-across-IO shape serialized them "
            + "(4 x 300ms and worse: on a busy pool the whole process froze)");
        loads.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task RacingLoads_PublishExactlyOneObject()
    {
        var item = SlowItem(TimeSpan.FromMilliseconds(50), () => { });

        var touches = await Task.WhenAll(Enumerable.Range(0, 8)
            .Select(_ => Task.Run(() => item.Object)));

        touches.Distinct().Should().HaveCount(1,
            "whatever races happened, one instance is published and everyone sees it");
        item.Object.Should().BeSameAs(touches[0], "the published instance is stable");
    }

    // === The thread-pool-free sync path ===
    // The sync getter prefers the synchronous loader: the whole load runs on the calling thread
    // down to ADO.NET, so a saturated thread pool cannot slow or deadlock a touch of Object
    // (the blocking-over-async fallback still parks a thread waiting for pool-scheduled
    // continuations).

    [Fact]
    public void SyncLoader_IsPreferred_AndRunsOnTheCallingThread()
    {
        var item = new RedbListItem { Id = 8, IdList = 1, Value = "probe", IdObject = 43 };
        var asyncTouched = false;
        item.AttachObjectLoader(id =>
        {
            asyncTouched = true;
            return Task.FromResult((IRedbObject?)new RedbObject { id = id });
        });
        var loaderThread = -1;
        item.AttachSyncObjectLoader(id =>
        {
            loaderThread = Environment.CurrentManagedThreadId;
            return new RedbObject { id = id, name = "sync-" + id };
        });

        var callerThread = Environment.CurrentManagedThreadId;
        var obj = item.Object;

        obj!.Name.Should().Be("sync-43");
        loaderThread.Should().Be(callerThread,
            "the sync path runs the whole load on the calling thread - no pool handoff anywhere");
        asyncTouched.Should().BeFalse("the async loader must stay untouched when a sync one is attached");
        item.IsObjectLoaded.Should().BeTrue();
    }

    [Fact]
    public async Task GetObjectAsync_FallsBackToTheSyncLoader_WhenNoAsyncOneIsAttached()
    {
        var item = new RedbListItem { Id = 9, IdList = 1, Value = "probe", IdObject = 44 };
        item.AttachSyncObjectLoader(id => new RedbObject { id = id, name = "sync-" + id });

        var obj = await item.GetObjectAsync();

        obj!.Name.Should().Be("sync-44");
        item.IsObjectLoaded.Should().BeTrue();
    }

    [Fact]
    public async Task SyncLoader_RacingTouches_PublishExactlyOneObject()
    {
        var item = new RedbListItem { Id = 10, IdList = 1, Value = "probe", IdObject = 45 };
        var loads = 0;
        item.AttachSyncObjectLoader(id =>
        {
            Interlocked.Increment(ref loads);
            Thread.Sleep(50);
            return new RedbObject { id = id, name = "sync-" + id };
        });

        var touches = await Task.WhenAll(Enumerable.Range(0, 8)
            .Select(_ => Task.Run(() => item.Object)));

        touches.Distinct().Should().HaveCount(1,
            "whatever races happened, one instance is published and everyone sees it");
        loads.Should().BeGreaterThan(0);
    }
}
