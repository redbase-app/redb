using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Core.Pro.Extensions;
using redb.SQLite.Data;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// DIAGNOSTIC — reproduces the OAuth "already redeemed" story on the storage layer with
/// <c>EnablePropsCache=true</c>, to find WHERE a stale read comes from (not to assert final behaviour).
/// Self-contained: own ServiceProvider with the cache ON, own SQLite file. SQLite because the bug
/// report is SQLite (Free+Pro).
/// </summary>
public sealed class PropsCacheMutationDiagTests : IAsyncLifetime
{
    private ServiceProvider _sp = null!;
    private IRedbService _redb = null!;

    public async Task InitializeAsync()
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        const string cs = "Data Source=redb_diag_cache.db";
        SqliteTestSupport.DeleteDbFiles(cs);

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddRedbPro(options =>   // ← Pro (free, no license key): ChangeTracking is Pro-only
            options.UseSqlite(cs)
                .Configure(c =>
                {
                    c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;  // ← Identity Pro uses this, not DeleteInsert
                    c.SkipHashValidationOnCacheCheck = false;
                    c.EnableLazyLoadingForProps = false;
                    c.EnablePropsCache = true;   // ← the whole point
                }));

        _sp = services.BuildServiceProvider();
        _redb = _sp.GetRequiredService<IRedbService>();
        try { await _redb.InitializeAsync(ensureCreated: true); }
        catch { await _redb.InitializeAsync(); }
        await _redb.SyncSchemeAsync<SimpleProps>();
    }

    public async Task DisposeAsync()
    {
        if (_sp is not null) await _sp.DisposeAsync();
    }

    private static RedbObject<SimpleProps> New(string title) => new()
    {
        name = "diag",
        Props = new SimpleProps { Title = title, Price = 1m, Count = 1, IsActive = true }
    };

    /// <summary>Baseline: plain Save → mutate → Save → Load. Does the cache alone go stale?</summary>
    [Fact]
    public async Task MutateCycle_NoLock_LoadSeesLatest()
    {
        var obj = New("valid");
        obj.id = await _redb.SaveAsync(obj);

        var l1 = await _redb.LoadAsync<SimpleProps>(obj.id);
        l1!.Props.Title.Should().Be("valid");

        // mutate a field (analogue of Status: valid → redeemed) and persist
        l1.Props.Title = "redeemed";
        await _redb.SaveAsync(l1);

        var l2 = await _redb.LoadAsync<SimpleProps>(obj.id);
        l2!.Props.Title.Should().Be("redeemed", "a full Save recomputes _hash; Load must see the new state");
    }

    /// <summary>The OAuth shape: LockForUpdate → Load(current) → check → Save, done TWICE per request.</summary>
    [Fact]
    public async Task LockLoadSaveCycle_SecondPass_CurrentIsFresh()
    {
        var obj = New("valid");
        obj.id = await _redb.SaveAsync(obj);

        // pass 1 — redeem
        await _redb.Context.ExecuteAtomicAsync(async () =>
        {
            await _redb.LockForUpdateAsync(obj.id);
            var current = await _redb.LoadAsync<SimpleProps>(obj.id);
            current!.Props.Title.Should().Be("valid", "pass-1 current must be the pre-redeem state");
            current.Props.Title = "redeemed";
            await _redb.SaveAsync(current);
        });

        // pass 2 — a metadata-only touch in the same logical request; current must reflect pass 1
        await _redb.Context.ExecuteAtomicAsync(async () =>
        {
            await _redb.LockForUpdateAsync(obj.id);
            var current = await _redb.LoadAsync<SimpleProps>(obj.id);
            current!.Props.Title.Should().Be("redeemed",
                "pass-2 current must be fresh (redeemed) — a stale 'valid' here is the bug, " +
                "a stale 'redeemed' seen as valid would be the OAuth symptom");
        });
    }

    /// <summary>
    /// The real Identity shape: writes go through SaveAsync, but the token/authorization stores READ via
    /// <c>Query&lt;TProps&gt;()</c> (server-side, bypasses PropsCache) AND via <c>LoadAsync</c> (through
    /// PropsCache). After a mutation, do the two read paths agree?
    /// </summary>
    [Fact]
    public async Task QueryVsLoad_AfterMutation_BothSeeLatest()
    {
        var obj = New("valid");
        obj.id = await _redb.SaveAsync(obj);

        // warm the cache via Load (as a prior request would)
        (await _redb.LoadAsync<SimpleProps>(obj.id))!.Props.Title.Should().Be("valid");

        // mutate + persist through the normal Save path
        var toUpdate = await _redb.LoadAsync<SimpleProps>(obj.id);
        toUpdate!.Props.Title = "redeemed";
        await _redb.SaveAsync(toUpdate);

        // Query path (bypasses PropsCache — reads SQL)
        var viaQuery = await _redb.Query<SimpleProps>()
            .WhereRedb(o => o.Id == obj.id)
            .FirstOrDefaultAsync();

        // Load path (through PropsCache)
        var viaLoad = await _redb.LoadAsync<SimpleProps>(obj.id);

        viaQuery!.Props.Title.Should().Be("redeemed", "Query reads SQL directly");
        viaLoad!.Props.Title.Should().Be("redeemed", "Load must not serve a stale cached copy");
        viaLoad.Props.Title.Should().Be(viaQuery.Props.Title, "the two read paths must agree");
    }

    /// <summary>
    /// The path the single-object tests missed: BATCH load routes through
    /// ProLazyPropsLoader.LoadPropsForManyAsync → FilterNeedToLoad (the batch cache filter). Warm a
    /// batch, mutate ONE member, reload the batch — does the mutated one come back fresh?
    /// </summary>
    [Fact]
    public async Task BatchLoad_AfterMutatingOne_AllFresh()
    {
        var a = New("a"); a.id = await _redb.SaveAsync(a);
        var b = New("b"); b.id = await _redb.SaveAsync(b);
        var c = New("c"); c.id = await _redb.SaveAsync(c);

        // warm the cache with a BATCH load (FilterNeedToLoad → Set all three)
        _ = await _redb.LoadAsync(new[] { a.id, b.id, c.id });

        // mutate only B, persist through the normal Save path
        var bLoad = await _redb.LoadAsync<SimpleProps>(b.id);
        bLoad!.Props.Title = "b-changed";
        await _redb.SaveAsync(bLoad);

        // reload the whole batch — B must reflect the mutation, A/C untouched
        var again = await _redb.LoadAsync(new[] { a.id, b.id, c.id });
        var bAgain = again.First(o => o.Id == b.id) as RedbObject<SimpleProps>;
        var aAgain = again.First(o => o.Id == a.id) as RedbObject<SimpleProps>;

        bAgain!.Props.Title.Should().Be("b-changed", "batch FilterNeedToLoad must not serve a stale B");
        aAgain!.Props.Title.Should().Be("a", "A was not mutated");
    }

    /// <summary>
    /// THE root cause: PropsCache stores/returns a REFERENCE. If a caller mutates a loaded object
    /// in place (as OpenIddict's SetStatusAsync does BEFORE Save), the cache — holding the same
    /// reference — instantly reflects the mutation, under the SAME hash key. A later LoadAsync then
    /// returns the mutated object ("redeemed") even though the DB row is still "valid" → "already
    /// redeemed" on a valid code. The cache must isolate its snapshot from external mutation.
    /// </summary>
    [Fact]
    public async Task Aliasing_InPlaceMutationOfLoadedObject_DoesNotLeakIntoCache()
    {
        var obj = New("valid");
        obj.id = await _redb.SaveAsync(obj);

        var loaded1 = await _redb.LoadAsync<SimpleProps>(obj.id);   // warms cache (Set stores the ref)
        loaded1!.Props.Title = "redeemed-inplace";                  // mutate IN PLACE, WITHOUT Save

        var loaded2 = await _redb.LoadAsync<SimpleProps>(obj.id);   // must still read committed "valid"
        loaded2!.Props.Title.Should().Be("valid",
            "the cache must isolate its snapshot — an in-place mutation of a loaded object must not leak in");
    }
}
