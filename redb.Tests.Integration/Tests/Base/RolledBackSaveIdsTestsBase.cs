using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Attributes;
using redb.Core.Data;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Base;

[RedbScheme("RolledBackSaveProbe")]
public class RolledBackSaveProps
{
    public string Text { get; set; } = "";
    public string[] Tags { get; set; } = Array.Empty<string>();
    public List<RolledBackSaveItem> Items { get; set; } = new();
    public RedbObject<RolledBackSaveLeafProps>? Leaf { get; set; }
}

public class RolledBackSaveItem
{
    public string Label { get; set; } = "";
    public int Weight { get; set; }
}

[RedbScheme("RolledBackSaveLeaf")]
public class RolledBackSaveLeafProps
{
    public string Note { get; set; } = "";
}

/// <summary>A scheme no other test syncs: the scheme-cache case drops and recreates it.</summary>
[RedbScheme("RolledBackSaveFreshScheme")]
public class RolledBackSaveFreshProps
{
    public string Text { get; set; } = "";
    public int Number { get; set; }
}

/// <summary>
/// A save inside a transaction that rolls back. On SQLite the ids of such a save came from the caller's transaction
/// (the key cache empty, the single writer's lock held by the caller) and went back with it, while the saved objects
/// kept them: the next save got the same ids, and saving the first object again overwrote the second one's row
/// (since 4.0.0). The ids of a rolled-back save now go back to 0 on the objects, and the generator never hands out an
/// id twice. A rolled-back scheme sync leaves nothing in the scheme caches either.
/// <para>Every case starves the shared key cache after BEGIN, so the ids come from inside the transaction; the
/// precondition is asserted, a case that took its ids elsewhere fails rather than passes.</para>
/// </summary>
public abstract class RolledBackSaveIdsTestsBase : IAsyncLifetime
{
    private readonly IServiceProvider _services;
    private readonly string _tag = "rbids-" + Guid.NewGuid().ToString("N")[..10] + "-";

    protected RolledBackSaveIdsTestsBase(IServiceProvider services) => _services = services;

    /// <summary>The top of the id sequence, read inside the caller's transaction; null where ids never roll back.</summary>
    protected abstract string? SequenceTopSql { get; }

    public async Task InitializeAsync()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<RolledBackSaveLeafProps>();
        await redb.SyncSchemeAsync<RolledBackSaveProps>();
    }

    public async Task DisposeAsync()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await redb.Context.ExecuteAsync($"DELETE FROM _objects WHERE _name LIKE '{_tag}%'");
    }

    private RedbObject<RolledBackSaveProps> NewGraph(string who) => new()
    {
        name = _tag + who,
        Props = new RolledBackSaveProps
        {
            Text = "written by " + who,
            Tags = new[] { who + "-t1", who + "-t2", who + "-t3" },
            Items = new List<RolledBackSaveItem>
            {
                new() { Label = who + "-i1", Weight = 1 },
                new() { Label = who + "-i2", Weight = 2 },
            },
            Leaf = new RedbObject<RolledBackSaveLeafProps>
            {
                name = _tag + who + "-leaf",
                Props = new RolledBackSaveLeafProps { Note = "leaf of " + who },
            },
        },
    };

    /// <summary>Empties the shared key cache inside an open transaction and returns the sequence top it sees.</summary>
    private async Task<long?> StarveKeysAsync(IRedbService redb)
    {
        RedbKeyGeneratorBase.ClearAllCaches();
        return SequenceTopSql == null ? null : await redb.Context.ExecuteScalarAsync<long>(SequenceTopSql);
    }

    private static void AssertTakenInsideTransaction(long? top, long id, string what)
    {
        if (top is { } t)
            Assert.True(id > t, $"precondition: the id of {what} ({id}) comes from inside the transaction (sequence top {t})");
    }

    private async Task AssertGraphAsync(IRedbService redb, long id, string who)
    {
        var loaded = await redb.LoadAsync<RolledBackSaveProps>(id);
        Assert.NotNull(loaded);
        Assert.Equal(_tag + who, loaded!.name);
        Assert.Equal("written by " + who, loaded.Props.Text);
        Assert.Equal(new[] { who + "-t1", who + "-t2", who + "-t3" }, loaded.Props.Tags);
        Assert.Equal(new[] { who + "-i1", who + "-i2" }, loaded.Props.Items.Select(i => i.Label).ToArray());
        Assert.Equal(new[] { 1, 2 }, loaded.Props.Items.Select(i => i.Weight).ToArray());
        Assert.NotNull(loaded.Props.Leaf);
        Assert.Equal("leaf of " + who, loaded.Props.Leaf!.Props.Note);
    }

    private async Task<long> CountOwnObjectsAsync(IRedbService redb)
        => await redb.Context.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM _objects WHERE _name LIKE '{_tag}%'");

    [Fact]
    public async Task RolledBackSave_ThenAnotherSave_ThenTheFirstAgain_KeepsBothObjects()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        var a = NewGraph("A");
        await using (var tx = await redb.Context.BeginTransactionAsync())
        {
            var top = await StarveKeysAsync(redb);
            await redb.SaveAsync(a);
            AssertTakenInsideTransaction(top, a.Id, "A");
            await tx.RollbackAsync();
        }
        var (aIdAfterRollback, leafIdAfterRollback) = (a.Id, a.Props.Leaf!.Id);

        var b = NewGraph("B");
        var bId = await redb.SaveAsync(b);
        var aId = await redb.SaveAsync(a);

        await AssertGraphAsync(redb, bId, "B");
        await AssertGraphAsync(redb, aId, "A");
        Assert.NotEqual(aId, bId);
        Assert.Equal(4, await CountOwnObjectsAsync(redb));
        Assert.Equal((0L, 0L), (aIdAfterRollback, leafIdAfterRollback));
    }

    [Fact]
    public async Task RolledBackSave_ValueRows_EverySaveInItsOwnStarvedTransaction_KeepsBothObjects()
    {
        // The same sequence with every save inside a transaction whose ids come from the transaction itself: the
        // value rows of the second save and of the retry take their ids there too.
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        async Task<long> SaveStarvedAsync(RedbObject<RolledBackSaveProps> obj, bool commit)
        {
            await using var tx = await redb.Context.BeginTransactionAsync();
            await StarveKeysAsync(redb);
            var id = await redb.SaveAsync(obj);
            if (commit)
                await tx.CommitAsync();
            else
                await tx.RollbackAsync();
            return id;
        }

        var a = NewGraph("A");
        await SaveStarvedAsync(a, commit: false);
        var b = NewGraph("B");
        var bId = await SaveStarvedAsync(b, commit: true);
        var aId = await SaveStarvedAsync(a, commit: true);

        await AssertGraphAsync(redb, bId, "B");
        await AssertGraphAsync(redb, aId, "A");
        Assert.NotEqual(aId, bId);
        Assert.Equal(4, await CountOwnObjectsAsync(redb));
        var valueIds = await redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(*) FROM _values v JOIN _objects o ON o._id = v._id_object WHERE o._name LIKE '{_tag}%'");
        var distinctValueIds = await redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(DISTINCT v._id) FROM _values v JOIN _objects o ON o._id = v._id_object WHERE o._name LIKE '{_tag}%'");
        Assert.Equal(valueIds, distinctValueIds);
    }

    /// <summary>Recognized as a deadlock by <see cref="DbErrorClassifier"/> (the type name and SqlState 40P01).</summary>
    private sealed class PostgresException : Exception
    {
        public PostgresException() : base("simulated deadlock") { }
        public string SqlState => "40P01";
    }

    [Fact]
    public async Task RetryHelper_FirstAttemptRolledBack_AnotherSaveBetween_KeepsBothObjects()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var a = NewGraph("A");
        long cId = 0;
        var attempt = 0;

        var aId = await DeadlockRetryHelper.ExecuteWithRetryAsync(async () =>
        {
            attempt++;
            if (attempt == 2)
            {
                // Another unit of work saves between the attempts, from its own scope.
                await using var other = _services.CreateAsyncScope();
                cId = await other.ServiceProvider.GetRequiredService<IRedbService>().SaveAsync(NewGraph("C"));
            }
            await using var tx = await redb.Context.BeginTransactionAsync();
            var top = attempt == 1 ? await StarveKeysAsync(redb) : null;
            var id = await redb.SaveAsync(a);
            if (attempt == 1)
            {
                AssertTakenInsideTransaction(top, id, "A");
                throw new PostgresException();
            }
            await tx.CommitAsync();
            return id;
        }, baseDelayMs: 1);

        Assert.Equal(2, attempt);
        await AssertGraphAsync(redb, cId, "C");
        await AssertGraphAsync(redb, aId, "A");
        Assert.NotEqual(aId, cId);
        Assert.Equal(4, await CountOwnObjectsAsync(redb));
    }

    [Fact]
    public async Task ExistingObject_InARolledBackSave_KeepsItsId_NewNestedObjectGoesBackToZero()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var e = NewGraph("E");
        var eId = await redb.SaveAsync(e);
        var oldLeafId = e.Props.Leaf!.Id;

        e.Props.Text = "changed inside";
        e.Props.Leaf = new RedbObject<RolledBackSaveLeafProps>
        {
            name = _tag + "E-leaf2",
            Props = new RolledBackSaveLeafProps { Note = "second leaf" },
        };
        await using (var tx = await redb.Context.BeginTransactionAsync())
        {
            var top = await StarveKeysAsync(redb);
            await redb.SaveAsync(e);
            AssertTakenInsideTransaction(top, e.Props.Leaf.Id, "the new leaf");
            await tx.RollbackAsync();
        }
        var (eIdAfterRollback, leafIdAfterRollback) = (e.Id, e.Props.Leaf.Id);
        var rolledBackLeaf = e.Props.Leaf;

        var b = NewGraph("B");
        var bId = await redb.SaveAsync(b);
        e.Props.Text = "written by E";
        e.Props.Leaf = new RedbObject<RolledBackSaveLeafProps>
        {
            name = _tag + "E-leaf3",
            Props = new RolledBackSaveLeafProps { Note = "leaf of E" },
        };
        await redb.SaveAsync(e);

        // The leaf of the rolled-back save, saved on its own now: a row of its own, not B's or B's leaf's.
        var leafId = await redb.SaveAsync(rolledBackLeaf);

        await AssertGraphAsync(redb, bId, "B");
        await AssertGraphAsync(redb, eId, "E");
        Assert.Equal("second leaf", (await redb.LoadAsync<RolledBackSaveLeafProps>(leafId))!.Props.Note);
        Assert.NotEqual(0L, oldLeafId);
        Assert.Equal((eId, 0L), (eIdAfterRollback, leafIdAfterRollback));
    }

    [Fact]
    public async Task IdKeptFromARolledBackTransaction_SavedLater_IsNeverHandedOutAgain()
    {
        // An id the application took itself inside a transaction that rolled back, stored later with that id: the row
        // stands above the sequence. The ids handed out after it (another process does not know the in-memory mark,
        // which the fresh cache domain stands for here), and the trash container of a delete, must skip it.
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        long held;
        await using (var tx = await redb.Context.BeginTransactionAsync())
        {
            var top = await StarveKeysAsync(redb);
            held = (await redb.Context.NextObjectIdBatchAsync(1))[0];
            AssertTakenInsideTransaction(top, held, "the explicit id");
            await tx.RollbackAsync();
        }
        // The bulk insert keeps preset ids (SaveAsync would switch an unknown id to a new one).
        var kept = NewGraph("K");
        kept.Props.Leaf = null;
        kept.Id = held;
        await redb.AddNewObjectsAsync(new IRedbObject<RolledBackSaveProps>[] { kept });
        Assert.Equal(held, kept.Id);
        // Another process: its first range comes from the stored sequence, not from this process's mark.
        if (SequenceTopSql != null)
            await redb.Context.ExecuteAsync($"UPDATE sqlite_sequence SET seq = {held - 1} WHERE name = '_global_identity' AND seq >= {held}");

        var victim = NewGraph("V");
        var victimId = await redb.SaveAsync(victim);
        Assert.True(await redb.DeleteAsync(victimId));
        RedbKeyGeneratorBase.ClearAllCaches();
        var b = NewGraph("B");
        var bId = await redb.SaveAsync(b);

        var keptBack = await redb.LoadAsync<RolledBackSaveProps>(held);
        Assert.Equal((_tag + "K", "written by K"), (keptBack!.name, keptBack.Props.Text));
        await AssertGraphAsync(redb, bId, "B");
        Assert.NotEqual(held, bId);
    }

    [Fact]
    public async Task SchemeCreatedInARolledBackTransaction_IsCreatedAgainOnTheNextSave()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await DropFreshSchemeAsync(redb);

        var first = new RedbObject<RolledBackSaveFreshProps>
        {
            name = _tag + "fresh-1",
            Props = new RolledBackSaveFreshProps { Text = "first", Number = 1 },
        };
        await using (var tx = await redb.Context.BeginTransactionAsync())
        {
            await StarveKeysAsync(redb);
            await redb.SaveAsync(first);
            await tx.RollbackAsync();
        }
        Assert.Equal(0L, await redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _schemes WHERE _name = 'RolledBackSaveFreshScheme'"));

        var second = new RedbObject<RolledBackSaveFreshProps>
        {
            name = _tag + "fresh-2",
            Props = new RolledBackSaveFreshProps { Text = "second", Number = 2 },
        };
        var secondId = await redb.SaveAsync(second);
        var firstId = await redb.SaveAsync(first);

        var loadedSecond = await redb.LoadAsync<RolledBackSaveFreshProps>(secondId);
        var loadedFirst = await redb.LoadAsync<RolledBackSaveFreshProps>(firstId);
        Assert.Equal(("second", 2), (loadedSecond!.Props.Text, loadedSecond.Props.Number));
        Assert.Equal(("first", 1), (loadedFirst!.Props.Text, loadedFirst.Props.Number));
        var found = await redb.Query<RolledBackSaveFreshProps>().Where(p => p.Number == 2).ToListAsync();
        Assert.Contains(found, o => o.Id == secondId);
    }

    // Dropped at the start, before this process cached the scheme (no other test uses it), so a rerun on the same
    // database starts without it too.
    private static async Task DropFreshSchemeAsync(IRedbService redb)
    {
        const string scheme = "(SELECT _id FROM _schemes WHERE _name = 'RolledBackSaveFreshScheme')";
        await redb.Context.ExecuteAsync($"DELETE FROM _objects WHERE _id_scheme IN {scheme}");
        await redb.Context.ExecuteAsync($"DELETE FROM _scheme_metadata_cache WHERE _scheme_id IN {scheme}");
        await redb.Context.ExecuteAsync($"DELETE FROM _structures WHERE _id_scheme IN {scheme}");
        await redb.Context.ExecuteAsync("DELETE FROM _schemes WHERE _name = 'RolledBackSaveFreshScheme'");
    }

    [Fact]
    public async Task StarvedCache_ParallelSavesWithRollbacks_EveryCommittedObjectKeepsItsData()
    {
        // The shape of the failures seen under load: the key cache starved, writers in parallel, some transactions
        // rolled back and their objects saved again.
        const int workers = 4;
        const int iterations = 40;
        var committed = new System.Collections.Concurrent.ConcurrentBag<(long Id, string Who)>();

        async Task WorkAsync(int worker)
        {
            await using var scope = _services.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            var rolledBack = new List<(RedbObject<RolledBackSaveProps> Obj, string Who)>();

            async Task SaveAsync(RedbObject<RolledBackSaveProps> obj, string who, bool rollback)
            {
                await using var tx = await redb.Context.BeginTransactionAsync();
                await StarveKeysAsync(redb);
                await redb.SaveAsync(obj);
                if (rollback)
                {
                    await tx.RollbackAsync();
                    rolledBack.Add((obj, who));
                }
                else
                {
                    await tx.CommitAsync();
                    committed.Add((obj.Id, who));
                }
            }

            for (var i = 0; i < iterations; i++)
            {
                var who = $"w{worker}-{i}";
                await SaveAsync(NewGraph(who), who, rollback: (i + worker) % 3 == 0);
                // The retries come later, after this worker's and the other workers' saves took ids: every third
                // iteration retries the oldest rolled-back object, the rest wait for the end.
                if (i % 3 == 2 && rolledBack.Count > 0)
                {
                    var (obj, w) = rolledBack[0];
                    rolledBack.RemoveAt(0);
                    await SaveAsync(obj, w, rollback: false);
                }
            }
            while (rolledBack.Count > 0)
            {
                var (obj, w) = rolledBack[0];
                rolledBack.RemoveAt(0);
                await SaveAsync(obj, w, rollback: false);
            }
        }

        await Task.WhenAll(Enumerable.Range(0, workers).Select(WorkAsync));

        await using var check = _services.CreateAsyncScope();
        var reader = check.ServiceProvider.GetRequiredService<IRedbService>();
        var lost = new List<string>();
        foreach (var (id, who) in committed)
        {
            var loaded = await reader.LoadAsync<RolledBackSaveProps>(id);
            if (loaded?.name != _tag + who || loaded.Props.Text != "written by " + who)
                lost.Add($"{who} (id {id}) loads as '{loaded?.name}'");
        }
        Assert.True(lost.Count == 0, $"{lost.Count} of {committed.Count} saved objects lost their data: {string.Join("; ", lost.Take(10))}");
        foreach (var (id, who) in committed)
            await AssertGraphAsync(reader, id, who);
        Assert.Equal(workers * iterations, committed.Select(c => c.Id).Distinct().Count());
        Assert.Equal(workers * iterations * 2, await CountOwnObjectsAsync(reader));
    }
}
