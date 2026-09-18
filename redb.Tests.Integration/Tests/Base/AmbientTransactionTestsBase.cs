using System.Transactions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// redb inside an ambient <see cref="TransactionScope"/> (plan docs/V4/AMBIENT_TRANSACTION_PLAN.md, 2026-09-14).
/// A redb.Route <c>.Transacted()</c> block is a TransactionScope, and every database write inside it - redb
/// objects, raw SQL through <c>redb.Context</c>, a second DI scope of the same database - must commit or roll
/// back with that scope. Before the plan none of the three providers did it:
/// <list type="bullet">
///   <item>MSSQL - a speculative ROLLBACK right after opening a connection killed the enlistment; raw SQL
///   autocommitted and the batch save failed on its savepoint;</item>
///   <item>PostgreSQL and MSSQL - the key generator and a second scope opened a second connection inside the
///   scope (MSSQL: implicit distributed transaction refused; PostgreSQL: abort at commit);</item>
///   <item>SQLite - the driver never takes part in System.Transactions, so everything autocommitted silently.</item>
/// </list>
/// Verification always goes through a fresh DI scope OUTSIDE any transaction.
/// </summary>
public abstract class AmbientTransactionTestsBase : IAsyncLifetime
{
    private const string NamePrefix = "ambient-tx-";
    private readonly IServiceProvider _services;

    // Free and Pro collections of one provider share a database and may run at the same time: every test
    // removes only the tags it created, never a whole prefix.
    private readonly List<string> _tags = new();

    protected AmbientTransactionTestsBase(IServiceProvider services) => _services = services;

    /// <summary>Idempotent DDL of the probe table <c>ambient_tx_rows (tag)</c> in the provider's dialect.</summary>
    protected abstract string CreateProbeTableSql { get; }

    /// <summary>
    /// A standalone wrapper of this collection's database, outside DI. With <paramref name="differentSessionSettings"/>
    /// it carries a session setting the default one does not (lazy references on MSSQL and PostgreSQL, Unicode case
    /// folding on SQLite).
    /// </summary>
    protected abstract IRedbConnection CreateStandaloneConnection(IServiceProvider services, bool differentSessionSettings);

    protected static string ConnString(string name) =>
        new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build()
            .GetConnectionString(name)!;

    public async Task InitializeAsync()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SyncSchemeAsync<UniqueOrderProps>();
        await redb.Context.ExecuteAsync(CreateProbeTableSql);
    }

    public async Task DisposeAsync()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        foreach (var tag in _tags)
        {
            await redb.Context.ExecuteAsync($"DELETE FROM _objects WHERE _name LIKE '{tag}%'");
            await redb.Context.ExecuteAsync($"DELETE FROM ambient_tx_rows WHERE tag LIKE '{tag}%'");
        }
    }

    private static TransactionScope NewScope() => new(TransactionScopeAsyncFlowOption.Enabled);

    private string NewTag()
    {
        var tag = NamePrefix + Guid.NewGuid().ToString("N")[..10];
        _tags.Add(tag);
        return tag;
    }

    private static Task<int> InsertRowAsync(IRedbService redb, string tag)
        => redb.Context.ExecuteAsync("INSERT INTO ambient_tx_rows (tag) VALUES ($1)", tag);

    private async Task<(long Objects, long Rows)> CountCommittedAsync(string tag)
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var objects = await redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(*) FROM _objects WHERE _name LIKE '{tag}%'");
        var rows = await redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(*) FROM ambient_tx_rows WHERE tag LIKE '{tag}%'");
        return (objects, rows);
    }

    [Fact]
    public async Task SaveAndRawSql_CommitWithScope()
    {
        var tag = NewTag();

        using (var tx = NewScope())
        {
            await using var scope = _services.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            await redb.SaveAsync(TestDataFactory.CreateSimple(tag, 1m));
            await InsertRowAsync(redb, tag);
            tx.Complete();
        }

        (await CountCommittedAsync(tag)).Should().Be((1L, 1L),
            "the object and the raw row commit together with the scope");
    }

    [Fact]
    public async Task SaveAndRawSql_RollbackWithScope()
    {
        var tag = NewTag();

        using (NewScope())
        {
            await using var scope = _services.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            await redb.SaveAsync(TestDataFactory.CreateSimple(tag, 2m));
            await InsertRowAsync(redb, tag);
            // not completed
        }

        (await CountCommittedAsync(tag)).Should().Be((0L, 0L),
            "an abandoned scope takes back both the object and the raw row");
    }

    [Fact]
    public async Task ConnectionOpenedBeforeScope_JoinsScope()
    {
        var tag = NewTag();
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        // The scope's connection is already open when the transaction starts - the shape of a route that
        // used redb in an earlier step of the same exchange.
        await redb.Context.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM ambient_tx_rows");

        using (NewScope())
        {
            await redb.SaveAsync(TestDataFactory.CreateSimple(tag, 3m));
            await InsertRowAsync(redb, tag);
            // not completed
        }

        (await CountCommittedAsync(tag)).Should().Be((0L, 0L),
            "work done inside the scope rolls back with it even on a connection opened before it");
    }

    [Fact]
    public async Task ColdKeyCacheInsideScope_NoSecondConnection()
    {
        var committed = NewTag();
        var abandoned = NewTag();

        RedbKeyGeneratorBase.ClearAllCaches();
        using (var tx = NewScope())
        {
            await using var scope = _services.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<IRedbService>()
                .SaveAsync(TestDataFactory.CreateSimple(committed, 4m));
            tx.Complete();
        }

        RedbKeyGeneratorBase.ClearAllCaches();
        using (NewScope())
        {
            await using var scope = _services.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<IRedbService>()
                .SaveAsync(TestDataFactory.CreateSimple(abandoned, 5m));
        }

        (await CountCommittedAsync(committed)).Objects.Should().Be(1, "a cold key cache inside the scope still commits");
        (await CountCommittedAsync(abandoned)).Objects.Should().Be(0, "and still rolls back");
    }

    [Fact]
    public async Task TwoScopesSameDatabase_OneTransaction()
    {
        var committed = NewTag();
        var abandoned = NewTag();

        using (var tx = NewScope())
        {
            await using var first = _services.CreateAsyncScope();
            await using var second = _services.CreateAsyncScope();
            await first.ServiceProvider.GetRequiredService<IRedbService>()
                .SaveAsync(TestDataFactory.CreateSimple(committed + "-a", 6m));
            await InsertRowAsync(second.ServiceProvider.GetRequiredService<IRedbService>(), committed);
            tx.Complete();
        }

        using (NewScope())
        {
            await using var first = _services.CreateAsyncScope();
            await using var second = _services.CreateAsyncScope();
            await first.ServiceProvider.GetRequiredService<IRedbService>()
                .SaveAsync(TestDataFactory.CreateSimple(abandoned + "-a", 7m));
            await InsertRowAsync(second.ServiceProvider.GetRequiredService<IRedbService>(), abandoned);
        }

        (await CountCommittedAsync(committed)).Should().Be((1L, 1L),
            "two DI scopes of one database write into the same transaction");
        (await CountCommittedAsync(abandoned)).Should().Be((0L, 0L),
            "and roll back together");
    }

    [Fact]
    public async Task UniqueViolationInsideScope_TransactionStaysAlive()
    {
        var tag = NewTag();
        await using (var setup = _services.CreateAsyncScope())
        {
            await setup.ServiceProvider.GetRequiredService<IRedbService>().SaveAsync(new RedbObject<UniqueOrderProps>
                { name = tag + "-winner", ValueUnique = tag + "-KEY", Props = new UniqueOrderProps { Note = "first" } });
        }

        using (var tx = NewScope())
        {
            await using var scope = _services.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

            var act = async () => await redb.SaveAsync(new RedbObject<UniqueOrderProps>
                { name = tag + "-loser", ValueUnique = tag + "-KEY", Props = new UniqueOrderProps { Note = "second" } });
            await act.Should().ThrowAsync<RedbUniqueViolationException>();

            (await redb.Context.ExecuteScalarAsync<long?>("SELECT 1")).Should().Be(1,
                "the scope's transaction survives a caught unique violation");
            await redb.SaveAsync(new RedbObject<UniqueOrderProps>
                { name = tag + "-recovered", ValueUnique = tag + "-OTHER", Props = new UniqueOrderProps { Note = "third" } });
            tx.Complete();
        }

        (await CountCommittedAsync(tag + "-recovered")).Objects.Should().Be(1,
            "work done after the recovery commits with the scope");
        (await CountCommittedAsync(tag + "-loser")).Objects.Should().Be(0);
    }

    [Fact]
    public async Task ExecuteAtomicWithoutScope_Unchanged()
    {
        var committed = NewTag();
        var failed = NewTag();
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        await redb.Context.ExecuteAtomicAsync(async () =>
        {
            await redb.SaveAsync(TestDataFactory.CreateSimple(committed, 8m));
            await InsertRowAsync(redb, committed);
        });

        var act = async () => await redb.Context.ExecuteAtomicAsync(async () =>
        {
            await redb.SaveAsync(TestDataFactory.CreateSimple(failed, 9m));
            await InsertRowAsync(redb, failed);
            throw new InvalidOperationException("ambient-tx-marker");
        });
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*ambient-tx-marker*");

        (await CountCommittedAsync(committed)).Should().Be((1L, 1L));
        (await CountCommittedAsync(failed)).Should().Be((0L, 0L));
    }

    [Fact]
    public async Task BeginTransactionInsideScope_StillRefused()
    {
        using var tx = NewScope();
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        var act = async () => await redb.Context.BeginTransactionAsync();
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*Ambient TransactionScope*",
            "an explicit transaction inside a scope stays refused; a route joins the scope instead");
    }

    [Fact]
    public async Task BatchSaveInsideScope_RollsBack()
    {
        var tag = NewTag();
        var batch = Enumerable.Range(0, 25)
            .Select(i => (IRedbObject)TestDataFactory.CreateSimple($"{tag}-{i:D2}", i))
            .ToList();

        using (NewScope())
        {
            await using var scope = _services.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<IRedbService>().SaveAsync(batch);
        }

        (await CountCommittedAsync(tag)).Objects.Should().Be(0, "the bulk path of a batch save rolls back with the scope");
    }

    [Fact]
    public async Task ConcurrentCommandsInOneTransaction_Fail()
    {
        // Owner decision P1: parallel branches in ONE transaction are not supported and must fail loudly,
        // never write silently. Two dependent clones write at the same time.
        var tag = NewTag();
        Exception? failure = null;

        try
        {
            using var tx = NewScope();
            var start = new Barrier(2);
            var branches = Enumerable.Range(0, 2).Select(branch =>
            {
                var dependent = Transaction.Current!.DependentClone(DependentCloneOption.BlockCommitUntilComplete);
                return Task.Run(async () =>
                {
                    try
                    {
                        using var branchScope = new TransactionScope(dependent, TransactionScopeAsyncFlowOption.Enabled);
                        await using var scope = _services.CreateAsyncScope();
                        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
                        start.SignalAndWait();
                        for (var i = 0; i < 200; i++)
                            await InsertRowAsync(redb, $"{tag}-{branch}-{i}");
                        branchScope.Complete();
                    }
                    finally
                    {
                        dependent.Complete();
                    }
                });
            }).ToArray();

            await Task.WhenAll(branches);
            tx.Complete();
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        failure.Should().NotBeNull("concurrent writes inside one transaction must fail, not commit silently");
        (await CountCommittedAsync(tag)).Rows.Should().Be(0, "nothing of a failed transaction may stay");
    }

    [Fact]
    public async Task OtherSessionSettings_SameDatabase_OneTransaction_AreRefused()
    {
        // The transaction holds one connection per database, set up by the configuration that opened it. A second
        // redb configuration of the same database with other session settings used to run its commands on it with
        // settings that are not its own (lazy-reference JSON, collation, case folding), without any error.
        await using var first = CreateStandaloneConnection(_services, differentSessionSettings: false);
        await using var other = CreateStandaloneConnection(_services, differentSessionSettings: true);

        using var tx = NewScope();
        (await first.ExecuteScalarAsync<long>("SELECT 1")).Should().Be(1);

        var act = () => other.ExecuteScalarAsync<long>("SELECT 1");
        (await act.Should().ThrowAsync<InvalidOperationException>())
            .Which.Message.Should().Contain("session settings");
    }

    [Fact]
    public async Task SameSessionSettings_TwoWrappers_ShareTheTransaction()
    {
        // Characterization, green before and after: the refusal above must not touch identical configurations.
        var tag = NewTag();
        await using var first = CreateStandaloneConnection(_services, differentSessionSettings: false);
        await using var second = CreateStandaloneConnection(_services, differentSessionSettings: false);

        using (NewScope())
        {
            await first.ExecuteAsync("INSERT INTO ambient_tx_rows (tag) VALUES ($1)", tag);
            await second.ExecuteAsync("INSERT INTO ambient_tx_rows (tag) VALUES ($1)", tag + "-2");
            // not completed
        }

        (await CountCommittedAsync(tag)).Rows.Should().Be(0, "both wrappers wrote into the one abandoned transaction");
    }
}
