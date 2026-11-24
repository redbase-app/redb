using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A delete answers with what it deleted, also when another session deletes the same object meanwhile. The race is
/// made deterministic: another session holds a lock on the object's row, the delete of redb runs until it waits on that
/// lock, then the holder deletes the object (or moves it to the trash) and commits. SQL Server's cascade procedures
/// returned the number of ids they had collected, so DeleteAsync answered true and the batch counted the object; a soft
/// delete moved an object already in the trash into its own trash and counted it (SQL Server and PostgreSQL).
/// </summary>
public abstract class ConcurrentDeleteCountTestsBase
{
    protected readonly IRedbService Redb;
    private readonly IServiceProvider _services;

    protected ConcurrentDeleteCountTestsBase(IRedbService redb, IServiceProvider services)
    {
        Redb = redb;
        _services = services;
    }

    /// <summary>A session of its own holding an update lock on one object's row, inside an open transaction.</summary>
    public interface IRowHolder : IAsyncDisposable
    {
        /// <summary>Returns once another session waits on the held lock; fails after the timeout.</summary>
        Task WaitUntilSomeoneWaitsAsync(TimeSpan timeout);

        /// <summary>Runs the statement in the holder's transaction and commits it.</summary>
        Task ExecuteAndCommitAsync(string sql);
    }

    protected abstract Task<IRowHolder> HoldRowAsync(long objectId);

    private async Task<long> NewObjectAsync(string tag)
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        return await Redb.SaveAsync(new RedbObject<SimpleProps> { name = tag, Props = new SimpleProps { Title = tag } });
    }

    private static string NewTag() => "cdc-" + Guid.NewGuid().ToString("N")[..8];

    /// <summary>The values go with the object: an FK cascade on PostgreSQL and SQLite, the INSTEAD OF trigger on SQL Server.</summary>
    private static string DeleteStatement(long id) => $"DELETE FROM _objects WHERE _id = {id}";

    /// <summary>The holder deletes the object while <paramref name="delete"/> waits on its lock; returns what the delete answered.</summary>
    private async Task<T> RaceAsync<T>(long heldId, string holderStatement, Func<Task<T>> delete)
    {
        var holder = await HoldRowAsync(heldId);
        var pending = Task.Run(delete);
        try
        {
            await holder.WaitUntilSomeoneWaitsAsync(TimeSpan.FromSeconds(20));
            await holder.ExecuteAndCommitAsync(holderStatement);
        }
        catch
        {
            // The delete must not outlive the test on the shared service: release the lock and let it finish.
            await holder.DisposeAsync();
            try { await pending; } catch { /* the holder's failure is the one reported */ }
            throw;
        }
        await holder.DisposeAsync();
        return await pending;
    }

    [Fact]
    public async Task DeleteById_OfAnObjectAnotherSessionDeletedMeanwhile_AnswersFalse()
    {
        var id = await NewObjectAsync(NewTag());

        var answer = await RaceAsync(id, DeleteStatement(id), () => Redb.DeleteAsync(id));

        answer.Should().BeFalse("the other session deleted the object; this call deleted nothing");
    }

    [Fact]
    public async Task DeleteObject_OfAnObjectAnotherSessionDeletedMeanwhile_AnswersFalse()
    {
        var id = await NewObjectAsync(NewTag());
        var obj = new RedbObject<SimpleProps> { id = id };

        var answer = await RaceAsync(id, DeleteStatement(id), () => Redb.DeleteAsync(obj));

        answer.Should().BeFalse("the other session deleted the object; this call deleted nothing");
    }

    [Fact]
    public async Task DeleteBatch_CountsOnlyTheObjectsThisCallDeleted()
    {
        var tag = NewTag();
        var raced = await NewObjectAsync(tag);
        var mine = await NewObjectAsync(tag);

        var count = await RaceAsync(raced, DeleteStatement(raced), () => Redb.DeleteAsync(new[] { raced, mine }));

        count.Should().Be(1, "of the two requested objects this call deleted one; the other session deleted the other");
        (await Redb.LoadAsync<SimpleProps>(mine)).Should().BeNull();
    }

    [Fact]
    public async Task DeleteBatch_DoesNotCountDescendants()
    {
        var tag = NewTag();
        var parent = new TreeRedbObject<SimpleProps> { name = tag, Props = new SimpleProps { Title = tag } };
        parent.id = await Redb.SaveAsync(parent);
        await Redb.CreateChildAsync(new TreeRedbObject<SimpleProps> { name = tag + "-c1", Props = new SimpleProps { Title = tag } }, parent);
        await Redb.CreateChildAsync(new TreeRedbObject<SimpleProps> { name = tag + "-c2", Props = new SimpleProps { Title = tag } }, parent);

        (await Redb.DeleteAsync(new[] { parent.id })).Should().Be(1, "one object was requested; its children go with it uncounted, on every provider");
    }

    [Fact]
    public async Task SoftDelete_OfAnObjectAnotherSessionMovedToTheTrashMeanwhile_MarksNothing()
    {
        var id = await NewObjectAsync(NewTag());

        var mark = await RaceAsync(id, $"UPDATE _objects SET _id_scheme = -10 WHERE _id = {id}",
            () => Redb.SoftDeleteAsync(new[] { id }));
        try
        {
            mark.MarkedCount.Should().Be(0, "the other session had moved the object to the trash; this call moved nothing");
            (await Redb.Context.ExecuteScalarAsync<long?>($"SELECT _id_parent FROM _objects WHERE _id = {id}"))
                .Should().NotBe(mark.TrashId, "the object stays where the other session put it, not in a second trash");
        }
        finally
        {
            await Redb.Context.ExecuteAsync(DeleteStatement(id));
            await Redb.Context.ExecuteAsync(DeleteStatement(mark.TrashId));
        }
    }

    /// <summary>
    /// The same race without choreography: several sessions delete one object at once, each through a scope (a connection)
    /// of its own. Exactly one of them deleted it, so the answers add up to one, round after round.
    /// </summary>
    [Fact]
    public async Task ConcurrentDeletesOfOneObject_ExactlyOneAnswersTrue()
    {
        const int rounds = 30, sessions = 8;
        var tag = NewTag();
        var wrong = new List<string>();
        for (var round = 0; round < rounds; round++)
        {
            var id = await NewObjectAsync($"{tag}-{round}");
            using var start = new Barrier(sessions);
            var answers = await Task.WhenAll(Enumerable.Range(0, sessions).Select(_ => Task.Run(async () =>
            {
                using var scope = _services.CreateScope();
                var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
                start.SignalAndWait();
                return await redb.DeleteAsync(id);
            })));
            var yes = answers.Count(a => a);
            if (yes != 1) wrong.Add($"round {round}: {yes} of {sessions} answered true");
        }

        wrong.Should().BeEmpty("one object was deleted once in every round");
    }
}