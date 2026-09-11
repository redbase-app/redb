using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// BR-9 (Tsak report, fixed 2026-09-02): LockForUpdateAsync on a deleted id used to be a silent
/// no-op - zero rows locked, no signal - and with the default MissingObjectStrategy.AutoSwitchToInsert
/// a CAS pattern (lock -> re-read -> save) could run on an unlocked row or resurrect a deleted
/// object. The lenient form now reports how many of the requested rows exist (and are locked); the
/// Required form throws naming the missing ids and refuses to run outside a transaction.
/// </summary>
public abstract class LockForUpdateTestsBase
{
    protected readonly IRedbService Redb;

    protected LockForUpdateTestsBase(IRedbService redb) => Redb = redb;

    private async Task<long> NewObjectAsync()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        return await Redb.SaveAsync(new RedbObject<SimpleProps>
            { name = "lock-probe", Props = new SimpleProps { Title = "lock" } });
    }

    [Fact]
    public async Task LockForUpdate_ReportsHowManyRequestedRowsExist()
    {
        var id = await NewObjectAsync();
        await Redb.Context.ExecuteAtomicAsync(async () =>
        {
            (await Redb.LockForUpdateAsync(id)).Should().Be(1);
            (await Redb.LockForUpdateAsync(id, -987654321L)).Should().Be(1,
                "a deleted or never-created id locks nothing - the count is the caller's signal");
            (await Redb.LockForUpdateAsync(id, id)).Should().Be(1,
                "the count is of DISTINCT existing ids, not of the argument list");
            (await Redb.LockForUpdateAsync()).Should().Be(0);
        });
    }

    [Fact]
    public async Task LockForUpdateRequired_NamesTheMissingIds()
    {
        var id = await NewObjectAsync();
        await Redb.Context.ExecuteAtomicAsync(async () =>
        {
            await Redb.LockForUpdateRequiredAsync(id); // every id exists - no throw

            var act = async () => await Redb.LockForUpdateRequiredAsync(id, -987654321L, -987654322L);
            (await act.Should().ThrowAsync<RedbLockNotAcquiredException>(
                    "a CAS must not proceed on an unlocked row"))
                .Which.MissingIds.Should().Equal(-987654322L, -987654321L);
        });
    }

    [Fact]
    public async Task LockForUpdateRequired_RefusesToRunOutsideATransaction()
    {
        var id = await NewObjectAsync();
        var act = async () => await Redb.LockForUpdateRequiredAsync(id);
        await act.Should().ThrowAsync<InvalidOperationException>(
            "a lock outside a transaction releases immediately and protects nothing");
    }
}
