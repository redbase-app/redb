using System.Data;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// BR-1 (owner decision 2026-09-02): the transaction isolation level is an optional parameter of
/// <c>BeginTransactionAsync</c> / <c>ExecuteAtomicAsync</c>. Without it nothing changes - each
/// provider keeps its own default. PostgreSQL and MSSQL apply the requested level to the
/// transaction they open; SQLite accepts the parameter for portability and stays what it always
/// is, a single serial writer (BEGIN IMMEDIATE). An active or ambient transaction is joined as it
/// is - its level is never changed. Free and Pro share the connection classes, so the three Free
/// suites cover both editions.
/// </summary>
public abstract class TransactionIsolationTestsBase
{
    protected readonly IRedbService Redb;

    protected TransactionIsolationTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>SQL returning the current isolation level as text; null when the provider has no readout (SQLite).</summary>
    protected abstract string? ReadLevelSql { get; }

    protected virtual string SerializableLevelText => "serializable";
    protected virtual string DefaultLevelText => "read committed";

    [Fact]
    public async Task ExplicitLevel_IsApplied_AndTheDefaultIsUntouched()
    {
        if (ReadLevelSql is { } sql)
        {
            await using (var tx = await Redb.Context.BeginTransactionAsync(IsolationLevel.Serializable))
            {
                (await Redb.Context.ExecuteScalarAsync<string>(sql)).Should().Be(SerializableLevelText,
                    "the requested level must reach the transaction the provider opens");
                await tx.RollbackAsync();
            }

            await using (var tx = await Redb.Context.BeginTransactionAsync())
            {
                (await Redb.Context.ExecuteScalarAsync<string>(sql)).Should().Be(DefaultLevelText,
                    "the parameter is optional and the default must stay exactly what it was");
                await tx.RollbackAsync();
            }
        }
        else
        {
            // SQLite: no readout - the parameter must be accepted and the transaction fully usable.
            await using var tx = await Redb.Context.BeginTransactionAsync(IsolationLevel.Serializable);
            (await Redb.Context.ExecuteScalarAsync<long?>("SELECT 1")).Should().Be(1);
            await tx.RollbackAsync();
        }
    }

    [Fact]
    public async Task ExecuteAtomic_WithLevel_CommitsTheWork()
    {
        long id = 0;
        await Redb.Context.ExecuteAtomicAsync(IsolationLevel.Serializable, async () =>
        {
            id = await Redb.SaveAsync(new RedbObject<SimpleProps>
            {
                name = "iso-atomic",
                Props = new SimpleProps { Title = "iso", Count = 7 }
            });
        });

        (await Redb.LoadAsync<SimpleProps>(id, depth: 1))!.Props.Title.Should().Be("iso",
            "work done under the isolation-level overload commits like the plain form");
    }
}
