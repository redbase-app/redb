using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Data;
using redb.Core.Models.Entities;
using redb.Export.Providers;
using redb.Export.Services;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The finishing steps of a parallel import (the id sequence, the index rebuild, the constraints) are each attempted
/// whatever the others do, and their failures are reported together. A target that had lost its id sequence failed at
/// the sequence step, and the steps after it never ran: every secondary index of the large tables stayed disabled
/// (SQL Server) or dropped (PostgreSQL), and on SQL Server every constraint and trigger stayed off.
/// </summary>
public abstract class ParallelImportFinishTestsBase
{
    protected abstract string ProviderName { get; }

    protected abstract Task<string> NewDatabaseAsync(string suffix);

    protected abstract ServiceProvider Build(string cs);

    protected abstract string NewFile();

    protected abstract Task<string?> ScalarAsync(string cs, string sql);

    /// <summary>Drops the id sequence, as a damaged database had lost it.</summary>
    protected abstract Task DropSequenceAsync(string cs);

    /// <summary>The indexes with their state, the foreign keys and triggers with theirs.</summary>
    protected abstract Task<List<string>> ShapeAsync(string cs);

    [Fact]
    public async Task AFailingSequenceStep_StillRebuildsTheIndexes_AndRestoresTheConstraints_AndReportsTheStep()
    {
        var source = await SeededAsync("src");
        var file = NewFile();
        await using (var provider = ProviderFactory.Create(ProviderName))
        {
            await provider.OpenAsync(source);
            await new ExportService(provider, verbose: false, batchSize: 1000).ExportAsync(file, null, compress: false, dryRun: false);
        }

        var target = await SeededAsync("dst");
        await DropSequenceAsync(target);
        var shape = await ShapeAsync(target);

        Exception? failure = null;
        try
        {
            await using var provider = ProviderFactory.Create(ProviderName);
            await provider.OpenAsync(target);
            await new ImportService(provider, verbose: false, batchSize: 1000, parallelism: 2)
                .ImportAsync(file, clean: true, dryRun: false);
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        RedbKeyGeneratorBase.ClearAllCaches();

        (await ShapeAsync(target)).Should().Equal(shape, "the index rebuild and the constraints ran after the failed step");
        failure.Should().BeOfType<InvalidOperationException>().Which.Message
            .Should().Contain("1 of its finishing steps failed; every step was attempted").And.Contain("- id sequence:");
        (await ScalarAsync(target, "SELECT COUNT(*) FROM _values"))
            .Should().Be(await ScalarAsync(source, "SELECT COUNT(*) FROM _values"), "every row was written");
    }

    private async Task<string> SeededAsync(string suffix)
    {
        var cs = await NewDatabaseAsync(suffix);
        await using var provider = Build(cs);
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true, typeof(ExportService).Assembly);
        await redb.SyncSchemeAsync<SimpleProps>();
        for (var i = 0; i < 3; i++)
            await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"{suffix}-{i}", Props = new SimpleProps { Title = $"{suffix}{i}" } });
        return cs;
    }
}
