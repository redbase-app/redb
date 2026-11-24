using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Export.Providers;
using redb.Export.Services;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// redb.Export checks the schema of its database before it reads or writes anything, with the exceptions a redb service
/// throws at start. An older source or target stops with <see cref="RedbSchemaOutdatedException"/>; a newer source is
/// refused for an export whatever it serves, since the file would leave out what this build does not know; a newer
/// target passes for an import only when it says it serves this build. Before, the first statement naming a column the
/// database lacked failed with a driver error, and a parallel import had already committed its cleaning by then.
/// "Older" is staged as an older version stated (on SQLite also without the table of its newest step); "newer" as a
/// newer version with or without the compatibility statement. Every test runs on scratch databases of its own.
/// </summary>
public abstract class ExportSchemaGateTestsBase
{
    /// <summary>The provider name of <see cref="ProviderFactory"/>: postgres, mssql or sqlite.</summary>
    protected abstract string ProviderName { get; }

    /// <summary>Whether the provider loads over several connections (--parallel).</summary>
    protected abstract bool SupportsParallel { get; }

    /// <summary>A text the remedy of an older database names on this provider.</summary>
    protected abstract string OutdatedRemedy { get; }

    /// <summary>A new empty database of this run; the subclass drops it afterwards.</summary>
    protected abstract Task<string> NewDatabaseAsync(string suffix);

    /// <summary>A redb service over <paramref name="cs"/> with a cache domain of its own.</summary>
    protected abstract ServiceProvider Build(string cs);

    /// <summary>A file the subclass deletes afterwards.</summary>
    protected abstract string NewFile();

    /// <summary>The database stating an older version (on SQLite also without the table of its newest step).</summary>
    protected abstract Task StageOlderAsync(string cs);

    /// <summary>A newer version stated, serving this build or not.</summary>
    protected abstract Task StageNewerAsync(string cs, bool servesThisBuild);

    /// <summary>The first column of the first row as text.</summary>
    protected abstract Task<string?> ScalarAsync(string cs, string sql);

    /// <summary>
    /// The provider's part of what a load changes: the indexes with their state, the foreign keys with theirs, the table
    /// the parallel import keeps dropped index definitions in.
    /// </summary>
    protected abstract Task<List<string>> ProviderStateAsync(string cs);

    private static readonly string[] Tables =
    [
        "_types", "_lists", "_list_items", "_schemes", "_structures", "_objects", "_values",
        "_users", "_roles", "_users_roles", "_permissions",
    ];

    /// <summary>What an import may change: the row count of every redb table, then <see cref="ProviderStateAsync"/>.</summary>
    private async Task<List<string>> SnapshotAsync(string cs)
    {
        var lines = new List<string>();
        foreach (var table in Tables)
            lines.Add($"{table}={await ScalarAsync(cs, $"SELECT COUNT(*) FROM {table}")}");
        lines.AddRange(await ProviderStateAsync(cs));
        return lines;
    }

    /// <summary>Runs a statement on <paramref name="cs"/>.</summary>
    protected abstract Task ExecuteSqlAsync(string cs, string sql);

    private const string TimeOnlyTypeId = "-9223372036854775685";
    private const string TimeSpanTypeId = "-9223372036854775684";

    [Fact]
    public async Task AFileOfTheTextTimeVersion_KeepsTheTargetsTypes_AndTheImportNamesTheTextValues()
    {
        // A file of a version that stored TimeSpan and TimeOnly as text: its _types say String, its values are text.
        var source = await NewDatabaseAsync("old");
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true, typeof(ExportService).Assembly);
            await redb.SyncSchemeAsync<ContainsTypesProps>();
            await redb.SaveAsync(new RedbObject<ContainsTypesProps>
            {
                name = "times",
                Props = new ContainsTypesProps { Time = new TimeOnly(10, 30), Span = TimeSpan.FromMinutes(90) },
            });
        }
        await ExecuteSqlAsync(source, $"UPDATE _types SET _db_type = 'String' WHERE _id IN ({TimeOnlyTypeId}, {TimeSpanTypeId})");
        await ExecuteSqlAsync(source,
            $"UPDATE _values SET _String = 'text', _Long = NULL WHERE _id_structure IN " +
            $"(SELECT _id FROM _structures WHERE _id_type IN ({TimeOnlyTypeId}, {TimeSpanTypeId}))");
        var file = NewFile();
        await ExportAsync(source, file);

        var target = await SeededAsync("dst");
        var (_, output) = await CapturedAsync(() => ImportAsync(target, file, SupportsParallel ? 2 : 1));

        (await ScalarAsync(target, $"SELECT COUNT(*) FROM _types WHERE _id IN ({TimeOnlyTypeId}, {TimeSpanTypeId}) AND _db_type = 'Long'"))
            .Should().Be("2", "the target keeps its own built-in types whatever the file says");
        output.Should().Contain("Note: 2 imported TimeSpan or TimeOnly value(s) are stored as text by an older version")
            .And.Contain("TimeSpan and TimeOnly are stored as ticks");
        (await ScalarAsync(target, "SELECT COUNT(*) FROM _values WHERE _String = 'text'")).Should().Be("2",
            "the values stay as the file has them; the upgrade statement converts them");
    }

    [Fact]
    public async Task AnImport_BuildsTheSchemeMetadataCache_AsTheSchemeSyncDoes()
    {
        var (file, source) = await ExportOfSeededAsync();
        const string cacheSql =
            "SELECT COUNT(*) FROM _scheme_metadata_cache WHERE _scheme_id IN (SELECT _id FROM _schemes WHERE _name LIKE '%SimpleProps%')";
        var expected = await ScalarAsync(source, cacheSql);
        expected.Should().NotBe("0", "precondition: the scheme sync of the source built the cache");

        var target = await SeededAsync("dst");
        await ExecuteSqlAsync(target, "DELETE FROM _scheme_metadata_cache");
        await ImportAsync(target, file, SupportsParallel ? 2 : 1);

        (await ScalarAsync(target, cacheSql)).Should().Be(expected);
    }

    private static async Task<(int Code, string Output)> CapturedAsync(Func<Task> run)
    {
        var console = Console.Out;
        var output = new StringWriter();
        Console.SetOut(output);
        try
        {
            await run();
            return (0, output.ToString());
        }
        finally
        {
            Console.SetOut(console);
        }
    }

    /// <summary>How many tables named _schemes the database has: 0 for one without the redb schema.</summary>
    protected virtual string SchemesTableCountSql =>
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '_schemes'";

    [Fact]
    public async Task AnExport_FromADatabaseWithoutTheRedbSchema_SaysThereIsNothingToExport()
    {
        var empty = await NewDatabaseAsync("empty");
        var file = NewFile();

        var exporting = async () => await ExportAsync(empty, file);

        (await exporting.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("holds no redb schema").And.Contain("there is nothing to export")
            .And.NotContain("outdated", "a database without the schema is not an older one");
        File.Exists(file).Should().BeFalse();
    }

    [Fact]
    public async Task AnImport_IntoADatabaseWithoutTheRedbSchema_NamesRedbInit_AndTouchesNothing()
    {
        var (file, _) = await ExportOfSeededAsync();
        var empty = await NewDatabaseAsync("empty");

        var importing = async () => await ImportAsync(empty, file, SupportsParallel ? 2 : 1);

        (await importing.Should().ThrowAsync<InvalidOperationException>()).Which.Message
            .Should().Contain("holds no redb schema").And.Contain($"`redb init --provider {ProviderName}`")
            .And.NotContain("outdated");
        (await ScalarAsync(empty, SchemesTableCountSql)).Should().Be("0", "nothing was created");
    }

    [Fact]
    public async Task AnExport_FromAnOlderSource_IsRefused_BeforeTheFileIsWritten()
    {
        var source = await SeededAsync("src");
        await StageOlderAsync(source);
        var file = NewFile();

        var exporting = async () => await ExportAsync(source, file);

        var error = (await exporting.Should().ThrowAsync<RedbSchemaOutdatedException>()).Which;
        error.Message.Should().Contain("the redb CLI does not upgrade a database").And.Contain(OutdatedRemedy)
            .And.Contain("then export from it again");
        File.Exists(file).Should().BeFalse("nothing is read for a file from a database of another schema");
    }

    [Fact]
    public async Task ASerialImport_IntoAnOlderTarget_IsRefused_AndTouchesNothing()
    {
        await ImportIntoOlderTargetAsync(parallelism: 1);
    }

    [Fact]
    public async Task AParallelImport_IntoAnOlderTarget_IsRefused_AndTouchesNothing()
    {
        if (!SupportsParallel)
            return;
        await ImportIntoOlderTargetAsync(parallelism: 2);
    }

    [Fact]
    public async Task AnExport_FromANewerSource_IsRefused_EvenWhenItServesThisBuild()
    {
        var source = await SeededAsync("src");
        await StageNewerAsync(source, servesThisBuild: true);
        var file = NewFile();

        var exporting = async () => await ExportAsync(source, file);

        (await exporting.Should().ThrowAsync<RedbSchemaNewerException>()).Which.Message
            .Should().Contain("would leave out what its build does not know");
        File.Exists(file).Should().BeFalse();
    }

    [Fact]
    public async Task AnImport_IntoANewerTarget_ThatServesThisBuild_Passes()
    {
        var (file, source) = await ExportOfSeededAsync();
        var target = await TargetWithOneObjectAsync();
        await StageNewerAsync(target, servesThisBuild: true);

        await ImportAsync(target, file, parallelism: 1);

        var objects = (await SnapshotAsync(source)).Single(l => l.StartsWith("_objects=", StringComparison.Ordinal));
        (await SnapshotAsync(target)).Should().Contain(objects, "the import replaced the target's rows with the file's");
        (await SnapshotAsync(target)).Should().Contain(l => l.StartsWith("_values=", StringComparison.Ordinal) && l != "_values=0");
    }

    [Fact]
    public async Task AnImport_IntoANewerTarget_ThatDoesNotServeThisBuild_IsRefused_AndTouchesNothing()
    {
        var (file, _) = await ExportOfSeededAsync();
        var target = await TargetWithOneObjectAsync();
        await StageNewerAsync(target, servesThisBuild: false);
        var before = await SnapshotAsync(target);

        var importing = async () => await ImportAsync(target, file, SupportsParallel ? 2 : 1);

        (await importing.Should().ThrowAsync<RedbSchemaNewerException>()).Which.Message
            .Should().Contain("Import with a redb CLI of the version whose packages run on this database");
        (await SnapshotAsync(target)).Should().Equal(before);
    }

    private async Task ImportIntoOlderTargetAsync(int parallelism)
    {
        var (file, _) = await ExportOfSeededAsync();
        var target = await TargetWithOneObjectAsync();
        await StageOlderAsync(target);
        var before = await SnapshotAsync(target);
        before.Should().NotContain("_objects=0", "precondition: the target holds rows the cleaning would remove");

        var importing = async () => await ImportAsync(target, file, parallelism);

        (await importing.Should().ThrowAsync<RedbSchemaOutdatedException>()).Which.Message
            .Should().Contain(OutdatedRemedy).And.Contain("then import into it again");
        (await SnapshotAsync(target)).Should().Equal(before,
            "the check runs before the cleaning, the index drop and the constraint change of the load");
    }

    /// <summary>A database holding one object of <see cref="SimpleProps"/>.</summary>
    private async Task<string> SeededAsync(string suffix)
    {
        var cs = await NewDatabaseAsync(suffix);
        await using var provider = Build(cs);
        var redb = provider.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true, typeof(ExportService).Assembly);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SaveAsync(new RedbObject<SimpleProps> { name = $"{suffix}-object", Props = new SimpleProps { Title = suffix } });
        return cs;
    }

    private async Task<(string File, string Source)> ExportOfSeededAsync()
    {
        var source = await SeededAsync("src");
        var file = NewFile();
        await ExportAsync(source, file);
        return (file, source);
    }

    private Task<string> TargetWithOneObjectAsync() => SeededAsync("dst");

    private async Task ExportAsync(string cs, string file)
    {
        await using var provider = ProviderFactory.Create(ProviderName);
        await provider.OpenAsync(cs);
        await new ExportService(provider, verbose: false, batchSize: 1000).ExportAsync(file, null, compress: false, dryRun: false);
    }

    private async Task ImportAsync(string cs, string file, int parallelism)
    {
        await using (var provider = ProviderFactory.Create(ProviderName))
        {
            await provider.OpenAsync(cs);
            await new ImportService(provider, verbose: false, batchSize: 1000, parallelism: parallelism)
                .ImportAsync(file, clean: true, dryRun: false);
        }

        // The service that seeded the target holds a block of ids from before the import (see the CLI README).
        RedbKeyGeneratorBase.ClearAllCaches();
    }
}
