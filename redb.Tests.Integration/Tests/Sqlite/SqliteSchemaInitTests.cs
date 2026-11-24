using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Export.Providers;
using redb.Export.Services;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// <c>redb init -p sqlite</c> (<c>Program.InitSchemaAsync</c>) leaves a file the export schema check and the SQLite
/// service both take as current: stamped as the service stamps a file it creates (<c>PRAGMA user_version</c>, one
/// <c>_schema_compat</c> row), and with the schema of this build, so the first service start changes nothing in it. The
/// schema script alone left the file at version 0, and the documented create, init, import was refused.
/// </summary>
[Collection("Console output")]
public class SqliteSchemaInitTests : IAsyncLifetime
{
    private readonly List<string> _databases = [];
    private readonly List<string> _files = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync()
    {
        SqliteConnection.ClearAllPools();
        foreach (var database in _databases)
            SqliteTestSupport.DeleteDbFiles(database);
        foreach (var file in _files)
            File.Delete(file);
        return Task.CompletedTask;
    }

    [Fact]
    public async Task Init_StampsTheFile_AsTheServiceDoes()
    {
        var cs = NewDatabase("init");

        (await global::redb.CLI.Program.InitSchemaAsync(cs, "sqlite", verbose: false, CancellationToken.None)).Should().Be(0);

        (await ScalarAsync(cs, "PRAGMA user_version")).Should().Be(global::redb.SQLite.RedbService.SchemaVersion.ToString());
        (await ScalarAsync(cs, "SELECT COUNT(*) FROM _schema_compat")).Should().Be("1");
    }

    [Fact]
    public async Task AFileMadeByInit_IsTheCurrentSchema_TheFirstServiceStartChangesNothing()
    {
        var cs = NewDatabase("init");
        (await global::redb.CLI.Program.InitSchemaAsync(cs, "sqlite", verbose: false, CancellationToken.None)).Should().Be(0);
        var before = await SchemaAsync(cs);

        await using (var provider = Build(cs))
            await provider.GetRequiredService<IRedbService>().InitializeAsync(typeof(ExportService).Assembly);

        (await SchemaAsync(cs)).Should().Equal(before, "no upgrade step had anything to add, drop or restamp");
    }

    [Fact]
    public async Task InitThenImport_ThePathTheCliReadmeDocuments_Passes()
    {
        var source = NewDatabase("src");
        await using (var provider = Build(source))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(ensureCreated: true, typeof(ExportService).Assembly);
            await redb.SyncSchemeAsync<SimpleProps>();
            await redb.SaveAsync(new RedbObject<SimpleProps> { name = "exported", Props = new SimpleProps { Title = "t" } });
        }
        var file = Path.Combine(Path.GetTempPath(), $"redb-init-{Guid.NewGuid():N}.redb");
        _files.Add(file);
        await using (var provider = ProviderFactory.Create("sqlite"))
        {
            await provider.OpenAsync(source);
            await new ExportService(provider, verbose: false, batchSize: 1000).ExportAsync(file, null, compress: false, dryRun: false);
        }

        var target = NewDatabase("dst");
        (await global::redb.CLI.Program.InitSchemaAsync(target, "sqlite", verbose: false, CancellationToken.None)).Should().Be(0);
        await using (var provider = ProviderFactory.Create("sqlite"))
        {
            await provider.OpenAsync(target);
            await new ImportService(provider, verbose: false, batchSize: 1000).ImportAsync(file, clean: true, dryRun: false);
        }

        (await ScalarAsync(target, "SELECT COUNT(*) FROM _objects WHERE _name = 'exported'")).Should().Be("1");
    }

    [Fact]
    public async Task InitOnAnExistingFile_KeepsItsDataAndStamp()
    {
        var cs = NewDatabase("again");
        (await global::redb.CLI.Program.InitSchemaAsync(cs, "sqlite", verbose: false, CancellationToken.None)).Should().Be(0);
        await using (var provider = Build(cs))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync(typeof(ExportService).Assembly);
            await redb.SyncSchemeAsync<SimpleProps>();
            await redb.SaveAsync(new RedbObject<SimpleProps> { name = "kept", Props = new SimpleProps { Title = "k" } });
        }
        var before = await SchemaAsync(cs);

        (await global::redb.CLI.Program.InitSchemaAsync(cs, "sqlite", verbose: false, CancellationToken.None)).Should().Be(0);

        (await SchemaAsync(cs)).Should().Equal(before);
        (await ScalarAsync(cs, "SELECT COUNT(*) FROM _objects WHERE _name = 'kept'")).Should().Be("1");
    }

    /// <summary>Every object of the schema with its DDL, the version stamp and the compatibility row.</summary>
    private static async Task<List<string>> SchemaAsync(string cs)
    {
        var lines = new List<string>();
        await using var connection = new SqliteConnection(cs);
        await connection.OpenAsync();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT type || ' ' || name || ': ' || IFNULL(sql, '') FROM sqlite_master ORDER BY type, name";
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                lines.Add(reader.GetString(0));
        }
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT 'compat ' || _min_reader || ' ' || _package FROM _schema_compat";
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                lines.Add(reader.GetString(0));
        }
        lines.Add("user_version " + await ScalarAsync(cs, "PRAGMA user_version"));
        return lines;
    }

    private string NewDatabase(string suffix)
    {
        global::redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var cs = $"Data Source=redb_tests_init_{suffix}_{Guid.NewGuid():N}.db";
        SqliteTestSupport.DeleteDbFiles(cs);
        _databases.Add(cs);
        return cs;
    }

    private static ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c => c.CacheDomain = $"init-{Guid.NewGuid():N}");
        });
        return services.BuildServiceProvider();
    }

    private static async Task<string?> ScalarAsync(string cs, string sql)
    {
        await using var connection = new SqliteConnection(cs);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToString(await command.ExecuteScalarAsync());
    }
}
