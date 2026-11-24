using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// <see cref="ExportSchemaGateTestsBase"/> on SQLite, scratch files deleted afterwards. SQLite loads over one
/// connection, so the parallel case does not apply.
/// </summary>
public class SqliteExportSchemaGateTests : ExportSchemaGateTestsBase, IAsyncLifetime
{
    private readonly List<string> _databases = [];
    private readonly List<string> _files = [];

    private static int SchemaVersion => global::redb.SQLite.RedbService.SchemaVersion;

    protected override string ProviderName => "sqlite";
    protected override bool SupportsParallel => false;
    protected override string SchemesTableCountSql => "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_schemes'";
    protected override string OutdatedRemedy => "Upgrade the file with `redb init --provider sqlite`";

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

    protected override Task<string> NewDatabaseAsync(string suffix)
    {
        global::redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var cs = $"Data Source=redb_tests_export_gate_{suffix}_{Guid.NewGuid():N}.db";
        SqliteTestSupport.DeleteDbFiles(cs);
        _databases.Add(cs);
        return Task.FromResult(cs);
    }

    protected override ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c => c.CacheDomain = $"export-gate-{Guid.NewGuid():N}");
        });
        return services.BuildServiceProvider();
    }

    protected override string NewFile()
    {
        var file = Path.Combine(Path.GetTempPath(), $"redb-export-gate-{Guid.NewGuid():N}.redb");
        _files.Add(file);
        return file;
    }

    protected override Task StageOlderAsync(string cs) => ExecuteAsync(cs, $"""
        DROP TABLE _schema_compat;
        PRAGMA user_version = {SchemaVersion - 1};
        """);

    protected override Task StageNewerAsync(string cs, bool servesThisBuild)
    {
        var minReader = servesThisBuild ? SchemaVersion : SchemaVersion + 1;
        return ExecuteAsync(cs, $"""
            DELETE FROM _schema_compat;
            INSERT INTO _schema_compat (_min_reader, _package) VALUES ({minReader}, '{(servesThisBuild ? "4.8.0" : "99.0.0")}');
            PRAGMA user_version = {SchemaVersion + 1};
            """);
    }

    protected override async Task<string?> ScalarAsync(string cs, string sql)
    {
        await using var connection = new SqliteConnection(cs);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToString(await command.ExecuteScalarAsync());
    }

    protected override async Task<List<string>> ProviderStateAsync(string cs)
    {
        var lines = new List<string>();
        await using var connection = new SqliteConnection(cs);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT 'index ' || tbl_name || '.' || name FROM sqlite_master WHERE type = 'index' ORDER BY 1";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            lines.Add(reader.GetString(0));
        return lines;
    }

    protected override Task ExecuteSqlAsync(string cs, string sql) => ExecuteAsync(cs, sql);

    private static async Task ExecuteAsync(string cs, string sql)
    {
        await using var connection = new SqliteConnection(cs);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
}
