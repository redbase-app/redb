using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core.Extensions;
using redb.Postgres.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary><see cref="ParallelImportFinishTestsBase"/> on PostgreSQL, scratch databases dropped afterwards.</summary>
public class PostgresParallelImportFinishTests : ParallelImportFinishTestsBase, IAsyncLifetime
{
    private string _server = null!;
    private readonly List<string> _databases = [];
    private readonly List<string> _files = [];

    protected override string ProviderName => "postgres";

    public Task InitializeAsync()
    {
        _server = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("Postgres")!;
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        NpgsqlConnection.ClearAllPools();
        foreach (var database in _databases)
            await ExecuteAsync(Maintenance(), $"DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)");
        foreach (var file in _files)
            File.Delete(file);
    }

    protected override async Task<string> NewDatabaseAsync(string suffix)
    {
        var name = $"redb_export_fin_{Guid.NewGuid().ToString("N")[..12]}_{suffix}";
        _databases.Add(name);
        await ExecuteAsync(Maintenance(), $"CREATE DATABASE \"{name}\"");
        return new NpgsqlConnectionStringBuilder(_server) { Database = name }.ConnectionString;
    }

    protected override ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UsePostgres(cs).Configure(c => c.CacheDomain = $"fin-{Guid.NewGuid():N}"));
        return services.BuildServiceProvider();
    }

    protected override string NewFile()
    {
        var file = Path.Combine(Path.GetTempPath(), $"redb-export-fin-{Guid.NewGuid():N}.redb");
        _files.Add(file);
        return file;
    }

    protected override async Task<string?> ScalarAsync(string cs, string sql)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync());
    }

    protected override Task DropSequenceAsync(string cs) => ExecuteAsync(cs, "DROP SEQUENCE global_identity CASCADE");

    protected override async Task<List<string>> ShapeAsync(string cs)
    {
        var lines = new List<string>();
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        foreach (var sql in new[]
        {
            "SELECT 'index ' || tablename || '.' || indexname FROM pg_indexes WHERE schemaname = current_schema() ORDER BY 1",
            "SELECT 'trigger ' || tgrelid::regclass || '.' || tgname || ' ' || tgenabled::text FROM pg_trigger ORDER BY 1",
            "SELECT 'dropped-indexes-table ' || (to_regclass('_import_dropped_indexes') IS NOT NULL)",
        })
        {
            await using var command = new NpgsqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                lines.Add(reader.GetString(0));
        }
        return lines;
    }

    private string Maintenance() => new NpgsqlConnectionStringBuilder(_server) { Database = "postgres" }.ConnectionString;

    private static async Task ExecuteAsync(string cs, string sql)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
