using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core.Extensions;
using redb.Postgres.Extensions;
using redb.Postgres.Sql;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary><see cref="ExportSchemaGateTestsBase"/> on PostgreSQL, scratch databases dropped afterwards.</summary>
public class PostgresExportSchemaGateTests : ExportSchemaGateTestsBase, IAsyncLifetime
{
    private string _server = null!;
    private readonly List<string> _databases = [];
    private readonly List<string> _files = [];

    protected override string ProviderName => "postgres";
    protected override bool SupportsParallel => true;
    protected override string OutdatedRemedy => "`redb schema --upgrade --provider postgres`";

    public Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        _server = config.GetConnectionString("Postgres")!;
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
        var name = $"redb_export_gate_{Guid.NewGuid().ToString("N")[..12]}_{suffix}";
        _databases.Add(name);
        await ExecuteAsync(Maintenance(), $"CREATE DATABASE \"{name}\"");
        return new NpgsqlConnectionStringBuilder(_server) { Database = name }.ConnectionString;
    }

    protected override ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options
            .UsePostgres(cs)
            .Configure(c => c.CacheDomain = $"export-gate-{Guid.NewGuid():N}"));
        return services.BuildServiceProvider();
    }

    protected override string NewFile()
    {
        var file = Path.Combine(Path.GetTempPath(), $"redb-export-gate-{Guid.NewGuid():N}.redb");
        _files.Add(file);
        return file;
    }

    protected override Task StageOlderAsync(string cs) => ExecuteAsync(cs, """
        CREATE OR REPLACE FUNCTION pvt_module_version() RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN '0.0.1'; END $$;
        """);

    protected override Task StageNewerAsync(string cs, bool servesThisBuild)
    {
        var compat = servesThisBuild ? $"{new PostgreSqlDialect().Query_PvtRequiredVersion()}|4.8.0" : "99.0.0|99.0.0";
        return ExecuteAsync(cs, $"""
            CREATE OR REPLACE FUNCTION pvt_module_version() RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN '99.0.0'; END $$;
            CREATE OR REPLACE FUNCTION pvt_module_compat() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT '{compat}'::text $$;
            """);
    }

    protected override async Task<string?> ScalarAsync(string cs, string sql)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync());
    }

    protected override async Task<List<string>> ProviderStateAsync(string cs)
    {
        var lines = new List<string>();
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        foreach (var sql in new[]
        {
            "SELECT 'index ' || tablename || '.' || indexname FROM pg_indexes WHERE schemaname = current_schema() ORDER BY 1",
            "SELECT 'fk ' || conrelid::regclass || '.' || conname FROM pg_constraint WHERE contype = 'f' ORDER BY 1",
            // The foreign keys are enforced by internal triggers: a load that switched them off leaves them disabled.
            "SELECT 'trigger ' || tgrelid::regclass || '.' || tgname || ' ' || tgenabled::text FROM pg_trigger WHERE tgisinternal ORDER BY 1",
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

    protected override Task ExecuteSqlAsync(string cs, string sql) => ExecuteAsync(cs, sql);

    private static async Task ExecuteAsync(string cs, string sql)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
