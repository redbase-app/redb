using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using redb.Core.Extensions;
using redb.Postgres.Extensions;
using redb.Postgres.Sql;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary><see cref="CliInitTestsBase"/> on PostgreSQL, scratch databases dropped afterwards.</summary>
[Collection("Console output")]
public class PostgresCliInitTests : CliInitTestsBase, IAsyncLifetime
{
    private string _server = null!;
    private readonly List<string> _databases = [];

    protected override string ProviderName => "postgres";
    protected override string RequiredVersion => new PostgreSqlDialect().Query_PvtRequiredVersion()!;

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
    }

    protected override async Task<string> NewDatabaseAsync(string suffix)
    {
        var name = $"redb_init_{Guid.NewGuid().ToString("N")[..12]}_{suffix}";
        _databases.Add(name);
        await ExecuteAsync(Maintenance(), $"CREATE DATABASE \"{name}\"");
        return new NpgsqlConnectionStringBuilder(_server) { Database = name }.ConnectionString;
    }

    protected override ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UsePostgres(cs).Configure(c => c.CacheDomain = $"init-{Guid.NewGuid():N}"));
        return services.BuildServiceProvider();
    }

    protected override async Task<string?> ScalarAsync(string cs, string sql)
    {
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync());
    }

    protected override async Task<List<string>> CatalogAsync(string cs)
    {
        var lines = new List<string>();
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        foreach (var sql in new[]
        {
            "SELECT 'sequence ' || sequencename FROM pg_sequences WHERE schemaname = current_schema() ORDER BY 1",
            "SELECT 'fk ' || conrelid::regclass || '.' || conname FROM pg_constraint WHERE contype = 'f' ORDER BY 1",
            "SELECT 'trigger ' || tgrelid::regclass || '.' || tgname || ' ' || tgenabled::text FROM pg_trigger ORDER BY 1",
            "SELECT 'module ' || pvt_module_version()",
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
