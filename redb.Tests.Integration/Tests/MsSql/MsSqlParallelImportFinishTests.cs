using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core.Extensions;
using redb.MSSql.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary><see cref="ParallelImportFinishTestsBase"/> on SQL Server, scratch databases dropped afterwards.</summary>
public class MsSqlParallelImportFinishTests : ParallelImportFinishTestsBase, IAsyncLifetime
{
    private string _server = null!;
    private readonly List<string> _databases = [];
    private readonly List<string> _files = [];

    protected override string ProviderName => "mssql";

    public Task InitializeAsync()
    {
        _server = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("MSSql")!;
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        SqlConnection.ClearAllPools();
        foreach (var database in _databases)
            await ExecuteAsync(Master(),
                $"IF DB_ID('{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}] END");
        foreach (var file in _files)
            File.Delete(file);
    }

    protected override async Task<string> NewDatabaseAsync(string suffix)
    {
        var name = $"redb_export_fin_{Guid.NewGuid().ToString("N")[..12]}_{suffix}";
        _databases.Add(name);
        await ExecuteAsync(Master(), $"CREATE DATABASE [{name}]");
        return new SqlConnectionStringBuilder(_server) { InitialCatalog = name }.ConnectionString;
    }

    protected override ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UseMsSql(cs).Configure(c => c.CacheDomain = $"fin-{Guid.NewGuid():N}"));
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
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync());
    }

    protected override Task DropSequenceAsync(string cs) => ExecuteAsync(cs, "DROP SEQUENCE [dbo].[global_identity]");

    protected override async Task<List<string>> ShapeAsync(string cs)
    {
        var lines = new List<string>();
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        foreach (var sql in new[]
        {
            """
            SELECT 'index ' + t.name + '.' + i.name + ' disabled=' + CAST(i.is_disabled AS varchar(1))
            FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id
            WHERE i.name IS NOT NULL ORDER BY 1
            """,
            // On the 4.7 line the service's bulk save leaves the keys of _objects and _values untrusted, and the import's
            // WITH CHECK makes them trusted: an improvement, not a change to hide. NOCHECK left behind shows as disabled.
            "SELECT 'fk ' + name + ' disabled=' + CAST(is_disabled AS varchar(1)) FROM sys.foreign_keys ORDER BY 1",
            "SELECT 'trigger ' + name + ' disabled=' + CAST(is_disabled AS varchar(1)) FROM sys.triggers ORDER BY 1",
        })
        {
            await using var command = new SqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                lines.Add(reader.GetString(0));
        }
        return lines;
    }

    private string Master() => new SqlConnectionStringBuilder(_server) { InitialCatalog = "master" }.ConnectionString;

    private static async Task ExecuteAsync(string cs, string sql)
    {
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
