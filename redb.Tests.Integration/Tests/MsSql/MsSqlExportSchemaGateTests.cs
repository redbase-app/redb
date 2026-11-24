using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core.Extensions;
using redb.MSSql.Extensions;
using redb.MSSql.Sql;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary><see cref="ExportSchemaGateTestsBase"/> on SQL Server, scratch databases dropped afterwards.</summary>
public class MsSqlExportSchemaGateTests : ExportSchemaGateTestsBase, IAsyncLifetime
{
    private string _server = null!;
    private readonly List<string> _databases = [];
    private readonly List<string> _files = [];

    protected override string ProviderName => "mssql";
    protected override bool SupportsParallel => true;
    protected override string OutdatedRemedy => "`redb schema --upgrade --provider mssql`";

    public Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        _server = config.GetConnectionString("MSSql")!;
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
        var name = $"redb_export_gate_{Guid.NewGuid().ToString("N")[..12]}_{suffix}";
        _databases.Add(name);
        await ExecuteAsync(Master(), $"CREATE DATABASE [{name}]");
        return new SqlConnectionStringBuilder(_server) { InitialCatalog = name }.ConnectionString;
    }

    protected override ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options
            .UseMsSql(cs)
            .Configure(c => c.CacheDomain = $"export-gate-{Guid.NewGuid():N}"));
        return services.BuildServiceProvider();
    }

    protected override string NewFile()
    {
        var file = Path.Combine(Path.GetTempPath(), $"redb-export-gate-{Guid.NewGuid():N}.redb");
        _files.Add(file);
        return file;
    }

    protected override async Task StageOlderAsync(string cs)
    {
        await ExecuteAsync(cs, """
            ALTER FUNCTION dbo.pvt_module_version() RETURNS nvarchar(50) WITH SCHEMABINDING
            AS BEGIN RETURN N'0.0.1'; END
            """);
    }

    protected override async Task StageNewerAsync(string cs, bool servesThisBuild)
    {
        var compat = servesThisBuild ? $"{new MsSqlDialect().Query_PvtRequiredVersion()}|4.8.0" : "99.0.0|99.0.0";
        await ExecuteAsync(cs, """
            ALTER FUNCTION dbo.pvt_module_version() RETURNS nvarchar(50) WITH SCHEMABINDING
            AS BEGIN RETURN N'99.0.0'; END
            """);
        await ExecuteAsync(cs, $"""
            CREATE OR ALTER FUNCTION dbo.pvt_module_compat() RETURNS nvarchar(50) WITH SCHEMABINDING
            AS BEGIN RETURN N'{compat}'; END
            """);
    }

    protected override async Task<string?> ScalarAsync(string cs, string sql)
    {
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync());
    }

    protected override async Task<List<string>> ProviderStateAsync(string cs)
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
            """
            SELECT 'fk ' + name + ' disabled=' + CAST(is_disabled AS varchar(1)) + ' untrusted=' + CAST(is_not_trusted AS varchar(1))
            FROM sys.foreign_keys ORDER BY 1
            """,
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

    protected override Task ExecuteSqlAsync(string cs, string sql) => ExecuteAsync(cs, sql);

    private static async Task ExecuteAsync(string cs, string sql)
    {
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
