using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core.Extensions;
using redb.MSSql.Extensions;
using redb.MSSql.Sql;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary><see cref="CliInitTestsBase"/> on SQL Server, scratch databases dropped afterwards.</summary>
[Collection("Console output")]
public class MsSqlCliInitTests : CliInitTestsBase, IAsyncLifetime
{
    private string _server = null!;
    private readonly List<string> _databases = [];

    protected override string ProviderName => "mssql";
    protected override string RequiredVersion => new MsSqlDialect().Query_PvtRequiredVersion()!;

    /// <summary>
    /// The id sequence of an imported database, restarted past every id it holds. Run after importing an export of a
    /// database that had lost its sequence: such an export carries none, and the import leaves the new sequence at its
    /// start, below the imported ids. The same statement as in UPGRADING.md.
    /// </summary>
    private const string RestartSequenceSql = """
        DECLARE @sql nvarchar(max) = (
            SELECT N'SELECT @max = MAX(id) FROM (' + STRING_AGG(CAST(N'SELECT MAX(_id) AS id FROM dbo.' + QUOTENAME(t.name) AS nvarchar(max)), N' UNION ALL ') + N') x'
            FROM sys.tables t JOIN sys.columns c ON c.object_id = t.object_id AND c.name = N'_id'
            WHERE t.schema_id = SCHEMA_ID(N'dbo'));
        DECLARE @max bigint;
        EXEC sp_executesql @sql, N'@max bigint OUTPUT', @max = @max OUTPUT;
        DECLARE @restart nvarchar(200) = N'ALTER SEQUENCE dbo.global_identity RESTART WITH '
            + CAST(CASE WHEN @max >= 1000000 THEN @max + 1 ELSE 1000000 END AS nvarchar(20));
        EXEC (@restart);
        """;

    [Fact]
    public async Task ADatabaseHitByASecondInit_IsRepaired_ByExport_OneInit_Import_AndTheSequenceRestart()
    {
        // The state a second init left: no foreign key, no sequence, the data in place.
        var damaged = await NewDatabaseAsync("hit");
        (await InitAsync(damaged)).Code.Should().Be(0);
        await using (var provider = Build(damaged))
        {
            var svc = provider.GetRequiredService<global::redb.Core.IRedbService>();
            await svc.InitializeAsync();
            await svc.SyncSchemeAsync<global::redb.Tests.Integration.Models.SimpleProps>();
            for (var i = 0; i < 3; i++)
                await svc.SaveAsync(new global::redb.Core.Models.Entities.RedbObject<global::redb.Tests.Integration.Models.SimpleProps>
                    { name = $"hit-{i}", Props = new global::redb.Tests.Integration.Models.SimpleProps { Title = $"t{i}" } });
        }
        var foreignKeys = await ScalarAsync(damaged, "SELECT COUNT(*) FROM sys.foreign_keys");
        await ExecuteAsync(damaged, """
            DECLARE @drop nvarchar(max) = N'';
            SELECT @drop += N'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + N'.' + QUOTENAME(OBJECT_NAME(parent_object_id))
                + N' DROP CONSTRAINT ' + QUOTENAME(name) + N';' FROM sys.foreign_keys;
            EXEC sp_executesql @drop;
            DROP SEQUENCE dbo.global_identity;
            """);

        var file = Path.Combine(Path.GetTempPath(), $"redb-cli-repair-{Guid.NewGuid():N}.redb");
        try
        {
            await using (var provider = global::redb.Export.Providers.ProviderFactory.Create("mssql"))
            {
                await provider.OpenAsync(damaged);
                await new global::redb.Export.Services.ExportService(provider, false, 1000).ExportAsync(file, null, false, false);
            }

            var repaired = await NewDatabaseAsync("repaired");
            (await InitAsync(repaired)).Code.Should().Be(0);
            await using (var provider = global::redb.Export.Providers.ProviderFactory.Create("mssql"))
            {
                await provider.OpenAsync(repaired);
                await new global::redb.Export.Services.ImportService(provider, false, 1000).ImportAsync(file, true, false);
            }
            await ExecuteAsync(repaired, RestartSequenceSql);

            (await ScalarAsync(repaired, "SELECT COUNT(*) FROM sys.sequences WHERE name = 'global_identity'")).Should().Be("1");
            (await ScalarAsync(repaired, "SELECT COUNT(*) FROM sys.foreign_keys")).Should().Be(foreignKeys);
            (await ScalarAsync(repaired, "SELECT COUNT(*) FROM _objects WHERE _name LIKE 'hit-%'")).Should().Be("3");

            global::redb.Core.Data.RedbKeyGeneratorBase.ClearAllCaches();
            await using var service = Build(repaired);
            var redb2 = service.GetRequiredService<global::redb.Core.IRedbService>();
            await redb2.InitializeAsync();
            var id = await redb2.SaveAsync(new global::redb.Core.Models.Entities.RedbObject<global::redb.Tests.Integration.Models.SimpleProps>
                { name = "after-repair", Props = new global::redb.Tests.Integration.Models.SimpleProps { Title = "new" } });
            id.Should().BeGreaterThan(long.Parse((await ScalarAsync(repaired, "SELECT MAX(_id) FROM _objects WHERE _name LIKE 'hit-%'"))!),
                "the restarted sequence hands out ids past the imported ones");
        }
        finally
        {
            File.Delete(file);
        }
    }

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
    }

    protected override async Task<string> NewDatabaseAsync(string suffix)
    {
        var name = $"redb_init_{Guid.NewGuid().ToString("N")[..12]}_{suffix}";
        _databases.Add(name);
        await ExecuteAsync(Master(), $"CREATE DATABASE [{name}]");
        return new SqlConnectionStringBuilder(_server) { InitialCatalog = name }.ConnectionString;
    }

    protected override ServiceProvider Build(string cs)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options => options.UseMsSql(cs).Configure(c => c.CacheDomain = $"init-{Guid.NewGuid():N}"));
        return services.BuildServiceProvider();
    }

    protected override async Task<string?> ScalarAsync(string cs, string sql)
    {
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync());
    }

    protected override async Task<List<string>> CatalogAsync(string cs)
    {
        var lines = new List<string>();
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        foreach (var sql in new[]
        {
            "SELECT 'sequence ' + name FROM sys.sequences ORDER BY 1",
            "SELECT 'fk ' + name + ' disabled=' + CAST(is_disabled AS varchar(1)) FROM sys.foreign_keys ORDER BY 1",
            "SELECT 'trigger ' + name + ' disabled=' + CAST(is_disabled AS varchar(1)) FROM sys.triggers ORDER BY 1",
            "SELECT 'module ' + dbo.pvt_module_version()",
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
