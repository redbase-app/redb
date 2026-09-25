using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using redb.Export.Providers;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// redb.Export on SQL Server restores the id sequence so that the next id is new. The exporter reads the last id
/// handed out (<c>current_value</c>), and <c>RESTART WITH</c> names the NEXT value, so restarting at the value read
/// handed out an id that an imported row already had (review 2026-09-24). Runs on a scratch database of its own:
/// rewinding the shared test database's sequence would break every other test.
/// </summary>
public class MsSqlExportSequenceTests : IAsyncLifetime
{
    private string _server = null!;
    private string _database = null!;

    public async Task InitializeAsync()
    {
        var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        _server = config.GetConnectionString("MSSql")!;
        _database = "redb_export_seq_" + Guid.NewGuid().ToString("N")[..12];
        await ExecuteAsync(Master(), $"CREATE DATABASE [{_database}]");
        await ExecuteAsync(Scratch(), "CREATE SEQUENCE global_identity AS BIGINT START WITH 1000 INCREMENT BY 1");
    }

    public async Task DisposeAsync()
    {
        SqlConnection.ClearAllPools();
        await ExecuteAsync(Master(), $"ALTER DATABASE [{_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{_database}]");
    }

    [Fact]
    public async Task AfterAnImport_TheNextIdIsNotOneAlreadyHandedOut()
    {
        // Three ids handed out: 1000, 1001, 1002 - the last one belongs to some exported row.
        for (var i = 0; i < 3; i++)
            await ScalarAsync(Scratch(), "SELECT NEXT VALUE FOR global_identity");

        await using var provider = ProviderFactory.Create("mssql");
        await provider.OpenAsync(Scratch());
        var lastHandedOut = await provider.GetSequenceValueAsync();
        lastHandedOut.Should().Be(1002, "precondition: the exporter reads the last id handed out");

        // What an import does with the value it carried over.
        await provider.BeginImportAsync();
        await provider.SetSequenceValueAsync(lastHandedOut);
        await provider.CommitImportAsync();

        (await ScalarAsync(Scratch(), "SELECT NEXT VALUE FOR global_identity")).Should().Be(1003,
            "the next object saved after the import must not take the id of the last imported row");
    }

    private string Master() => new SqlConnectionStringBuilder(_server) { InitialCatalog = "master" }.ConnectionString;

    private string Scratch() => new SqlConnectionStringBuilder(_server) { InitialCatalog = _database }.ConnectionString;

    private static async Task ExecuteAsync(string cs, string sql)
    {
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> ScalarAsync(string cs, string sql)
    {
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }
}
