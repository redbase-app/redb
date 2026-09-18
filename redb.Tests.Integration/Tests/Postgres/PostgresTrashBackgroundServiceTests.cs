using Npgsql;
using redb.Core.Extensions;
using redb.Postgres.Data;
using redb.Postgres.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

public class PostgresTrashBackgroundServiceTests : TrashBackgroundServiceTestsBase
{
    private readonly string _name = $"redb_probe_trashbg_{Guid.NewGuid():N}"[..40];

    protected override async Task<string> CreateEmptyDatabaseAsync()
    {
        await using var admin = new NpgsqlRedbConnection(ConnString("Postgres"));
        await admin.ExecuteAsync($"CREATE DATABASE \"{_name}\"");
        return new NpgsqlConnectionStringBuilder(ConnString("Postgres")) { Database = _name }.ConnectionString;
    }

    protected override async Task DropDatabaseAsync(string connectionString)
    {
        NpgsqlConnection.ClearPool(new NpgsqlConnection(connectionString));
        await using var admin = new NpgsqlRedbConnection(ConnString("Postgres"));
        await admin.ExecuteAsync($"DROP DATABASE IF EXISTS \"{_name}\" WITH (FORCE)");
    }

    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UsePostgres(connectionString);
}
