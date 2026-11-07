using Microsoft.Data.SqlClient;
using redb.Core.Extensions;
using redb.MSSql.Data;
using redb.MSSql.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

public class MsSqlSchemaMissingTests : SchemaMissingTestsBase
{
    private readonly string _name = $"redb_probe_empty_{Guid.NewGuid():N}"[..40];

    protected override async Task<string> CreateEmptyDatabaseAsync()
    {
        await using var admin = new SqlRedbConnection(ConnString("MSSql"));
        await admin.ExecuteAsync($"CREATE DATABASE [{_name}]");
        return new SqlConnectionStringBuilder(ConnString("MSSql")) { InitialCatalog = _name }.ConnectionString;
    }

    protected override async Task DropDatabaseAsync(string connectionString)
    {
        SqlConnection.ClearPool(new SqlConnection(connectionString));
        await using var admin = new SqlRedbConnection(ConnString("MSSql"));
        await admin.ExecuteAsync(
            $"IF DB_ID(N'{_name}') IS NOT NULL BEGIN " +
            $"ALTER DATABASE [{_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{_name}]; END");
    }

    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UseMsSql(connectionString);
}
