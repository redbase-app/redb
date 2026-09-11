using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

public class SqliteSchemaMissingTests : SchemaMissingTestsBase
{
    private const string Cs = "Data Source=redb_tests_schemamissing.db";

    protected override Task<string> CreateEmptyDatabaseAsync()
    {
        // A fresh file IS the empty database: the first connection creates it without any table.
        redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= Fixtures.SqliteTestSupport.ResolveNativeExtension();
        Fixtures.SqliteTestSupport.DeleteDbFiles(Cs);
        return Task.FromResult(Cs);
    }

    protected override Task DropDatabaseAsync(string connectionString)
    {
        Fixtures.SqliteTestSupport.DeleteDbFiles(connectionString);
        return Task.CompletedTask;
    }

    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UseSqlite(connectionString);
}
