using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

public class SqliteTrashBackgroundServiceTests : TrashBackgroundServiceTestsBase
{
    private const string Cs = "Data Source=redb_tests_trashbg.db";

    protected override Task<string> CreateEmptyDatabaseAsync()
    {
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
