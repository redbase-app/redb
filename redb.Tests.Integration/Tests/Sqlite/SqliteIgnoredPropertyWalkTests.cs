using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

public class SqliteIgnoredPropertyWalkTests : IgnoredPropertyWalkTestsBase
{
    protected override void UseProvider(RedbOptionsBuilder options)
    {
        redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= Fixtures.SqliteTestSupport.ResolveNativeExtension();
        // A fresh file per host.
        const string cs = "Data Source=redb_tests_ignored_walk_free.db";
        Fixtures.SqliteTestSupport.DeleteDbFiles(cs);
        options.UseSqlite(cs);
    }
}
