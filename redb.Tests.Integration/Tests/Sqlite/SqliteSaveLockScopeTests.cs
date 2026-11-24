using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

public class SqliteSaveLockScopeTests : SaveLockScopeTestsBase
{
    private const string Cs = "Data Source=redb_tests_savelock_free.db";
    private bool _prepared;

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= Fixtures.SqliteTestSupport.ResolveNativeExtension();
        // A fresh file per test class instance.
        if (!_prepared)
        {
            Fixtures.SqliteTestSupport.DeleteDbFiles(Cs);
            _prepared = true;
        }
        options.UseSqlite(Cs);
    }
}
