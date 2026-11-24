using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

public class SqliteDeleteSubtreeCountTests : DeleteSubtreeCountTestsBase
{
    private const string Cs = "Data Source=redb_tests_subc_free.db";
    private bool _prepared;

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= Fixtures.SqliteTestSupport.ResolveNativeExtension();
        // A fresh file per test, shared by every provider the test builds (its own and the writer's). Deleted per
        // provider, the file went away under the open connection of the first one wherever an open file can be
        // deleted (Linux), and the writer wrote into a new file the reader never saw.
        if (!_prepared)
        {
            Fixtures.SqliteTestSupport.DeleteDbFiles(Cs);
            _prepared = true;
        }
        options.UseSqlite(Cs);
    }
}
