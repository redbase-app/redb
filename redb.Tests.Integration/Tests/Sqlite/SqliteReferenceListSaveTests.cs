using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

public class SqliteReferenceListSaveTests : ReferenceListSaveTestsBase
{
    protected override void UseProvider(RedbOptionsBuilder options)
    {
        redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= Fixtures.SqliteTestSupport.ResolveNativeExtension();
        const string cs = "Data Source=redb_tests_reflist_free.db";
        Fixtures.SqliteTestSupport.DeleteDbFiles(cs);
        options.UseSqlite(cs);
    }
}
