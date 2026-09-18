using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

public class SqliteDbInfoTests : DbInfoTestsBase
{
    protected override void UseProvider(RedbOptionsBuilder options)
    {
        redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= Fixtures.SqliteTestSupport.ResolveNativeExtension();
        const string cs = "Data Source=redb_tests_dbinfo_free.db";
        Fixtures.SqliteTestSupport.DeleteDbFiles(cs);
        options.UseSqlite(cs);
    }

    protected override string VersionMarker => "3.";

    protected override string SizeInBytesSql => "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()";
}
