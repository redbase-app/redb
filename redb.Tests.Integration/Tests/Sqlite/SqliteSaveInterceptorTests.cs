using redb.Core.Extensions;
using redb.SQLite.Data;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteSaveInterceptorTests : SaveInterceptorTestsBase
{
    // The collection guarantees the shared db file exists and the native extension is resolved
    // before this suite opens its own connections to the same file.
    public SqliteSaveInterceptorTests(SqliteFixture _) { }

    protected override string ConnectionStringName => "Sqlite";
    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        options.UseSqlite(connectionString);
    }
}
