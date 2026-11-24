using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.SQLite.Data;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

public class SqliteSchemaNewerTests : SqliteSchemaNewerTestsBase
{
    protected override void AddRedbServices(IServiceCollection services, string connectionString)
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        services.AddRedb(options => options.UseSqlite(connectionString));
    }
}
