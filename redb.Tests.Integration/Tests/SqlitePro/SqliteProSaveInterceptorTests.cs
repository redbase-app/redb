using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.SQLite.Data;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProSaveInterceptorTests : SaveInterceptorTestsBase
{
    public SqliteProSaveInterceptorTests(SqliteProFixture _) { }

    protected override string ConnectionStringName => "Sqlite";
    protected override PropsSaveStrategy Strategy => PropsSaveStrategy.ChangeTracking;
    protected override void AddRedbServices(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);
    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        options.UseSqlite(connectionString);
    }
}
