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
    private readonly SqliteProFixture _fixture;

    public SqliteProSaveInterceptorTests(SqliteProFixture fixture) => _fixture = fixture;

    protected override string ConnectionStringName => "Sqlite";
    protected override PropsSaveStrategy Strategy => PropsSaveStrategy.ChangeTracking;
    protected override void AddRedbServices(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);
    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
    {
        // The database of the collection's fixture, which has the schema. The "Sqlite" connection string is the
        // file of the Free collection: it had a schema only when that collection, or a previous run, made it.
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        options.UseSqlite(_fixture.ConnectionString);
    }
}
