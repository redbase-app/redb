using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

public class SqliteProListItemObjectWalkTests : ListItemObjectWalkTestsBase
{
    protected override void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        // A fresh file per host; Pro SQLite runs without the Free native extension.
        const string cs = "Data Source=redb_tests_listitem_walk_pro.db";
        Fixtures.SqliteTestSupport.DeleteDbFiles(cs);
        options.UseSqlite(cs);
    }
}
