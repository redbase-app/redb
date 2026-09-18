using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

public class SqliteProSyncLoadTests : ProSyncLoadTestsBase
{
    protected override void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        // A fresh file per host. The verdict does not depend on the Free native extension another suite may have
        // loaded process-wide: the detector is the recorded SQL.
        const string cs = "Data Source=redb_tests_prosync_pro.db";
        Fixtures.SqliteTestSupport.DeleteDbFiles(cs);
        options.UseSqlite(cs);
    }
}
