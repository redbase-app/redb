using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

public class SqliteProSubSecondUpdateTests : SubSecondUpdateTestsBase
{
    protected override void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    protected override PropsSaveStrategy SaveStrategy => PropsSaveStrategy.ChangeTracking;

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        const string cs = "Data Source=redb_tests_subsecond_pro.db";
        Fixtures.SqliteTestSupport.DeleteDbFiles(cs);
        options.UseSqlite(cs);
    }
}
