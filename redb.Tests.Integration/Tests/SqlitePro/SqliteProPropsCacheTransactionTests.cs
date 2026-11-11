using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

public class SqliteProPropsCacheTransactionTests : PropsCacheTransactionTestsBase
{
    private const string Cs = "Data Source=redb_tests_cachetx_pro.db";
    private bool _prepared;

    protected override void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        // A fresh file per test, shared by every provider the test builds (its own and the seeder's). Deleted per
        // provider, the file went away under the open connection of the first one wherever an open file can be
        // deleted (Linux), and the seeder wrote into a new file the test's transaction never saw.
        if (!_prepared)
        {
            Fixtures.SqliteTestSupport.DeleteDbFiles(Cs);
            _prepared = true;
        }
        options.UseSqlite(Cs);
    }
}
