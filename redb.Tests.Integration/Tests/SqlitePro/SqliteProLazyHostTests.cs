using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

public class SqliteProLazyHostTests : LazyHostTestsBase
{
    protected override void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        const string cs = "Data Source=redb_tests_lazyhost_pro.db";
        Fixtures.SqliteTestSupport.DeleteDbFiles(cs);
        options.UseSqlite(cs);
    }

    protected override string ReadFlagSql => "SELECT redb_lazy_refs()";

    /// <summary>The Pro materializer reads the global option live; its registrations do not arm the session flag.</summary>
    protected override bool SessionFlagIsTheChannel => false;
}
