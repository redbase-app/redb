using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Pro.Extensions;
using redb.MSSql.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

public class MsSqlProLazyHostTests : LazyHostTestsBase
{
    protected override void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("MSSql")!;
        options.UseMsSql(cs);
    }

    protected override string ReadFlagSql => "SELECT CAST(SESSION_CONTEXT(N'redb.lazy_refs') AS NVARCHAR(10))";

    /// <summary>The Pro materializer reads the global option live; its registrations do not arm the session flag.</summary>
    protected override bool SessionFlagIsTheChannel => false;
}
