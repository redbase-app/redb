using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Pro.Extensions;
using redb.MSSql.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

public class MsSqlProDbInfoTests : DbInfoTestsBase
{
    protected override void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("MSSql")!;
        options.UseMsSql(cs);
    }

    protected override string VersionMarker => "Microsoft SQL Server";

    // sys.database_files.size is in 8 KB pages.
    protected override string SizeInBytesSql => "SELECT SUM(CAST(size AS BIGINT)) * 8192 FROM sys.database_files";
}
