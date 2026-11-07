using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.MSSql.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

public class MsSqlDbInfoTests : DbInfoTestsBase
{
    protected override void UseProvider(RedbOptionsBuilder options)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("MSSql")!;
        options.UseMsSql(cs);
    }

    protected override string VersionMarker => "Microsoft SQL Server";

    // sys.database_files.size is in 8 KB pages.
    protected override string SizeInBytesSql => "SELECT SUM(CAST(size AS BIGINT)) * 8192 FROM sys.database_files";
}
