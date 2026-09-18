using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.MSSql.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

public class MsSqlReaderConnectionTests : ReaderConnectionTestsBase
{
    protected override string SlowScalarSql => "WAITFOR DELAY '00:00:02'; SELECT CAST(1 AS bigint)";

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("MSSql")!;
        options.UseMsSql(cs);
    }
}
