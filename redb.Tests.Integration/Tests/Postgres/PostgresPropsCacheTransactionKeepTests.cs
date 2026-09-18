using Microsoft.Extensions.Configuration;
using redb.Core.Extensions;
using redb.Postgres.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

public class PostgresPropsCacheTransactionKeepTests : PropsCacheTransactionKeepTestsBase
{
    protected override void UseProvider(RedbOptionsBuilder options)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("Postgres")!;
        options.UsePostgres(cs);
    }
}
