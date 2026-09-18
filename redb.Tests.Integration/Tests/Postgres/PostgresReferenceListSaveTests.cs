using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Postgres.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

public class PostgresReferenceListSaveTests : ReferenceListSaveTestsBase
{
    protected override void UseProvider(RedbOptionsBuilder options)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("Postgres")!;
        options.UsePostgres(cs);
    }
}
