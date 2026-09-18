using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.Postgres.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

public class PostgresProSubSecondUpdateTests : SubSecondUpdateTestsBase
{
    protected override void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);

    protected override PropsSaveStrategy SaveStrategy => PropsSaveStrategy.ChangeTracking;

    protected override void UseProvider(RedbOptionsBuilder options)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("Postgres")!;
        options.UsePostgres(cs);
    }
}
