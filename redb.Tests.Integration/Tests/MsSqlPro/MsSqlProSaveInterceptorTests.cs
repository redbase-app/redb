using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.MSSql.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProSaveInterceptorTests : SaveInterceptorTestsBase
{
    protected override string ConnectionStringName => "MSSql";
    protected override PropsSaveStrategy Strategy => PropsSaveStrategy.ChangeTracking;
    protected override void AddRedbServices(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedbPro(configure);
    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UseMsSql(connectionString);
}
