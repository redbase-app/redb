using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Extensions;
using redb.MSSql.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

public class MsSqlReferenceListSaveTests : ReferenceListSaveTestsBase
{
    protected override void UseProvider(RedbOptionsBuilder options)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("MSSql")!;
        options.UseMsSql(cs);
    }
}
