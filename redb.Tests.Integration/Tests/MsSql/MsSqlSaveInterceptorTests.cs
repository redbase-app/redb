using redb.Core.Extensions;
using redb.MSSql.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlSaveInterceptorTests : SaveInterceptorTestsBase
{
    protected override string ConnectionStringName => "MSSql";
    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UseMsSql(connectionString);
}
