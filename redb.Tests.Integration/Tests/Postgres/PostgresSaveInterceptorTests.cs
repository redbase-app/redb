using redb.Core.Extensions;
using redb.Postgres.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresSaveInterceptorTests : SaveInterceptorTestsBase
{
    protected override string ConnectionStringName => "Postgres";
    protected override void UseProvider(RedbOptionsBuilder options, string connectionString)
        => options.UsePostgres(connectionString);
}
