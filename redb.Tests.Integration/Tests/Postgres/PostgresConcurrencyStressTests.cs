using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresConcurrencyStressTests : ConcurrencyStressTestsBase
{
    public PostgresConcurrencyStressTests(PostgresFixture fixture)
        : base(fixture.Redb, fixture.ServiceProvider) { }
}
