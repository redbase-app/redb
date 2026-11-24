using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresConcurrentWritersTests : ConcurrentWritersTestsBase
{
    public PostgresConcurrentWritersTests(PostgresFixture fixture) : base(fixture.ServiceProvider) { }
}