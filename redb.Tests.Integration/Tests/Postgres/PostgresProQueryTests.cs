using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresProQueryTests : ProQueryTestsBase
{
    public PostgresProQueryTests(PostgresFixture fixture) : base(fixture.Redb, isPro: false) { }
}
