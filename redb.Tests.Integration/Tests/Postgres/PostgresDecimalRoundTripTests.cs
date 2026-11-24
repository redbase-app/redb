using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresDecimalRoundTripTests : DecimalRoundTripTestsBase
{
    public PostgresDecimalRoundTripTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
