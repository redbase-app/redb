using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProTemporalRoundTripTests : TemporalRoundTripTestsBase
{
    public PostgresProTemporalRoundTripTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
