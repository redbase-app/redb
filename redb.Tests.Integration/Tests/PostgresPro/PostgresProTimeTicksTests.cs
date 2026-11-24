using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProTimeTicksTests : TimeTicksTestsBase
{
    public PostgresProTimeTicksTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
