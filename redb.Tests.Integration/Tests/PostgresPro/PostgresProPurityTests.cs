using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProPurityTests : ProPgPurityTestsBase
{
    public PostgresProPurityTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
