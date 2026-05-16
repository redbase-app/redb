using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProOrderingTests : OrderingTestsBase
{
    public PostgresProOrderingTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
