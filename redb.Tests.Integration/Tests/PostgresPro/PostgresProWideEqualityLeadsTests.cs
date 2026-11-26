using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProWideEqualityLeadsTests : WideEqualityLeadsTestsBase
{
    public PostgresProWideEqualityLeadsTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
