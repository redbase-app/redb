using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProProjectionTests : ProjectionTestsBase
{
    public PostgresProProjectionTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
