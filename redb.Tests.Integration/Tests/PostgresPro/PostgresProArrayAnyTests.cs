using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProArrayAnyTests : ArrayAnyTestsBase
{
    public PostgresProArrayAnyTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
