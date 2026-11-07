using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProTreeTests : ProTreeTestsBase
{
    public PostgresProTreeTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
