using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProWindowTests : WindowTestsBase
{
    public PostgresProWindowTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
