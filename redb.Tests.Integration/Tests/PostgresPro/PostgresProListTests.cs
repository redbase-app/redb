using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProListTests : ListTestsBase
{
    protected override bool IsPro => true;
    public PostgresProListTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
