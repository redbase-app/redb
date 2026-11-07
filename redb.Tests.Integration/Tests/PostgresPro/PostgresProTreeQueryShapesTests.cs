using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProTreeQueryShapesTests : TreeQueryShapesTestsBase
{
    public PostgresProTreeQueryShapesTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
