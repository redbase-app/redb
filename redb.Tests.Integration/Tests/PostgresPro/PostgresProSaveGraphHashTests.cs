using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProSaveGraphHashTests : SaveGraphHashTestsBase
{
    public PostgresProSaveGraphHashTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
