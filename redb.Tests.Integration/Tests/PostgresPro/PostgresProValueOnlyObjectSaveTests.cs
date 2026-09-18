using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProValueOnlyObjectSaveTests : ValueOnlyObjectSaveTestsBase
{
    public PostgresProValueOnlyObjectSaveTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
