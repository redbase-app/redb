using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProPvtPrefilterEquivalenceTests : PvtPrefilterEquivalenceTestsBase
{
    public PostgresProPvtPrefilterEquivalenceTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
