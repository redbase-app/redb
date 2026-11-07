using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProLikeEscapingTests : LikeEscapingTestsBase
{
    public PostgresProLikeEscapingTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
