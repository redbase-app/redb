using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProProQueryTests : ProQueryTestsBase
{
    public PostgresProProQueryTests(PostgresProFixture fixture) : base(fixture.Redb, isPro: true) { }
}
