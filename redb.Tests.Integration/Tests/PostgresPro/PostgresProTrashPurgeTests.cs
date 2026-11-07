using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProTrashPurgeTests : TrashPurgeTestsBase
{
    public PostgresProTrashPurgeTests(PostgresProFixture fixture) : base(fixture.ServiceProvider) { }
}
