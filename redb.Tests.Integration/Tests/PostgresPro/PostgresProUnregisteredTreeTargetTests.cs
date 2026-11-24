using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProUnregisteredTreeTargetTests : UnregisteredTreeTargetTestsBase
{
    public PostgresProUnregisteredTreeTargetTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
