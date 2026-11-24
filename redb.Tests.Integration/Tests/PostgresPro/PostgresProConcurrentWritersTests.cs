using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProConcurrentWritersTests : ConcurrentWritersTestsBase
{
    public PostgresProConcurrentWritersTests(PostgresProFixture fixture) : base(fixture.ServiceProvider) { }
}