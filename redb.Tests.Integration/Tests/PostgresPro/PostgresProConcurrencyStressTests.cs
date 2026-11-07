using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProConcurrencyStressTests : ConcurrencyStressTestsBase
{
    public PostgresProConcurrencyStressTests(PostgresProFixture fixture)
        : base(fixture.Redb, fixture.ServiceProvider) { }
}
