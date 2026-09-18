using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlConcurrencyStressTests : ConcurrencyStressTestsBase
{
    public MsSqlConcurrencyStressTests(MsSqlFixture fixture)
        : base(fixture.Redb, fixture.ServiceProvider) { }
}
