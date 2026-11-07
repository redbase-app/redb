using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlProQueryTests : ProQueryTestsBase
{
    public MsSqlProQueryTests(MsSqlFixture fixture) : base(fixture.Redb, isPro: false) { }
}
