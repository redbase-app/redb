using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProProQueryTests : ProQueryTestsBase
{
    public MsSqlProProQueryTests(MsSqlProFixture fixture) : base(fixture.Redb, isPro: true) { }
}
