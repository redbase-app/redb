using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProWideEqualityLeadsTests : WideEqualityLeadsTestsBase
{
    public MsSqlProWideEqualityLeadsTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
