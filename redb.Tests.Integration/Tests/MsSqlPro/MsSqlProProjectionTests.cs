using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProProjectionTests : ProjectionTestsBase
{
    public MsSqlProProjectionTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
