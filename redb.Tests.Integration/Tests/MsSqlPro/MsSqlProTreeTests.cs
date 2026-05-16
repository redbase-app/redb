using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProTreeTests : TreeTestsBase
{
    public MsSqlProTreeTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
