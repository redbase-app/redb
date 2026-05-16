using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProListTests : ListTestsBase
{
    protected override bool IsPro => true;
    public MsSqlProListTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
