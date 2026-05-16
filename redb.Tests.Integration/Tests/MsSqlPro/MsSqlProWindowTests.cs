using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProWindowTests : WindowTestsBase
{
    public MsSqlProWindowTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
