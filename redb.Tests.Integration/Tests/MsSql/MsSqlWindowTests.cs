using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlWindowTests : WindowTestsBase
{
    public MsSqlWindowTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
