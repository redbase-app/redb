using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlTreeTests : TreeTestsBase
{
    public MsSqlTreeTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
