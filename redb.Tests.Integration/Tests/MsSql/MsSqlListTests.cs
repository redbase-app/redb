using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlListTests : ListTestsBase
{
    public MsSqlListTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
