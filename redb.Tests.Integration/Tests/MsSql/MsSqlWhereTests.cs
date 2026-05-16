using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlWhereTests : WhereTestsBase
{
    public MsSqlWhereTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
