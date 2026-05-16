using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProGroupByTests : GroupByTestsBase
{
    public MsSqlProGroupByTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
