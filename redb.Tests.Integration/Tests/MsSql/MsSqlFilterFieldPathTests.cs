using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlFilterFieldPathTests : FilterFieldPathTestsBase
{
    public MsSqlFilterFieldPathTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
