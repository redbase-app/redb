using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlSchemeNamingTests : SchemeNamingTestsBase
{
    public MsSqlSchemeNamingTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
