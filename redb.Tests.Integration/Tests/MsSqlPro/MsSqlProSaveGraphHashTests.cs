using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProSaveGraphHashTests : SaveGraphHashTestsBase
{
    public MsSqlProSaveGraphHashTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
