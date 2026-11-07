using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlCtSaveInvariantsTests : CtSaveInvariantsTestsBase
{
    public MsSqlCtSaveInvariantsTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
