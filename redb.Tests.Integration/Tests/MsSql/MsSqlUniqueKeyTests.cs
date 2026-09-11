using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlUniqueKeyTests : UniqueKeyTestsBase
{
    public MsSqlUniqueKeyTests(MsSqlFixture fixture) : base(fixture.Redb) { }

    protected override string BoolTrue => "1";
}
