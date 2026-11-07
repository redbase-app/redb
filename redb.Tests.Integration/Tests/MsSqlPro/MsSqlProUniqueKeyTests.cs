using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProUniqueKeyTests : UniqueKeyTestsBase
{
    public MsSqlProUniqueKeyTests(MsSqlProFixture fixture) : base(fixture.Redb) { }

    protected override string BoolTrue => "1";

    // ChangeTracking: see PostgresProUniqueKeyTests.
    protected override bool KeySwapMustPass => false;
}
