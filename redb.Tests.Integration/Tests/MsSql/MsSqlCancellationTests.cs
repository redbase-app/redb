using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlCancellationTests : CancellationTestsBase
{
    public MsSqlCancellationTests(MsSqlFixture fixture) : base(fixture.Redb) { }

    protected override string? SleepSql => "WAITFOR DELAY '00:00:30'";
}
