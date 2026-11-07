using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresCancellationTests : CancellationTestsBase
{
    public PostgresCancellationTests(PostgresFixture fixture) : base(fixture.Redb) { }

    protected override string? SleepSql => "SELECT pg_sleep(30)";
}
