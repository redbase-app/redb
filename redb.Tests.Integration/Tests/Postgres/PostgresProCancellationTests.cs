using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("PostgresPro")]
public class PostgresProCancellationTests : CancellationTestsBase
{
    public PostgresProCancellationTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    protected override string? SleepSql => "SELECT pg_sleep(30)";
}
