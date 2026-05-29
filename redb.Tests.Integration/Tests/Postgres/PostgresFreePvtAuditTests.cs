using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// Free PG run of the shared PVT audit suite (<see cref="PvtAuditTestsBase"/>).
/// Locks the state recorded in docs/FreePvtQuery/FREE-OVER-PRO.md §2.x.
/// </summary>
[Collection("Postgres")]
public class PostgresFreePvtAuditTests : PvtAuditTestsBase
{
    public PostgresFreePvtAuditTests(PostgresFixture fixture) : base(fixture.Redb, isPro: false) { }
}
