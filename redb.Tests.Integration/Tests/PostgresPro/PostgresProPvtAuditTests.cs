using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

/// <summary>
/// Pro PG run of the shared PVT audit suite (<see cref="PvtAuditTestsBase"/>).
/// Pro must reach the same observable LINQ behaviour as Free; SQL dialects
/// differ but the test assertions are dialect-agnostic.
/// </summary>
[Collection("PostgresPro")]
public class PostgresProPvtAuditTests : PvtAuditTestsBase
{
    public PostgresProPvtAuditTests(PostgresProFixture fixture) : base(fixture.Redb, isPro: true) { }
}
