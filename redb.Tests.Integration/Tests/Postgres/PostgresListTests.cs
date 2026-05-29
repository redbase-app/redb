using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresListTests : ListTestsBase
{
    public PostgresListTests(PostgresFixture fixture) : base(fixture.Redb) { }

    // Postgres Free PVT supports OrderBy on ListItem.Value / .Alias
    // (verified 2026-05-22 by un-gating the assertions).
    protected override bool SupportsListItemValueAliasOrdering => true;
}
