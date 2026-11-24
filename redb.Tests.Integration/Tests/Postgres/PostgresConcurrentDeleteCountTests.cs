using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresConcurrentDeleteCountTests : ConcurrentDeleteCountTestsBase
{
    public PostgresConcurrentDeleteCountTests(PostgresFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }

    protected override async Task<IRowHolder> HoldRowAsync(long objectId) => await RowHolders.PostgresAsync(objectId);
}
