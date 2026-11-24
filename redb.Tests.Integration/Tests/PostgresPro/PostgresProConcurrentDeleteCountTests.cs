using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProConcurrentDeleteCountTests : ConcurrentDeleteCountTestsBase
{
    public PostgresProConcurrentDeleteCountTests(PostgresProFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }

    protected override async Task<IRowHolder> HoldRowAsync(long objectId) => await RowHolders.PostgresAsync(objectId);
}
