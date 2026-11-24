using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlConcurrentDeleteCountTests : ConcurrentDeleteCountTestsBase
{
    public MsSqlConcurrentDeleteCountTests(MsSqlFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }

    protected override async Task<IRowHolder> HoldRowAsync(long objectId) => await RowHolders.SqlServerAsync(objectId);
}
