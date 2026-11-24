using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProConcurrentDeleteCountTests : ConcurrentDeleteCountTestsBase
{
    public MsSqlProConcurrentDeleteCountTests(MsSqlProFixture fixture) : base(fixture.Redb, fixture.ServiceProvider) { }

    protected override async Task<IRowHolder> HoldRowAsync(long objectId) => await RowHolders.SqlServerAsync(objectId);
}
