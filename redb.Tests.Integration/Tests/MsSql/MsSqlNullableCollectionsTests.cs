using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlNullableCollectionsTests : NullableCollectionsTestsBase
{
    public MsSqlNullableCollectionsTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}