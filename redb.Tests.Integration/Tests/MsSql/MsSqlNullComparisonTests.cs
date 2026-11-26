using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlNullComparisonTests : NullComparisonTestsBase
{
    public MsSqlNullComparisonTests(MsSqlFixture fixture) : base(fixture.Redb) { }

    protected override bool NegatedListItemTakesObjectsWithoutItem => true;
}