using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProDecimalRoundTripTests : DecimalRoundTripTestsBase
{
    public SqliteProDecimalRoundTripTests(SqliteProFixture fixture) : base(fixture.Redb) { }

    protected override bool StoresDecimalAsDouble => true;
}
