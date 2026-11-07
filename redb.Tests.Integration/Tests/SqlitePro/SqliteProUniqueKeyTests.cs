using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProUniqueKeyTests : UniqueKeyTestsBase
{
    public SqliteProUniqueKeyTests(SqliteProFixture fixture) : base(fixture.Redb) { }

    protected override string BoolTrue => "1";

    // See SqliteUniqueKeyTests: the driver message carries no key tuple.
    protected override bool ReportsKeyTuple => false;
}
