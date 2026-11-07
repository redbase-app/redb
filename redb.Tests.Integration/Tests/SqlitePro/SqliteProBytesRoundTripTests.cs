using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProBytesRoundTripTests : BytesRoundTripTestsBase
{
    public SqliteProBytesRoundTripTests(SqliteProFixture fixture) : base(fixture.Redb) { }
}
