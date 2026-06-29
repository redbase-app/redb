using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProProQueryTests : ProQueryTestsBase
{
    public SqliteProProQueryTests(SqliteProFixture fixture) : base(fixture.Redb, isPro: true) { }
}
