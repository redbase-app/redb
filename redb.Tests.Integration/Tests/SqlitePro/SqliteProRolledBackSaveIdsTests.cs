using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProRolledBackSaveIdsTests : RolledBackSaveIdsTestsBase
{
    public SqliteProRolledBackSaveIdsTests(SqliteProFixture fixture) : base(fixture.ServiceProvider) { }

    protected override string? SequenceTopSql => "SELECT seq FROM sqlite_sequence WHERE name = '_global_identity'";
}
