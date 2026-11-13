using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteRolledBackSaveIdsTests : RolledBackSaveIdsTestsBase
{
    public SqliteRolledBackSaveIdsTests(SqliteFixture fixture) : base(fixture.ServiceProvider) { }

    protected override string? SequenceTopSql => "SELECT seq FROM sqlite_sequence WHERE name = '_global_identity'";
}
