using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteUniqueKeyTests : UniqueKeyTestsBase
{
    public SqliteUniqueKeyTests(SqliteFixture fixture) : base(fixture.Redb) { }

    protected override string BoolTrue => "1";

    // SQLite's message names only the columns ("UNIQUE constraint failed: _values._id_structure,
    // _values._unique"), so the exception cannot resolve the property.
    protected override bool ReportsKeyTuple => false;
}
