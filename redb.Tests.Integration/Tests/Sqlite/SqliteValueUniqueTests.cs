using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteValueUniqueTests : ValueUniqueTestsBase
{
    public SqliteValueUniqueTests(SqliteFixture fixture) : base(fixture.Redb) { }

    /// <summary>SQLite names only columns - the violated structure is not resolvable.</summary>
    protected override bool DriverReportsViolatedStructure => false;
}
