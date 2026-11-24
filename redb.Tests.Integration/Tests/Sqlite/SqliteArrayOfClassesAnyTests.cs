using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Sqlite;

[Collection("Sqlite")]
public class SqliteArrayOfClassesAnyTests : ArrayOfClassesAnyTestsBase
{
    public SqliteArrayOfClassesAnyTests(SqliteFixture fixture) : base(fixture.Redb) { }

    protected override bool ReadsAnyOverClasses => false;
}
