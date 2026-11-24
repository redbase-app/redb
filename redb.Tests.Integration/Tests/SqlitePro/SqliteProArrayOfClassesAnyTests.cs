using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProArrayOfClassesAnyTests : ArrayOfClassesAnyTestsBase
{
    public SqliteProArrayOfClassesAnyTests(SqliteProFixture fixture) : base(fixture.Redb) { }

    protected override bool ReadsAnyOverClasses => true;
}
