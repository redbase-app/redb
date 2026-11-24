using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresArrayOfClassesAnyTests : ArrayOfClassesAnyTestsBase
{
    public PostgresArrayOfClassesAnyTests(PostgresFixture fixture) : base(fixture.Redb) { }

    protected override bool ReadsAnyOverClasses => false;
}
