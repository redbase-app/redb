using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProArrayOfClassesAnyTests : ArrayOfClassesAnyTestsBase
{
    public PostgresProArrayOfClassesAnyTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    protected override bool ReadsAnyOverClasses => true;
}
