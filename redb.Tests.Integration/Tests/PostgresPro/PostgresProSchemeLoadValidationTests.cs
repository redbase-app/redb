using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProSchemeLoadValidationTests : SchemeLoadValidationTestsBase
{
    public PostgresProSchemeLoadValidationTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
