using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresSchemeLoadValidationTests : SchemeLoadValidationTestsBase
{
    public PostgresSchemeLoadValidationTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
