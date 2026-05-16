using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresPolymorphicTreeTests : PolymorphicTreeTestsBase
{
    public PostgresPolymorphicTreeTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
