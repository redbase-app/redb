using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresTagsFieldTests : TagsFieldTestsBase
{
    public PostgresTagsFieldTests(PostgresFixture fixture) : base(fixture.Redb) { }
}
