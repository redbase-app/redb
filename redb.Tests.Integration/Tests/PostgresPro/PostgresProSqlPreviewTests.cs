using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProSqlPreviewTests : SqlPreviewTestsBase
{
    public PostgresProSqlPreviewTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
