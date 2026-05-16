using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProWhereTests : WhereTestsBase
{
    public PostgresProWhereTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
