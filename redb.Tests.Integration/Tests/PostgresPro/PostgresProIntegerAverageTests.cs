using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProIntegerAverageTests : IntegerAverageTestsBase
{
    public PostgresProIntegerAverageTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
