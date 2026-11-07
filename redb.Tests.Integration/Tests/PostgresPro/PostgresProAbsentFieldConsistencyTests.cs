using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProAbsentFieldConsistencyTests : AbsentFieldConsistencyTestsBase
{
    public PostgresProAbsentFieldConsistencyTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
