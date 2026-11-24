using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProDeleteBatchCountTests : DeleteBatchCountTestsBase
{
    public PostgresProDeleteBatchCountTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
