using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProArrayGroupMetadataCacheTests : ArrayGroupMetadataCacheTestsBase
{
    public PostgresProArrayGroupMetadataCacheTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
