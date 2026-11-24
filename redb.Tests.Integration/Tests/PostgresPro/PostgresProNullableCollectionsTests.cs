using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProNullableCollectionsTests : NullableCollectionsTestsBase
{
    public PostgresProNullableCollectionsTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}