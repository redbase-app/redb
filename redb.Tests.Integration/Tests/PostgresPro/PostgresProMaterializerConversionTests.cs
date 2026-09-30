using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProMaterializerConversionTests : ProMaterializerConversionTestsBase
{
    public PostgresProMaterializerConversionTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
