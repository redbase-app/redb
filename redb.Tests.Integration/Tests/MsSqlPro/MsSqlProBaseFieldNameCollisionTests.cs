using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProBaseFieldNameCollisionTests : BaseFieldNameCollisionTestsBase
{
    public MsSqlProBaseFieldNameCollisionTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
