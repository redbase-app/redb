using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProCrudTests : CrudTestsBase
{
    public MsSqlProCrudTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
