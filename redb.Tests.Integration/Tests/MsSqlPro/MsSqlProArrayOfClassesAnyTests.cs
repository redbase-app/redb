using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProArrayOfClassesAnyTests : ArrayOfClassesAnyTestsBase
{
    public MsSqlProArrayOfClassesAnyTests(MsSqlProFixture fixture) : base(fixture.Redb) { }

    protected override bool ReadsAnyOverClasses => true;
}
