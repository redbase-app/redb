using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProCtSaveInvariantsTests : CtSaveInvariantsTestsBase
{
    public MsSqlProCtSaveInvariantsTests(MsSqlProFixture fixture) : base(fixture.Redb) { }

    /// <summary>Фикстура гоняет ChangeTracking - строки нетронутых значений обязаны сохранять _id.</summary>
    protected override bool ExpectStableValueRowIds => true;
}
