using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProCtSaveInvariantsTests : CtSaveInvariantsTestsBase
{
    public PostgresProCtSaveInvariantsTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    /// <summary>Фикстура гоняет ChangeTracking - строки нетронутых значений обязаны сохранять _id.</summary>
    protected override bool ExpectStableValueRowIds => true;
}
