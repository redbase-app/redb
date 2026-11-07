using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProUniqueKeyTests : UniqueKeyTestsBase
{
    public PostgresProUniqueKeyTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    // The Pro fixture runs PropsSaveStrategy.ChangeTracking: an in-batch key swap may hit the
    // index mid-way - the documented boundary of §4.6 (typed error, never corruption).
    protected override bool KeySwapMustPass => false;
}
