using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProIntegerAverageTests : IntegerAverageTestsBase
{
    public MsSqlProIntegerAverageTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
