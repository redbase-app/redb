using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProRegexQueryTests : RegexQueryTestsBase
{
    public MsSqlProRegexQueryTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
