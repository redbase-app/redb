using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProTagsFieldTests : TagsFieldTestsBase
{
    public MsSqlProTagsFieldTests(MsSqlProFixture fixture) : base(fixture.Redb) { }
}
