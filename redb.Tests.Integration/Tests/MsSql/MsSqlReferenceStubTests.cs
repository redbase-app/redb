using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlReferenceStubTests : ReferenceStubTestsBase
{
    public MsSqlReferenceStubTests(MsSqlFixture fixture) : base(fixture.Redb) { }
}
