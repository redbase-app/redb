using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProCtPerfBenchmarkTests : CtPerfBenchmarkTestsBase
{
    public MsSqlProCtPerfBenchmarkTests(MsSqlProFixture fixture) : base(fixture.Redb) { }

    protected override string ProviderName => "MsSqlPro";
}
