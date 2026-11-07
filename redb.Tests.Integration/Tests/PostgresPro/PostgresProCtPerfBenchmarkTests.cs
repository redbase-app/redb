using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProCtPerfBenchmarkTests : CtPerfBenchmarkTestsBase
{
    public PostgresProCtPerfBenchmarkTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    protected override string ProviderName => "PostgresPro";
}
