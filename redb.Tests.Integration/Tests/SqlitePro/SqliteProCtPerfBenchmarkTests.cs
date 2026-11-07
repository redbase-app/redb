using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProCtPerfBenchmarkTests : CtPerfBenchmarkTestsBase
{
    public SqliteProCtPerfBenchmarkTests(SqliteProFixture fixture) : base(fixture.Redb) { }

    protected override string ProviderName => "SqlitePro";
}
