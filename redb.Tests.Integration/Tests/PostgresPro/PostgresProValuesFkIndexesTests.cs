using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProValuesFkIndexesTests : ValuesFkIndexesTestsBase
{
    public PostgresProValuesFkIndexesTests(PostgresProFixture fixture) : base(fixture.Redb) { }

    protected override async Task<bool> IndexExistsAsync(string indexName)
        => await Redb.Context.ExecuteScalarAsync<long>(
               "SELECT COUNT(*) FROM pg_indexes WHERE indexname = '{0}'".Replace("{0}", indexName)) > 0;
}
