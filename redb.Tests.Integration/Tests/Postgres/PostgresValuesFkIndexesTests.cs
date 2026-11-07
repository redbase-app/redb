using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresValuesFkIndexesTests : ValuesFkIndexesTestsBase
{
    public PostgresValuesFkIndexesTests(PostgresFixture fixture) : base(fixture.Redb) { }

    protected override async Task<bool> IndexExistsAsync(string indexName)
        => await Redb.Context.ExecuteScalarAsync<long>(
               "SELECT COUNT(*) FROM pg_indexes WHERE indexname = '{0}'".Replace("{0}", indexName)) > 0;
}
