using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProValuesFkIndexesTests : ValuesFkIndexesTestsBase
{
    public SqliteProValuesFkIndexesTests(SqliteProFixture fixture) : base(fixture.Redb) { }

    protected override async Task<bool> IndexExistsAsync(string indexName)
        => await Redb.Context.ExecuteScalarAsync<long>(
               "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = '{0}'".Replace("{0}", indexName)) > 0;
}
