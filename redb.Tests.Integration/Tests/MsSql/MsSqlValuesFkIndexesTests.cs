using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlValuesFkIndexesTests : ValuesFkIndexesTestsBase
{
    public MsSqlValuesFkIndexesTests(MsSqlFixture fixture) : base(fixture.Redb) { }

    protected override async Task<bool> IndexExistsAsync(string indexName)
        => await Redb.Context.ExecuteScalarAsync<long>(
               "SELECT COUNT(*) FROM sys.indexes WHERE name = '{0}'".Replace("{0}", indexName)) > 0;
}
