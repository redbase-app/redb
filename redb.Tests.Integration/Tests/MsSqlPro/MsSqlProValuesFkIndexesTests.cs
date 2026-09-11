using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSqlPro;

[Collection("MsSqlPro")]
public class MsSqlProValuesFkIndexesTests : ValuesFkIndexesTestsBase
{
    public MsSqlProValuesFkIndexesTests(MsSqlProFixture fixture) : base(fixture.Redb) { }

    protected override async Task<bool> IndexExistsAsync(string indexName)
        => await Redb.Context.ExecuteScalarAsync<long>(
               "SELECT COUNT(*) FROM sys.indexes WHERE name = '{0}'".Replace("{0}", indexName)) > 0;
}
