using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProDictionaryEntryLeadsTests : DictionaryEntryLeadsTestsBase
{
    public PostgresProDictionaryEntryLeadsTests(PostgresProFixture fixture) : base(fixture.Redb) { }
}
