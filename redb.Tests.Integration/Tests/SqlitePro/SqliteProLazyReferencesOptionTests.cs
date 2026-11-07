using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

[Collection("SqlitePro")]
public class SqliteProLazyReferencesOptionTests : LazyReferencesOptionTestsBase
{
    public SqliteProLazyReferencesOptionTests(SqliteProFixture fixture) : base(fixture.Redb) { }

    // Л2 boundary: the Pro materializer reads the global option, not the session flag.
    protected override bool PerQueryOverrideWorks => false;
    protected override bool GlobalMutationAffects => true;
}
