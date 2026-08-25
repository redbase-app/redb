using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Collation;

/// <summary>
/// PostgreSQL, feature ON, against a database created with LC_CTYPE=C. Without that database the
/// suite is vacuous: on a normal locale every assertion passes with the feature removed.
/// </summary>
[Collection("PostgresCollation")]
public class PostgresCaseFoldingTests : CaseFoldingTestsBase
{
    public PostgresCaseFoldingTests(PostgresCollationFixture fixture) : base(fixture.Redb) { }

    // ICU root collation: neither fold applies.
    protected override bool FoldsSharpSToDoubleS => false;
    protected override bool FoldsTurkishDottedI  => false;
}
