using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Collation;

/// <summary>
/// PostgreSQL Pro, feature ON, C-ctype database, PVT prefilter forced on.
///
/// <para>
/// Pro does not share the Free path: it builds SQL in C# and attaches the collation through the
/// dialect, where Free builds SQL inside the database and reads a GUC. The first version of this
/// suite covered Free only, which would have left every Pro user unprotected while the tests stayed
/// green.
/// </para>
/// </summary>
[Collection("PostgresProCollation")]
public class PostgresProCaseFoldingTests : CaseFoldingTestsBase
{
    public PostgresProCaseFoldingTests(PostgresProCollationFixture fixture) : base(fixture.Redb) { }

    // ICU root collation: neither fold applies.
    protected override bool FoldsSharpSToDoubleS => false;
    protected override bool FoldsTurkishDottedI  => false;
}
