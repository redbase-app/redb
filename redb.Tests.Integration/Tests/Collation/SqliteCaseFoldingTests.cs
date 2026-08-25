using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Collation;

/// <summary>
/// SQLite, feature ON. No special database is needed: ASCII-only folding is unconditional there,
/// so an ordinary file reproduces it.
/// </summary>
[Collection("SqliteCollation")]
public class SqliteCaseFoldingTests : CaseFoldingTestsBase
{
    public SqliteCaseFoldingTests(SqliteCollationFixture fixture) : base(fixture.Redb) { }

    // .NET ToLowerInvariant folds character by character, like ICU root.
    protected override bool FoldsSharpSToDoubleS => false;
    protected override bool FoldsTurkishDottedI  => false;
}
