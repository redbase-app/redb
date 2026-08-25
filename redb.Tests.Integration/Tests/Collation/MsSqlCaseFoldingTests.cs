using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Collation;

/// <summary>
/// SQL Server, with NO setting configured.
///
/// <para>
/// This is not a copy of the other two: it asserts that MSSQL needs nothing. Its default collation
/// (SQL_Latin1_General_CP1_CI_AS and its regional siblings) already folds every script, so the same
/// language matrix must pass on the ordinary fixture with <c>StringCollation</c> unset. If it ever
/// stops passing, the claim in COLLATION.md that MSSQL is unaffected has become false, and this is
/// the test that says so.
/// </para>
/// </summary>
[Collection("MsSql")]
public class MsSqlCaseFoldingTests : CaseFoldingTestsBase
{
    public MsSqlCaseFoldingTests(MsSqlFixture fixture) : base(fixture.Redb) { }

    // Measured, not assumed: SQL Server CI_AS folds BOTH, unlike the ICU root collation.
    // This asymmetry is the reason the boundaries are per-provider rather than shared.
    protected override bool FoldsSharpSToDoubleS => true;
    protected override bool FoldsTurkishDottedI  => true;
}
