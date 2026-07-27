using redb.Core.Attributes;
using redb.Core.Exceptions;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Scenario H of docs/SCHEME_EXPLICIT_NAME_PLAN.md — the explicit-name rules, one test per rule.
/// <para>
/// These live here rather than in the integration suite on purpose: a type declaring an invalid
/// <c>[RedbScheme(Name = "...")]</c> cannot exist in the test assembly, because InitializeAsync
/// auto-syncs every attributed type it can see and rethrows, which is exactly the "a bad name must
/// not boot" behaviour we want in production. The rules are enforced by this validator, so this is
/// where they are pinned down.
/// </para>
/// <para>
/// The same rules are duplicated in the database triggers of all three providers. When a rule changes
/// here, it has to change in redbPostgre.sql, redbMSSQL.sql and redbSqlite.sql too.
/// </para>
/// </summary>
public class SchemeNameValidatorTests
{
    [Theory]
    [InlineData("Employee")]
    [InlineData("identity.user")]
    [InlineData("lumi.author_stats")]
    [InlineData("_tsak_metrics")]
    [InlineData("redb.Core.Models.SomeProps")]
    [InlineData("Outer+Inner")]
    [InlineData("_leading_underscore")]
    [InlineData("With9Digits")]
    public void ValidNames_Accepted(string name)
    {
        SchemeNameValidator.IsValid(name, out var error).Should().BeTrue($"'{name}' should be valid but got: {error}");
    }

    [Theory]
    [InlineData("@@__deleted")]
    [InlineData("@@anything goes here")]
    public void SystemPrefixed_BypassesEveryRule(string name)
    {
        SchemeNameValidator.IsValid(name, out _).Should().BeTrue();
    }

    [Theory]
    [InlineData("", "empty")]
    [InlineData("   ", "empty")]
    [InlineData("9Lives", "digit")]
    [InlineData("Naming With Spaces", "characters outside")]
    [InlineData("Именование", "characters outside")]
    [InlineData("_parent: Store", "characters outside")]
    [InlineData("trailing.", "ends with a dot")]
    [InlineData("two..dots", "two consecutive dots")]
    [InlineData("naming.class", "reserved word")]
    [InlineData("object", "reserved word")]
    [InlineData("a.9bad", "not a valid identifier")]
    public void InvalidNames_RejectedWithReason(string name, string expectedReasonFragment)
    {
        SchemeNameValidator.IsValid(name, out var error).Should().BeFalse($"'{name}' must be rejected");
        error.Should().NotBeNull().And.Contain(expectedReasonFragment);
    }

    [Fact]
    public void NameLongerThan128_Rejected()
    {
        var name = new string('a', SchemeNameValidator.MaxLength + 1);

        SchemeNameValidator.IsValid(name, out var error).Should().BeFalse();
        error.Should().Contain("longer than");
    }

    [Fact]
    public void NameOfExactly128_Accepted()
    {
        var name = new string('a', SchemeNameValidator.MaxLength);

        SchemeNameValidator.IsValid(name, out _).Should().BeTrue("128 is the limit, not one past it");
    }

    [Fact]
    public void Validate_NamesTheOffendingTypeAndTheBrokenRule()
    {
        var act = () => SchemeNameValidator.Validate("Naming With Spaces", typeof(SchemeNameValidatorTests));

        var ex = act.Should().Throw<RedbSchemeNameException>();
        ex.Which.SchemeName.Should().Be("Naming With Spaces");
        ex.Which.DeclaringType.Should().Be(typeof(SchemeNameValidatorTests));
        ex.Which.Message.Should().Contain(nameof(SchemeNameValidatorTests),
            "the developer needs to know which class to fix, not just that something is wrong");
    }

    /// <summary>
    /// Scenario G's payload: the conflict exception must name BOTH claimants. End-to-end detection
    /// (two attributed types with one name) is not automatable in-process — see the class remarks.
    /// </summary>
    [Fact]
    public void ConflictException_NamesBothTypes()
    {
        var ex = new RedbSchemeNameConflictException("naming.duplicate", typeof(string), typeof(int));

        ex.Message.Should().Contain("System.String").And.Contain("System.Int32");
        ex.SchemeName.Should().Be("naming.duplicate");
    }
}
