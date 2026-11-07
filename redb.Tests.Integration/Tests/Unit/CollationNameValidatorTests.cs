using redb.Core.Models.Configuration;
using redb.Core.Query;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The collation name reaches generated SQL as an <b>identifier</b>, which cannot be a bound
/// parameter, so it is concatenated into SQL text. That makes it an injection point reachable from
/// application configuration, and it is defended twice: rejected at assignment, quoted at emission.
/// Both defences are tested separately, because a test that only exercised one would let the other
/// rot unnoticed.
/// </summary>
public class CollationNameValidatorTests
{
    [Theory]
    [InlineData("und-x-icu")]
    [InlineData("ru-x-icu")]
    [InlineData("en_US.utf8")]
    [InlineData("C")]
    [InlineData("POSIX")]
    [InlineData("de-DE-x-icu")]
    public void Accepts_RealCollationNames(string name)
    {
        CollationNameValidator.IsValid(name).Should().BeTrue();
        var act = () => CollationNameValidator.Validate(name);
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData("x\" OR 1=1--")]          // the injection this exists to stop
    [InlineData("und-x-icu; DROP TABLE _objects")]
    [InlineData("und x icu")]             // space
    [InlineData("und'x'icu")]             // single quote
    [InlineData("-leading-dash")]
    [InlineData("")]
    [InlineData("   ")]
    public void Rejects_AnythingThatIsNotAPlainIdentifier(string name)
    {
        CollationNameValidator.IsValid(name).Should().BeFalse();
        var act = () => CollationNameValidator.Validate(name);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Rejects_OverlongName()
    {
        var name = new string('a', CollationNameValidator.MaxLength + 1);
        var act = () => CollationNameValidator.Validate(name);
        act.Should().Throw<ArgumentException>();
    }

    /// <summary>
    /// Quoting is asserted on input the validator would already reject. The escaping has to be
    /// correct on its own rather than because something upstream promised the case cannot arise:
    /// two independent defences, not one defence and one assumption.
    /// </summary>
    [Fact]
    public void Quote_DoublesEmbeddedQuotes_EvenThoughValidateWouldReject()
    {
        CollationNameValidator.Quote("und-x-icu").Should().Be("\"und-x-icu\"");
        CollationNameValidator.Quote("x\" OR 1=1--").Should().Be("\"x\"\" OR 1=1--\"");
    }

    /// <summary>
    /// The setting validates on assignment, so a typo fails at startup naming the setting, instead
    /// of surfacing later as a driver syntax error on the first search query.
    /// </summary>
    [Fact]
    public void Configuration_RejectsBadNameAtAssignment()
    {
        var config = new RedbServiceConfiguration();
        var act = () => config.StringCollation = "x\" OR 1=1--";
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Configuration_AcceptsGoodNameAndTrims()
    {
        var config = new RedbServiceConfiguration();
        config.StringCollation = "  und-x-icu  ";
        config.StringCollation.Should().Be("und-x-icu");
    }

    /// <summary>Empty and whitespace normalise to null, which is the "feature off" state.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Configuration_BlankMeansOff(string? value)
    {
        var config = new RedbServiceConfiguration { StringCollation = value };
        config.StringCollation.Should().BeNull();
    }

    [Fact]
    public void Configuration_DefaultsToOff()
    {
        new RedbServiceConfiguration().StringCollation.Should().BeNull();
    }
}
