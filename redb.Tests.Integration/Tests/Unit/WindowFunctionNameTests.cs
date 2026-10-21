using System;
using AwesomeAssertions;
using redb.Core.Query.Window;
using Xunit;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// <see cref="WindowFuncRequest.Func"/> is a public settable string, so a provider that appends it to
/// the call text unquoted (the old <c>func + "()"</c> fallback in nine places across the three free
/// and three Pro providers) lets a caller close a comment, a bracket or a quote and inject SQL
/// (SQLI-PoC: window function name). <see cref="WindowFunctionName.ZeroArgCall"/> is the single
/// gate: plain identifiers pass, everything else is refused.
/// </summary>
public class WindowFunctionNameTests
{
    [Theory]
    [InlineData("ROW_NUMBER", "ROW_NUMBER()")]
    [InlineData("rank", "RANK()")]
    [InlineData("Cume_Dist", "CUME_DIST()")]
    [InlineData("percent_rank", "PERCENT_RANK()")]
    public void PlainIdentifier_BecomesZeroArgCall(string input, string expected)
        => WindowFunctionName.ZeroArgCall(input).Should().Be(expected);

    [Theory]
    [InlineData("x*/ as [i],(select count(*) from sys.objects)/*")]
    [InlineData("COUNT); DROP TABLE _objects; --")]
    [InlineData("COUNT) OVER (ORDER BY 1)--")]
    [InlineData("1=1 OR ")]
    [InlineData("ROW NUMBER")]
    [InlineData("")]
    [InlineData("   ")]
    public void AnythingElse_IsRefused(string payload)
    {
        var act = () => WindowFunctionName.ZeroArgCall(payload);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void OverlongName_IsRefused()
        => WindowFunctionName.IsPlainIdentifier(new string('a', WindowFunctionName.MaxLength + 1))
            .Should().BeFalse();
}
