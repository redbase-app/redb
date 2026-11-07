using System.Text.Json;
using redb.Core.Pro.Query;
using redb.Postgres.Sql;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The Pro HAVING translator (review PR-16). A constant that is not a scalar was bound as its raw JSON text, so
/// <c>COUNT(*) &gt; [1,2]</c> compared a count with the string "[1,2]" instead of being refused.
/// No database required.
/// </summary>
public class HavingSqlTranslatorTests
{
    private static string Translate(string havingJson)
    {
        using var doc = JsonDocument.Parse(havingJson);
        var collector = new SqlParameterCollector(new PostgreSqlDialect());
        return HavingSqlTranslator.Translate(doc.RootElement, collector, field => $"pvt.\"{field}\"");
    }

    [Theory]
    [InlineData("[1,2]")]
    [InlineData("{\"a\":1}")]
    public void AConstantThatIsNotAScalar_IsRefused(string constant)
    {
        var translate = () => Translate($"{{\"$gt\":[{{\"$count\":\"*\"}},{{\"$const\":{constant}}}]}}");

        translate.Should().Throw<InvalidOperationException>().WithMessage("*$const*");
    }

    [Fact]
    public void AScalarConstant_IsBound()
    {
        Translate("{\"$gt\":[{\"$count\":\"*\"},{\"$const\":5}]}").Should().StartWith("(COUNT(*) > ");
    }
}
