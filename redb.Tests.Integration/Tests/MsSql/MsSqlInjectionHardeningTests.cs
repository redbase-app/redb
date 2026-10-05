using FluentAssertions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using redb.Core.Data;
using redb.Core.Models.Users;
using redb.Core.Query.Aggregation;
using redb.Core.Serialization;
using redb.MSSql.Query;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;
using Xunit;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// Fail-closed checks for the Free SQL Server module (SQLI-PoC, MSSQL half). The two builders used
/// to echo a caller-supplied name inside a comment, where a crafted name closed the comment and the
/// remainder became live T-SQL; both now call dbo.pvt_fail. The LIKE check covers the bracket that
/// SQL Server reads as a character-class opener while PostgreSQL and SQLite read it literally.
/// </summary>
[Collection("MsSql")]
public class MsSqlInjectionHardeningTests(MsSqlFixture fixture)
{
    private MssqlQueryProvider Provider => new(
        fixture.ServiceProvider.GetRequiredService<IRedbContext>(),
        fixture.ServiceProvider.GetRequiredService<IRedbObjectSerializer>(),
        schemeSync: fixture.Redb);

    private IRedbContext Ctx => fixture.ServiceProvider.GetRequiredService<IRedbContext>();

    private async Task<long> EmployeeSchemeIdAsync()
        => (await fixture.Redb.LoadSchemeByTypeAsync<EmployeeProps>()).Id;

    [Fact]
    public async Task UnknownFieldInExprFilter_FailsClosed()
    {
        var schemeId = await EmployeeSchemeIdAsync();

        // The control run proves the filter shape itself is fine: a legitimate bound, no rows.
        var control = async () => await Provider.ExecuteAggregateAsync(
            schemeId, "Age", AggregateFunction.Sum,
            """{"$expr":{"$gt":[{"$field":"Age"},{"$const":9999}]}}""");
        await control.Should().NotThrowAsync();

        // The payload closes the comment the builder used to emit around the unknown field name.
        var act = async () => await Provider.ExecuteAggregateAsync(
            schemeId, "Age", AggregateFunction.Sum,
            """{"$expr":{"$gt":[{"$field":"x*/ 1 = 1 OR /*"},{"$const":0}]}}""");

        var ex = (await act.Should().ThrowAsync<SqlException>()).Which;
        ex.Message.Should().Contain("unknown props field");
    }

    [Fact]
    public async Task UnknownWindowFunction_FailsClosedInSql()
    {
        // pvt_build_window_expr(node, fields, base_prefix): an unsupported function name must not
        // travel into the returned SQL. Called straight at the module, since the C# layer refuses
        // such a name even earlier.
        var act = async () => await Ctx.ExecuteScalarAsync<string>(
            "SELECT dbo.pvt_build_window_expr($1, $2, $3)",
            new object[] { """{"func":"x*/ as [i],(select count(*) from sys.objects)/*"}""", "{}", "" });

        var ex = (await act.Should().ThrowAsync<SqlException>()).Which;
        ex.Message.Should().Contain("unsupported window function");
    }

    [Fact]
    public async Task WindowFunctionName_FromCSharp_IsRefused()
    {
        var schemeId = await EmployeeSchemeIdAsync();

        var act = async () => await Provider.ExecuteWindowQueryAsync(
            schemeId,
            selectFields: [],
            windowFuncs: [new redb.Core.Query.Window.WindowFuncRequest { Func = "ROW_NUMBER) OVER (ORDER BY 1)--", Alias = "v" }],
            partitionBy: [],
            orderBy: []);

        // Two layers can refuse it: the C# gate (ArgumentException from WindowFunctionName) and the
        // T-SQL module (SqlException from dbo.pvt_fail). Either is fail-closed, which is the point.
        var ex = await act.Should().ThrowAsync<Exception>();
        ex.Which.Message.Should().MatchRegex("(?i)unsupported window function|plain identifier");
    }

    [Fact]
    public async Task ExactEmailSearch_DoesNotTreatBracketAsCharacterClass()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var email = $"poc.user{suffix}@example.com";
        await fixture.Redb.UserProvider.CreateUserAsync(new CreateUserRequest
        {
            Login = $"poc_like_{suffix}",
            Password = "Poc!12345",
            Name = "POC Like User",
            Email = email
        });

        // Control: the plain address matches, a different one does not.
        var exact = await fixture.Redb.UserProvider.GetUsersAsync(new UserSearchCriteria { EmailExact = email });
        exact.Should().ContainSingle(u => u.Email == email);

        var miss = await fixture.Redb.UserProvider.GetUsersAsync(
            new UserSearchCriteria { EmailExact = $"poc.userz{suffix}@example.com" });
        miss.Should().BeEmpty();

        // The payload used to act as a character class and match a different address.
        var evil = $"poc.user[{suffix[0]}]{suffix[1..]}@example.com";
        var byEvil = await fixture.Redb.UserProvider.GetUsersAsync(new UserSearchCriteria { EmailExact = evil });
        byEvil.Should().BeEmpty("'[' must be searched literally, not as a LIKE character class");
    }
}
