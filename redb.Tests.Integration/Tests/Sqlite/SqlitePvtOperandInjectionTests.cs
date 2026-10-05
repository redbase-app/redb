using FluentAssertions;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Data;
using redb.Core.Models.Entities;
using redb.Core.Query.Aggregation;
using redb.Core.Serialization;
using redb.SQLite.Query;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;
using Xunit;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// The native PVT builder used to paste a JSON **string** operand into the statement unquoted
/// whenever the column was not text, so caller-supplied text became live SQL
/// (SQLI-PoC: operand splice, <c>redb_pvt.c</c> <c>pvtAppendOperand</c>). It now splices only a
/// plain numeric literal and otherwise emits a call to <c>pvt_refuse()</c>, which raises when the
/// statement runs. The last test asserts the connection no longer leaves the extension loader
/// enabled, because that turned such a splice into <c>load_extension()</c>.
/// </summary>
[Collection("Sqlite")]
public class SqlitePvtOperandInjectionTests(SqliteFixture fixture)
{
    private const string SumAlias = "value";

    private SqliteQueryProvider Provider => new(
        fixture.ServiceProvider.GetRequiredService<IRedbContext>(),
        fixture.ServiceProvider.GetRequiredService<IRedbObjectSerializer>(),
        schemeSync: fixture.Redb);

    private static AggregateRequest SumAge() =>
        new() { FieldPath = "Age", Function = AggregateFunction.Sum, Alias = SumAlias };

    private async Task<long> SchemeIdAsync()
        => (await fixture.Redb.LoadSchemeByTypeAsync<EmployeeProps>()).Id;

    private Task SeedOneEmployeeAsync()
        => fixture.Redb.SaveAsync(new RedbObject<EmployeeProps>
        {
            name = "poc-operand",
            Props = new EmployeeProps { FirstName = "A", LastName = "B", Age = 30 }
        });

    [Fact]
    public async Task TextOperand_ForNonTextColumn_IsRefused()
    {
        await SeedOneEmployeeAsync();
        var schemeId = await SchemeIdAsync();

        var act = async () => await Provider.ExecuteAggregateBatchAsync(
            schemeId, [SumAge()], """{"Age": "0 OR 1=1"}""");

        var ex = (await act.Should().ThrowAsync<SqliteException>()).Which;
        ex.Message.Should().Contain("operand type mismatch",
            "a string against a Long column has no safe literal form and must be refused");
    }

    [Fact]
    public async Task SubqueryOperand_IsRefused()
    {
        await SeedOneEmployeeAsync();
        var schemeId = await SchemeIdAsync();

        var act = async () => await Provider.ExecuteAggregateBatchAsync(
            schemeId, [SumAge()], """{"Age": "0 OR (SELECT COUNT(*) FROM _users) > 0"}""");

        await act.Should().ThrowAsync<SqliteException>();
    }

    [Fact]
    public async Task NumericStringOperand_IsStillAccepted()
    {
        await SeedOneEmployeeAsync();
        var schemeId = await SchemeIdAsync();

        var batch = await Provider.ExecuteAggregateBatchAsync(
            schemeId, [SumAge()], """{"Age": "30"}""");

        batch.Values.Should().ContainKey(SumAlias);
        batch.Values[SumAlias].Should().NotBeNull("a numeric literal is a legitimate operand for a Long column");
    }

    [Fact]
    public async Task ArrayOperand_IsRefused()
    {
        await SeedOneEmployeeAsync();
        var schemeId = await SchemeIdAsync();

        var act = async () => await Provider.ExecuteAggregateBatchAsync(
            schemeId, [SumAge()], """{"Age": ["x] OR 1=1 --"]}""");

        await act.Should().ThrowAsync<Exception>("an array operand for a Long column must be refused");
    }

    [Fact]
    public async Task LoadExtension_IsRefused_OnTheProviderConnection()
    {
        // SqliteDataSource.LoadNativeExtension turns EnableExtensions(true) back off once our
        // extension is loaded, so no statement on a redb connection may reach the loader.
        var ctx = fixture.ServiceProvider.GetRequiredService<IRedbContext>();

        var act = async () => await ctx.ExecuteAsync("SELECT load_extension('redb_missing_ext')");

        var ex = (await act.Should().ThrowAsync<SqliteException>()).Which;
        ex.Message.Should().Contain("not authorized");
    }
}
