using Microsoft.Extensions.Configuration;
using Npgsql;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// Runs the SQL module's own smoke runner, <c>redb.Postgres/sql/v2-pvt/99_smoke_auto.sql</c>, against the fixture
/// database (review SQ-4). It was run by hand only, so nothing kept it passing. It builds SQL for a table of filter
/// cases over the test model corpus, asserts shapes, and raises at the end when any case failed; the NOTICE lines
/// name the cases and are attached to the failure.
/// </summary>
[Collection("Postgres")]
public class PostgresModuleSmokeTests
{
    // Ensures the fixture has initialized the database (schemes, module) before the script runs.
    public PostgresModuleSmokeTests(PostgresFixture fixture) => _ = fixture.Redb;

    [Fact]
    public async Task TheModuleSmokeRunner_Passes()
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("Postgres")!;
        var script = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Smoke", "postgres_99_smoke_auto.sql"));
        var notices = new List<string>();

        await using var connection = new NpgsqlConnection(cs);
        connection.Notice += (_, e) => notices.Add(e.Notice.MessageText);
        await connection.OpenAsync();
        try
        {
            await using var command = new NpgsqlCommand(script, connection) { CommandTimeout = 600 };
            await command.ExecuteNonQueryAsync();
        }
        catch (PostgresException ex)
        {
            throw new Xunit.Sdk.XunitException(
                ex.MessageText + Environment.NewLine + string.Join(Environment.NewLine, notices.Where(IsFailure)));
        }

        notices.Where(IsFailure).Should().BeEmpty();
    }

    // "FAIL [scheme / case] ...", "INSPECT [...] FAILED: ...", "SHAPE-FAIL ..." - not a summary such as "fail: 0".
    private static bool IsFailure(string line) =>
        line.StartsWith("FAIL [", StringComparison.Ordinal)
        || line.Contains(" FAILED", StringComparison.Ordinal)
        || line.Contains("SHAPE-FAIL", StringComparison.Ordinal);
}
