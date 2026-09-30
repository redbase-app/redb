using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// Runs the SQL module's own smoke runner, <c>redb.MSSql/sql/v2-pvt/99_smoke_auto.sql</c>, against the fixture
/// database (review SQ-4). It was run by hand only, so nothing kept it passing. It builds SQL for a table of filter
/// cases over the test model corpus, asserts shapes, and raises at the end when any case failed; the PRINT lines
/// name the cases and are attached to the failure.
/// </summary>
[Collection("MsSql")]
public class MsSqlModuleSmokeTests
{
    // Ensures the fixture has initialized the database (schemes, module) before the script runs.
    public MsSqlModuleSmokeTests(MsSqlFixture fixture) => _ = fixture.Redb;

    [Fact]
    public async Task TheModuleSmokeRunner_Passes()
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build().GetConnectionString("MSSql")!;
        var script = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Smoke", "mssql_99_smoke_auto.sql"));
        var printed = new List<string>();

        // One connection for every batch: the runner keeps its cases in a #temp table.
        await using var connection = new SqlConnection(cs);
        connection.InfoMessage += (_, e) => printed.Add(e.Message);
        await connection.OpenAsync();
        try
        {
            foreach (var batch in Regex.Split(script, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase))
            {
                if (string.IsNullOrWhiteSpace(batch)) continue;
                await using var command = new SqlCommand(batch, connection) { CommandTimeout = 600 };
                await command.ExecuteNonQueryAsync();
            }
        }
        catch (SqlException ex)
        {
            throw new Xunit.Sdk.XunitException(
                ex.Message + Environment.NewLine + string.Join(Environment.NewLine, printed.Where(IsFailure)));
        }

        printed.Where(IsFailure).Should().BeEmpty();
    }

    // "FAIL [scheme / case] ...", "INSPECT [...] FAILED: ...", "SHAPE-FAIL ..." - not a summary such as "fail: 0".
    private static bool IsFailure(string line) =>
        line.StartsWith("FAIL [", StringComparison.Ordinal)
        || line.Contains(" FAILED", StringComparison.Ordinal)
        || line.Contains("SHAPE-FAIL", StringComparison.Ordinal);
}
