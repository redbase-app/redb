using System.Data.Common;
using System.Globalization;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Export.Providers;

namespace redb.Export.Services;

/// <summary>
/// The schema an export or import needs from its database: what a redb service of this build requires at start.
/// PostgreSQL and SQL Server state it through their module functions, SQLite through <c>PRAGMA user_version</c>.
/// </summary>
/// <param name="Provider">Provider name as the services report it (PostgreSQL, MSSql, SQLite).</param>
/// <param name="RequiredVersion">Module version (SQLite: file schema version) this build requires.</param>
/// <param name="VersionFunction">Module version function; null on SQLite.</param>
/// <param name="CompatFunction">Module compatibility function; null on SQLite.</param>
internal sealed record ExpectedSchema(string Provider, string RequiredVersion, string? VersionFunction, string? CompatFunction)
{
    internal bool IsSqlite => VersionFunction is null;

    /// <summary>The schema the provider build referenced by redb.Export requires, for an export provider.</summary>
    public static ExpectedSchema For(IDataProvider provider) => provider.Name switch
    {
        "postgres" => FromDialect("PostgreSQL", new global::redb.Postgres.Sql.PostgreSqlDialect()),
        "mssql" => FromDialect("MSSql", new global::redb.MSSql.Sql.MsSqlDialect()),
        "sqlite" => new ExpectedSchema("SQLite",
            global::redb.SQLite.RedbService.SchemaVersion.ToString(CultureInfo.InvariantCulture), null, null),
        _ => throw new ArgumentException($"Unknown provider: {provider.Name}", nameof(provider)),
    };

    private static ExpectedSchema FromDialect(string name, global::redb.Core.Query.ISqlDialect dialect) =>
        new(name, dialect.Query_PvtRequiredVersion()!, dialect.Query_PvtModuleVersionFunction()!, dialect.Query_PvtModuleCompatFunction());
}

/// <summary>
/// Stops an export from or an import into a database whose schema this build does not match, before anything is read
/// for the file or written to the database, with the exceptions a redb service throws at start:
/// <list type="bullet">
/// <item>the same version passes;</item>
/// <item>an older one stops with <see cref="RedbSchemaOutdatedException"/>: the tool upgrades nothing;</item>
/// <item>a newer database is refused for an export whatever it serves (the file would miss what this build does not
/// know), and passes for an import only when it says it still serves this build, as a service start does;
/// otherwise <see cref="RedbSchemaNewerException"/>.</item>
/// </list>
/// Without it the first statement naming a column the database does not have failed with a driver error, and a
/// parallel import had already committed its cleaning by then.
/// </summary>
internal static class SchemaGate
{
    private const string Reason = "the redb CLI does not upgrade a database";

    /// <summary>Checks the source of an export.</summary>
    public static Task CheckSourceAsync(IDataProvider provider, CancellationToken ct) =>
        CheckAsync(provider, ExpectedSchema.For(provider), export: true, ct);

    /// <summary>Checks the target of an import.</summary>
    public static Task CheckTargetAsync(IDataProvider provider, CancellationToken ct) =>
        CheckAsync(provider, ExpectedSchema.For(provider), export: false, ct);

    private static async Task CheckAsync(IDataProvider provider, ExpectedSchema expected, bool export, CancellationToken ct)
    {
        var connection = provider.Connection;

        // A database without the redb schema is not an older one: it needs `redb init`, not an upgrade.
        if (!await RedbSchemaProbe.ExistsAsync(provider, ct))
        {
            throw new InvalidOperationException(export
                ? $"The {expected.Provider} database holds no redb schema (table _schemes not found): there is nothing " +
                  "to export, and nothing was read. Check that the connection string names the database you meant."
                : $"The {expected.Provider} database holds no redb schema (table _schemes not found), and nothing was " +
                  $"changed. Create it with `redb init --provider {provider.Name}`, then import again.");
        }

        var (deployed, newer) = expected.IsSqlite
            ? await ReadSqliteAsync(connection, expected, ct)
            : await ReadModuleAsync(connection, expected, ct);

        if (deployed == expected.RequiredVersion)
            return;

        var what = export ? "export from it" : "import into it";
        if (!newer)
        {
            // No version number: a build between releases carries the last released one, and naming it would send
            // the reader to the wrong packages. `redb --version` prints this CLI's own.
            var remedy = expected.IsSqlite
                ? $"Upgrade the file with `redb init --provider sqlite` of this CLI, or open it once with a redb " +
                  $"application of the same redb version as this CLI (`redb --version`), then {what} again."
                : $"Bring the database to the module this build requires: have the schema owner run the script of " +
                  $"`redb schema --upgrade --provider {CliProviderName(expected)}` with psql / sqlcmd, or start a redb " +
                  $"application of the same redb version as this CLI (`redb --version`) on it, then {what} again.";
            throw new RedbSchemaOutdatedException(expected.Provider, deployed, expected.RequiredVersion, Reason, remedy);
        }

        var (serves, fromPackage) = await ReadCompatAsync(connection, expected, ct);
        if (export)
        {
            throw new RedbSchemaNewerException(expected.Provider, deployed!, expected.RequiredVersion, fromPackage,
                "An export by this redb CLI would leave out what its build does not know of the newer schema: export " +
                "with a redb CLI of the version whose packages run on this database.");
        }

        if (!serves)
        {
            throw new RedbSchemaNewerException(expected.Provider, deployed!, expected.RequiredVersion, fromPackage,
                "Import with a redb CLI of the version whose packages run on this database.");
        }
    }

    /// <summary>The module version and whether it is newer than the required one; a missing module reads as null.</summary>
    private static async Task<(string? Deployed, bool Newer)> ReadModuleAsync(
        DbConnection connection, ExpectedSchema expected, CancellationToken ct)
    {
        var deployed = await TryScalarFunctionAsync(connection, expected.VersionFunction!, ct);
        return (deployed, ModuleCompatibility.IsNewer(deployed, expected.RequiredVersion));
    }

    /// <summary>The file schema version and whether it is newer than the one this build writes.</summary>
    private static async Task<(string? Deployed, bool Newer)> ReadSqliteAsync(
        DbConnection connection, ExpectedSchema expected, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA user_version";
        var version = Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
        var required = long.Parse(expected.RequiredVersion, CultureInfo.InvariantCulture);
        return (version.ToString(CultureInfo.InvariantCulture), version > required);
    }

    /// <summary>
    /// Whether the newer database serves this build, and the package version it names. PostgreSQL and SQL Server ask
    /// the module's compatibility function (<see cref="ModuleCompatibility.Serves"/>); SQLite reads the
    /// <c>_schema_compat</c> row as the SQLite service does, and a file without it serves nothing.
    /// </summary>
    private static async Task<(bool Serves, string? FromPackage)> ReadCompatAsync(
        DbConnection connection, ExpectedSchema expected, CancellationToken ct)
    {
        if (!expected.IsSqlite)
        {
            var compat = expected.CompatFunction is null ? null : await TryScalarFunctionAsync(connection, expected.CompatFunction, ct);
            var serves = ModuleCompatibility.Serves(compat, expected.RequiredVersion, out var fromPackage);
            return (serves, fromPackage);
        }

        await using (var exists = connection.CreateCommand())
        {
            exists.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_schema_compat'";
            if (Convert.ToInt64(await exists.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) == 0)
                return (false, null);
        }

        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT _min_reader, _package FROM _schema_compat LIMIT 1";
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            return (false, null);

        var minReader = reader.IsDBNull(0) ? (long?)null : reader.GetInt64(0);
        var package = reader.IsDBNull(1) ? null : reader.GetString(1);
        var own = long.Parse(expected.RequiredVersion, CultureInfo.InvariantCulture);
        return (minReader is not null && minReader <= own, package);
    }

    private static async Task<string?> TryScalarFunctionAsync(DbConnection connection, string function, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {function}()";
        try
        {
            var value = await command.ExecuteScalarAsync(ct);
            return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (DbErrorClassifier.IsUndefinedFunction(ex))
        {
            return null;
        }
    }

    private static string CliProviderName(ExpectedSchema expected) =>
        expected.Provider == "PostgreSQL" ? "postgres" : "mssql";
}
