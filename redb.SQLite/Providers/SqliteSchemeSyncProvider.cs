using System.Text.Json;
using redb.Core.Data;
using redb.Core.Models.Configuration;
using redb.Core.Providers;
using redb.Core.Providers.Base;
using redb.SQLite.Sql;
using Microsoft.Extensions.Logging;

namespace redb.SQLite.Providers;

/// <summary>
/// SQLite implementation of scheme synchronization provider.
/// Inherits all logic from SchemeSyncProviderBase, provides SQLite-specific SQL via SqliteDialect.
/// </summary>
public class SqliteSchemeSyncProvider : SchemeSyncProviderBase
{
    /// <summary>
    /// Creates SQLite scheme sync provider with default SqliteDialect.
    /// </summary>
    public SqliteSchemeSyncProvider(
        IRedbContext context, 
        RedbServiceConfiguration? configuration = null,
        string? cacheDomain = null,
        ILogger? logger = null)
        : base(context, new SqliteDialect(), configuration, cacheDomain, logger)
    {
    }

    /// <summary>
    /// Gets structure tree as JsonElement (for API compatibility).
    /// </summary>
    public new async Task<JsonElement> GetStructureTreeJsonAsync(long schemeId)
    {
        var result = await base.GetStructureTreeJsonAsync(schemeId);
        
        if (string.IsNullOrEmpty(result))
            return JsonSerializer.SerializeToElement("[]");
        
        return JsonSerializer.SerializeToElement(result);
    }

    /// <summary>
    /// SQLite type migration for an existing structure whose property type changed. The Postgres/MSSQL
    /// path runs a stored function (<c>migrate_structure_type</c>); SQLite has no stored functions, so the
    /// equivalent runs here as plain SQL. Fixes GitHub #5, where the base path emitted
    /// <c>SELECT * FROM migrate_structure_type(...)</c> and failed with
    /// <c>no such table: migrate_structure_type</c>, so <c>InitializeAsync</c> never came up.
    /// </summary>
    public override async Task<TypeMigrationResult> MigrateStructureTypeAsync(
        long structureId, string oldTypeName, string newTypeName, bool dryRun = false)
    {
        var source = GetValueColumn(oldTypeName);
        var target = GetValueColumn(newTypeName);

        // No scalar value-column mapping on one side (e.g. a nested Class property): there is nothing to
        // move in _values here; let the caller update _id_type. Mirrors the PG function returning a benign
        // result rather than throwing.
        if (source == null || target == null)
            return new TypeMigrationResult { Errors = $"No value-column mapping for '{oldTypeName}' -> '{newTypeName}'" };

        // Same storage column (Int -> Long both _Long; DateTime -> DateOnly both _DateTimeOffset;
        // String -> MimeType both _String): values already sit in the right column, only _id_type changes.
        if (string.Equals(source, target, StringComparison.OrdinalIgnoreCase))
            return new TypeMigrationResult();

        // Rows that actually hold a value under the old column for this structure.
        var affected = (int)(await Context.ExecuteScalarAsync<long?>(
            $"SELECT COUNT(*) FROM _values WHERE _id_structure = $1 AND {source} IS NOT NULL", structureId) ?? 0);

        if (affected == 0 || dryRun)
            return new TypeMigrationResult { AffectedRows = affected };

        // There IS data to move across storage columns. Which of those moves is safe is decided by an
        // explicit matrix, mirroring the pairs PostgreSQL's migrate_structure_type() supports. A blanket
        // refusal used to stand here; it was written when the alternative was a cryptic "no such table"
        // (GitHub #5) and it swept up conversions that are trivially safe — Boolean and Long are both
        // INTEGER columns, so bool -> int is a plain copy.
        var conversion = BuildConversion(source, target);

        if (conversion == null)
        {
            // Reported as a result rather than thrown, which is how PostgreSQL and MSSQL report the same
            // refusal. The scheme-sync path turns any failing result into RedbTypeMigrationException, so
            // the caller that matters still gets an exception — the same one on all three providers.
            return new TypeMigrationResult
            {
                AffectedRows = affected,
                SuccessCount = 0,
                ErrorCount = affected,
                Errors =
                    $"SQLite: cannot auto-migrate structure {structureId} from '{oldTypeName}' to '{newTypeName}' — " +
                    $"{affected} value(s) live in column {source} and moving them to {target} is not a safe automatic " +
                    $"conversion. Remove and re-add the property (dropping its values), or migrate the data manually " +
                    $"before changing the type."
            };
        }

        // Rows the conversion can actually take. For text sources this is narrower than `affected`:
        // an unparseable string must stay where it is rather than become CAST's silent 0.
        var convertible = (int)(await Context.ExecuteScalarAsync<long?>(
            $"SELECT CAST(COUNT(*) AS BIGINT) FROM _values WHERE _id_structure = $1 " +
            $"AND {source} IS NOT NULL AND {conversion.Guard}", structureId) ?? 0);

        await Context.ExecuteAsync(
            $"UPDATE _values SET {target} = {conversion.Expression}, {source} = NULL " +
            $"WHERE _id_structure = $1 AND {source} IS NOT NULL AND {conversion.Guard}", structureId);

        return new TypeMigrationResult
        {
            AffectedRows = affected,
            SuccessCount = convertible,
            ErrorCount = affected - convertible,
            Errors = convertible == affected
                ? null
                : $"SQLite: {affected - convertible} of {affected} value(s) in {source} could not be read as " +
                  $"'{newTypeName}' and were left in place; {convertible} were moved to {target}."
        };
    }

    /// <summary>A conversion: the value expression for the target column, and the rows it applies to.</summary>
    private sealed record Conversion(string Expression, string Guard);

    /// <summary>
    /// Cross-column conversions SQLite performs, keyed by storage column pair. Mirrors the matrix in
    /// PostgreSQL's <c>migrate_structure_type()</c> so a type change behaves the same on every provider.
    ///
    /// <para>
    /// <b>Guards are not optional.</b> SQLite has affinity rather than types: <c>CAST('abc' AS INTEGER)</c>
    /// is <c>0</c>, not an error, so an unguarded move would turn unreadable text into a plausible-looking
    /// number. Each text source therefore carries a predicate selecting only the rows that survive a
    /// round-trip, and the rest are left in place and reported — the same shape as PostgreSQL's regex.
    /// </para>
    ///
    /// <para>
    /// <b>Known divergence from PostgreSQL, deliberate.</b> PostgreSQL guards with <c>~ '^-?[0-9]+$'</c>,
    /// which accepts <c>'007'</c> and stores 7. The round-trip check here rejects it, because SQLite has
    /// no regex operator without registering one on every connection. The difference only ever moves a
    /// row into the "left behind and reported" group, never into the "silently changed" one, so the
    /// stricter side is the safe side; registering a <c>regexp</c> function for exact parity was judged
    /// not worth a per-connection callback for leading zeros.
    /// </para>
    ///
    /// <para>
    /// Dates need no special handling: <c>_DateTimeOffset</c> holds a UTC Julian day as REAL precisely so
    /// that SQLite's own <c>strftime</c> / <c>julianday</c> read it directly (see SqliteJulian).
    /// </para>
    /// </summary>
    private static Conversion? BuildConversion(string source, string target) => (source, target) switch
    {
        // --- numeric and boolean: same storage domain, nothing can fail ---
        ("_Boolean", "_Long") => new("_Boolean", "1=1"),
        ("_Long", "_Boolean") => new("CASE WHEN _Long <> 0 THEN 1 ELSE 0 END", "1=1"),
        ("_Long", "_Double") => new("CAST(_Long AS REAL)", "1=1"),
        ("_Long", "_Numeric") => new("CAST(_Long AS REAL)", "1=1"),
        ("_Double", "_Long") => new("CAST(_Double AS INTEGER)", "1=1"),
        ("_Double", "_Numeric") => new("_Double", "1=1"),
        ("_Numeric", "_Long") => new("CAST(_Numeric AS INTEGER)", "1=1"),
        ("_Numeric", "_Double") => new("_Numeric", "1=1"),

        // --- to text: always representable ---
        ("_Boolean", "_String") => new("CASE WHEN _Boolean <> 0 THEN 'true' ELSE 'false' END", "1=1"),
        ("_Long", "_String") => new("CAST(_Long AS TEXT)", "1=1"),
        ("_Double", "_String") => new("CAST(_Double AS TEXT)", "1=1"),
        ("_Numeric", "_String") => new("CAST(_Numeric AS TEXT)", "1=1"),
        ("_Guid", "_String") => new("_Guid", "1=1"),

        // --- temporal: Julian REAL is what SQLite's own date functions consume ---
        ("_DateTimeOffset", "_String") => new("strftime('%Y-%m-%dT%H:%M:%fZ', _DateTimeOffset)", "1=1"),
        ("_DateTimeOffset", "_Long") => new("CAST(strftime('%s', _DateTimeOffset) AS INTEGER)", "1=1"),
        ("_Long", "_DateTimeOffset") => new("julianday(_Long, 'unixepoch')", "1=1"),

        // --- from text: only rows that read back unchanged ---
        ("_String", "_Long") => new(
            "CAST(_String AS INTEGER)",
            "CAST(CAST(_String AS INTEGER) AS TEXT) = _String"),
        ("_String", "_Double") => new(
            "CAST(_String AS REAL)",
            "CAST(CAST(_String AS REAL) AS TEXT) = _String OR CAST(CAST(_String AS INTEGER) AS TEXT) = _String"),
        ("_String", "_Numeric") => new(
            "CAST(_String AS REAL)",
            "CAST(CAST(_String AS REAL) AS TEXT) = _String OR CAST(CAST(_String AS INTEGER) AS TEXT) = _String"),
        ("_String", "_Boolean") => new(
            "CASE WHEN lower(_String) IN ('true','t','1','yes','y') THEN 1 ELSE 0 END",
            "lower(_String) IN ('true','t','1','yes','y','false','f','0','no','n')"),
        ("_String", "_Guid") => new(
            "_String",
            "_String GLOB '[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-" +
            "[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-" +
            "[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]-[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]" +
            "[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]'"),
        ("_String", "_DateTimeOffset") => new(
            "julianday(_String)",
            "julianday(_String) IS NOT NULL"),

        _ => null
    };

    /// <summary>Maps a REDB type name to its <c>_values</c> storage column (SQLite), or null for non-scalar types.</summary>
    private static string? GetValueColumn(string? typeName) => typeName?.Trim().ToLowerInvariant() switch
    {
        "string" or "text" or "mimetype" or "filepath" or "filename" => "_String",
        "long" or "int" or "short" or "byte" or "timespan" => "_Long",
        "double" or "float" => "_Double",
        "boolean" => "_Boolean",
        "datetime" or "datetimeoffset" or "dateonly" or "timeonly" => "_DateTimeOffset",
        "guid" => "_Guid",
        "bytearray" => "_ByteArray",
        "numeric" => "_Numeric",
        "listitem" => "_ListItem",
        "object" => "_Object",
        _ => null
    };
}
