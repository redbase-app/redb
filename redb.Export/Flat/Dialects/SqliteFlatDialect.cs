using System.Data;
using System.Globalization;

namespace redb.Export.Flat.Dialects;

/// <summary>SQLite: rows go through a prepared parameterised <c>INSERT</c>.</summary>
internal sealed class SqliteFlatDialect : IFlatDialect
{
    public string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    public string ColumnType(FlatStorageType type) => type switch
    {
        FlatStorageType.Int64 or FlatStorageType.Int32 or FlatStorageType.Int16 or FlatStorageType.Byte => "INTEGER",
        FlatStorageType.Boolean => "INTEGER",
        FlatStorageType.Double or FlatStorageType.Single => "REAL",
        FlatStorageType.Bytes => "BLOB",
        // Decimals, GUIDs and every date and time form are kept as text: exact and readable.
        _ => "TEXT"
    };

    // SQLite cannot add a foreign key to an existing table.
    public bool InlineForeignKeys => true;

    public string BulkColumnName(string column) => Quote(column);

    public object ToDbValue(object value, FlatStorageType type) => value switch
    {
        bool flag => flag ? 1L : 0L,
        decimal number => number.ToString(CultureInfo.InvariantCulture),
        Guid guid => guid.ToString("D").ToUpperInvariant(),
        DateTime dateTime => dateTime.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture),
        DateTimeOffset dateTimeOffset => dateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFFzzz", CultureInfo.InvariantCulture),
        DateOnly date => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        TimeOnly time => time.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture),
        TimeSpan span => span.ToString("c", CultureInfo.InvariantCulture),
        char character => character.ToString(),
        _ => value
    };

    // The only level SQLite has; a read transaction sees one snapshot of the file.
    public IsolationLevel SnapshotIsolation => IsolationLevel.Serializable;
}
