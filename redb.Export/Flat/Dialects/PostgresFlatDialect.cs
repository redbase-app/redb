using System.Data;
using System.Globalization;

namespace redb.Export.Flat.Dialects;

/// <summary>PostgreSQL: rows go through <c>COPY ... FROM STDIN</c> in TEXT format.</summary>
internal sealed class PostgresFlatDialect : IFlatDialect
{
    public string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";

    public string ColumnType(FlatStorageType type) => type switch
    {
        FlatStorageType.Int64 => "bigint",
        FlatStorageType.Int32 => "integer",
        FlatStorageType.Int16 => "smallint",
        FlatStorageType.Byte => "smallint",
        FlatStorageType.Double => "double precision",
        FlatStorageType.Single => "real",
        FlatStorageType.Decimal => "numeric(38, 18)",
        FlatStorageType.Boolean => "boolean",
        FlatStorageType.Text => "text",
        FlatStorageType.KeyText => "text",
        FlatStorageType.Char => "character(1)",
        FlatStorageType.Guid => "uuid",
        FlatStorageType.DateTime => "timestamp with time zone",
        FlatStorageType.DateTimeOffset => "timestamp with time zone",
        FlatStorageType.Date => "date",
        FlatStorageType.Time => "time",
        FlatStorageType.Duration => "interval",
        FlatStorageType.Bytes => "bytea",
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
    };

    public bool InlineForeignKeys => false;

    // COPY takes the column list as SQL text.
    public string BulkColumnName(string column) => Quote(column);

    public object ToDbValue(object value, FlatStorageType type) => value switch
    {
        // A zone-less timestamp would be read in the session time zone; send the UTC offset with it.
        DateTime dateTime => new DateTimeOffset(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc)),
        DateOnly date => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        TimeOnly time => time.ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        // interval input does not read the .NET "d.hh:mm:ss" form; microseconds are exact and unambiguous.
        TimeSpan span => (span.Ticks / 10).ToString(CultureInfo.InvariantCulture) + " microseconds",
        char character => character.ToString(),
        long or int or short or byte => Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
        _ => value
    };

    public IsolationLevel SnapshotIsolation => IsolationLevel.RepeatableRead;
}
