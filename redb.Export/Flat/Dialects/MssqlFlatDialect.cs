using System.Data;

namespace redb.Export.Flat.Dialects;

/// <summary>SQL Server: rows go through <c>SqlBulkCopy</c>.</summary>
internal sealed class MssqlFlatDialect : IFlatDialect
{
    public string Quote(string identifier) => "[" + identifier.Replace("]", "]]") + "]";

    public string ColumnType(FlatStorageType type) => type switch
    {
        FlatStorageType.Int64 => "bigint",
        FlatStorageType.Int32 => "int",
        FlatStorageType.Int16 => "smallint",
        FlatStorageType.Byte => "tinyint",
        FlatStorageType.Double => "float",
        FlatStorageType.Single => "real",
        FlatStorageType.Decimal => "decimal(38, 18)",
        FlatStorageType.Boolean => "bit",
        FlatStorageType.Text => "nvarchar(max)",
        // Same bound redb keeps for _values._array_index: an index key cannot hold nvarchar(max).
        FlatStorageType.KeyText => "nvarchar(430)",
        FlatStorageType.Char => "nchar(1)",
        FlatStorageType.Guid => "uniqueidentifier",
        FlatStorageType.DateTime => "datetime2",
        FlatStorageType.DateTimeOffset => "datetimeoffset",
        FlatStorageType.Date => "date",
        FlatStorageType.Time => "time",
        // SQL Server has no interval type; time holds a span below 24 hours.
        FlatStorageType.Duration => "time",
        FlatStorageType.Bytes => "varbinary(max)",
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
    };

    public bool InlineForeignKeys => false;

    // SqlBulkCopy column mappings take the plain column name.
    public string BulkColumnName(string column) => column;

    public object ToDbValue(object value, FlatStorageType type) => value switch
    {
        DateOnly date => date.ToDateTime(TimeOnly.MinValue),
        TimeOnly time => time.ToTimeSpan(),
        char character => character.ToString(),
        _ => value
    };

    public IsolationLevel SnapshotIsolation => IsolationLevel.Snapshot;
}
