using redb.Export.Flat.Model;

namespace redb.Export.Flat;

/// <summary>
/// Dialect-independent type of a flat column. Each dialect maps it to a column type and to the form
/// its bulk insert accepts.
/// </summary>
public enum FlatStorageType
{
    /// <summary><see cref="long"/>.</summary>
    Int64,
    /// <summary><see cref="int"/>.</summary>
    Int32,
    /// <summary><see cref="short"/>.</summary>
    Int16,
    /// <summary><see cref="byte"/>.</summary>
    Byte,
    /// <summary><see cref="double"/>.</summary>
    Double,
    /// <summary><see cref="float"/>.</summary>
    Single,
    /// <summary><see cref="decimal"/>.</summary>
    Decimal,
    /// <summary><see cref="bool"/>.</summary>
    Boolean,
    /// <summary>Unbounded text: strings, enum names and every type scheme sync stores as text.</summary>
    Text,
    /// <summary>Text that takes part in a unique constraint (a dictionary key).</summary>
    KeyText,
    /// <summary><see cref="char"/>.</summary>
    Char,
    /// <summary><see cref="System.Guid"/>.</summary>
    Guid,
    /// <summary><see cref="System.DateTime"/>, a UTC reading.</summary>
    DateTime,
    /// <summary><see cref="System.DateTimeOffset"/>.</summary>
    DateTimeOffset,
    /// <summary><see cref="DateOnly"/>.</summary>
    Date,
    /// <summary><see cref="TimeOnly"/>.</summary>
    Time,
    /// <summary><see cref="TimeSpan"/>.</summary>
    Duration,
    /// <summary><c>byte[]</c>.</summary>
    Bytes
}

/// <summary>Chooses the <see cref="FlatStorageType"/> of a column from its CLR type.</summary>
public static class FlatStorageTypes
{
    /// <summary>Storage type of <paramref name="column"/>.</summary>
    public static FlatStorageType Of(FlatColumn column)
    {
        var type = Nullable.GetUnderlyingType(column.ClrType) ?? column.ClrType;

        if (type == typeof(long)) return FlatStorageType.Int64;
        if (type == typeof(int)) return FlatStorageType.Int32;
        if (type == typeof(short)) return FlatStorageType.Int16;
        if (type == typeof(byte)) return FlatStorageType.Byte;
        if (type == typeof(double)) return FlatStorageType.Double;
        if (type == typeof(float)) return FlatStorageType.Single;
        if (type == typeof(decimal)) return FlatStorageType.Decimal;
        if (type == typeof(bool)) return FlatStorageType.Boolean;
        if (type == typeof(char)) return FlatStorageType.Char;
        if (type == typeof(Guid)) return FlatStorageType.Guid;
        if (type == typeof(DateTime)) return FlatStorageType.DateTime;
        if (type == typeof(DateTimeOffset)) return FlatStorageType.DateTimeOffset;
        if (type == typeof(DateOnly)) return FlatStorageType.Date;
        if (type == typeof(TimeOnly)) return FlatStorageType.Time;
        if (type == typeof(TimeSpan)) return FlatStorageType.Duration;
        if (type == typeof(byte[])) return FlatStorageType.Bytes;

        return column.Role == FlatColumnRole.DictionaryKey ? FlatStorageType.KeyText : FlatStorageType.Text;
    }
}
