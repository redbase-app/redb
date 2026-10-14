using System.Data;
using System.Text;
using redb.Core.Utils;
using redb.Export.Flat.Dialects;
using redb.Export.Flat.Model;

namespace redb.Export.Flat;

/// <summary>Rows of one flat table waiting for the next bulk insert.</summary>
internal sealed class FlatTableBuffer
{
    public FlatTableBuffer(FlatTable table, IFlatDialect dialect)
    {
        Table = table;
        StorageTypes = table.Columns.Select(FlatStorageTypes.Of).ToArray();
        Data = new DataTable();
        foreach (var column in table.Columns)
            Data.Columns.Add(dialect.BulkColumnName(column.Name), typeof(object));
    }

    public FlatTable Table { get; }

    public FlatStorageType[] StorageTypes { get; }

    public DataTable Data { get; }
}

/// <summary>
/// Turns the <c>_values</c> rows of one object into rows of the scheme table and of every table
/// below it.
/// <para>
/// The layout it reads is the one <c>ObjectStorageProviderBase.SaveAsync</c> writes: a field outside
/// a collection is one row found by its structure under the row of its parent; a class has a base
/// row its fields hang from; a collection has a base row and, under it, one row per element with
/// the position or the key in <c>_array_index</c>; an element that is a class is itself the parent
/// of the rows of its fields.
/// </para>
/// </summary>
internal sealed class FlatRowAssembler
{
    // Parent of the rows that hang directly from the object.
    private const long RootParent = 0;

    private readonly IFlatDialect _dialect;
    private readonly IReadOnlyDictionary<string, FlatTableBuffer> _buffers;
    private readonly ILookup<string?, FlatTable> _tablesByOwner;

    public FlatRowAssembler(
        IReadOnlyList<FlatTable> schemeTables,
        IReadOnlyDictionary<string, FlatTableBuffer> buffers,
        IFlatDialect dialect)
    {
        _dialect = dialect;
        _buffers = buffers;
        _tablesByOwner = schemeTables.ToLookup(t => t.OwnerTable);
    }

    /// <summary>Appends the rows of one object to the buffers.</summary>
    public void Append(FlatTable schemeTable, long objectId, List<FlatSourceValue> values)
    {
        var index = values.ToLookup(v => (v.ArrayParentId ?? RootParent, v.StructureId));
        EmitRow(schemeTable, objectId, ownerRowId: null, RootParent, element: null, index);
    }

    private void EmitRow(
        FlatTable table,
        long rowId,
        long? ownerRowId,
        long parentValueId,
        FlatSourceValue? element,
        ILookup<(long Parent, long Structure), FlatSourceValue> index)
    {
        var buffer = _buffers[table.Name];
        var row = buffer.Data.NewRow();
        object? key = null;

        for (var i = 0; i < table.Columns.Count; i++)
        {
            var column = table.Columns[i];
            var storageType = buffer.StorageTypes[i];
            object? value;

            switch (column.Role)
            {
                case FlatColumnRole.PrimaryKey:
                    value = rowId;
                    break;

                case FlatColumnRole.Owner:
                    value = ownerRowId;
                    break;

                case FlatColumnRole.ArrayIndex:
                    value = int.Parse(element!.ArrayIndex!, System.Globalization.CultureInfo.InvariantCulture);
                    break;

                case FlatColumnRole.DictionaryKey:
                    key ??= DecodeKey(table, element!.ArrayIndex!);
                    value = KeyColumnValue(table, column, key, storageType);
                    break;

                default:
                    // The payload of a collection of scalars or references sits on the element row
                    // itself; everything else is a row of its own under the parent.
                    var source = table.Kind == FlatTableKind.Collection && table.ElementKind != FlatElementKind.Class
                        ? element
                        : index[(parentValueId, column.StructureId!.Value)].FirstOrDefault();
                    value = source == null ? null : FlatValueConverter.Read(source, column, storageType);
                    break;
            }

            row[i] = value == null ? DBNull.Value : _dialect.ToDbValue(value, storageType);
        }

        buffer.Data.Rows.Add(row);

        foreach (var child in _tablesByOwner[table.Name])
        {
            var baseRow = index[(parentValueId, child.StructureId!.Value)].FirstOrDefault();
            if (baseRow == null)
                continue;

            if (child.Kind == FlatTableKind.NestedClass)
            {
                EmitRow(child, baseRow.Id, rowId, baseRow.Id, element: null, index);
                continue;
            }

            foreach (var item in index[(baseRow.Id, child.StructureId.Value)])
                EmitRow(child, item.Id, rowId, item.Id, item, index);
        }
    }

    private static object DecodeKey(FlatTable table, string arrayIndex) => table.KeyKind switch
    {
        // A complex key is stored as Base64 of its JSON.
        FlatKeyKind.Json => Encoding.UTF8.GetString(Convert.FromBase64String(arrayIndex)),
        _ => RedbKeySerializer.DeserializeObject(arrayIndex, table.KeyType!)
             ?? throw new InvalidOperationException(
                 $"Dictionary key '{arrayIndex}' of table '{table.FullName}' cannot be read as {table.KeyType!.Name}.")
    };

    private static object KeyColumnValue(FlatTable table, FlatColumn column, object key, FlatStorageType storageType)
    {
        var value = column.KeyComponent == null
            ? key
            : table.KeyType!.GetField(column.KeyComponent)!.GetValue(key)!;

        // Enum keys are stored, and exported, by name.
        return storageType is FlatStorageType.Text or FlatStorageType.KeyText ? value.ToString()! : value;
    }
}

/// <summary>Reads the typed value of a column out of a <c>_values</c> row.</summary>
internal static class FlatValueConverter
{
    public static object? Read(FlatSourceValue source, FlatColumn column, FlatStorageType storageType)
    {
        switch (column.ValueSource)
        {
            case FlatValueSource.Long:
                if (source.Long is not { } number) return null;
                return storageType switch
                {
                    FlatStorageType.Int32 => (object)checked((int)number),
                    FlatStorageType.Int16 => checked((short)number),
                    FlatStorageType.Byte => checked((byte)number),
                    _ => number
                };

            case FlatValueSource.Double:
                if (source.Double is not { } real) return null;
                return storageType == FlatStorageType.Single ? (object)(float)real : real;

            case FlatValueSource.Numeric:
                return source.Numeric;

            case FlatValueSource.Boolean:
                return source.Boolean;

            case FlatValueSource.Guid:
                return source.Guid;

            case FlatValueSource.ByteArray:
                return source.ByteArray;

            case FlatValueSource.Object:
                return source.Object;

            case FlatValueSource.ListItem:
                return source.ListItem;

            case FlatValueSource.DateTimeOffset:
                if (source.DateTimeOffset is not { } moment) return null;
                return storageType switch
                {
                    // DateTime and DateOnly are stored as a UTC clock reading (DateTimeConverter.NormalizeForStorage).
                    FlatStorageType.DateTime => (object)moment.UtcDateTime,
                    FlatStorageType.Date => DateOnly.FromDateTime(moment.UtcDateTime),
                    _ => moment
                };

            case FlatValueSource.String:
                if (source.String is not { } text) return null;
                switch (storageType)
                {
                    case FlatStorageType.Time:
                        return RedbTemporalFormat.TryParseTimeOnly(text, out var time)
                            ? time
                            : throw new FormatException($"Value '{text}' of column '{column.Name}' is not a TimeOnly.");
                    case FlatStorageType.Duration:
                        return RedbTemporalFormat.TryParseTimeSpan(text, out var span)
                            ? span
                            : throw new FormatException($"Value '{text}' of column '{column.Name}' is not a TimeSpan.");
                    default:
                        return text;
                }

            default:
                throw new InvalidOperationException($"Column '{column.Name}' has no value source.");
        }
    }
}
