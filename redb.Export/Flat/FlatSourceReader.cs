using System.Data.Common;
using System.Runtime.CompilerServices;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Export.Flat.Model;
using redb.Export.Providers;

namespace redb.Export.Flat;

/// <summary>One row of <c>_values</c> as the flat export reads it (hashes and unique keys left out).</summary>
internal sealed class FlatSourceValue
{
    public long Id { get; init; }
    public long StructureId { get; init; }
    public long? ArrayParentId { get; init; }
    public string? ArrayIndex { get; init; }
    public string? String { get; init; }
    public long? Long { get; init; }
    public Guid? Guid { get; init; }
    public double? Double { get; init; }
    public DateTimeOffset? DateTimeOffset { get; init; }
    public bool? Boolean { get; init; }
    public byte[]? ByteArray { get; init; }
    public decimal? Numeric { get; init; }
    public long? ListItem { get; init; }
    public long? Object { get; init; }
}

/// <summary>
/// Reads the redb tables of the source database. Every command runs inside the one transaction the
/// reader was given, so the whole export sees a single snapshot. Readers are consumed one at a
/// time: a connection carries one open reader.
/// </summary>
internal sealed class FlatSourceReader
{
    private readonly IDataProvider _provider;
    private readonly DbTransaction _transaction;

    public FlatSourceReader(IDataProvider provider, DbTransaction transaction)
    {
        _provider = provider;
        _transaction = transaction;
    }

    /// <summary>Id of the scheme stored under <paramref name="schemeName"/>, or null when there is none.</summary>
    public async Task<long?> FindSchemeIdAsync(string schemeName, CancellationToken ct)
    {
        await using var command = Command("SELECT _id FROM _schemes WHERE _name = @name");
        AddParameter(command, "@name", schemeName);

        var result = await command.ExecuteScalarAsync(ct);
        return result is null or DBNull ? null : Convert.ToInt64(result);
    }

    /// <summary>The structures of a scheme, with the columns the structure tree is built from.</summary>
    public async Task<List<IRedbStructure>> ReadStructuresAsync(long schemeId, CancellationToken ct)
    {
        await using var command = Command(
            "SELECT _id, _id_parent, _name, _id_type, _collection_type, _key_type, _order " +
            "FROM _structures WHERE _id_scheme = @scheme");
        AddParameter(command, "@scheme", schemeId);

        var structures = new List<IRedbStructure>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            structures.Add(new RedbStructure
            {
                Id = reader.GetInt64(0),
                IdParent = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                IdScheme = schemeId,
                Name = reader.GetString(2),
                IdType = reader.GetInt64(3),
                CollectionType = reader.IsDBNull(4) ? null : reader.GetInt64(4),
                KeyType = reader.IsDBNull(5) ? null : reader.GetInt64(5),
                Order = reader.IsDBNull(6) ? null : reader.GetInt64(6)
            });
        }

        return structures;
    }

    /// <summary>
    /// Streams a redb table into the shape of its flat copy: one array per row, a value per column of
    /// <paramref name="table"/>, read from the redb column named by <see cref="FlatColumn.SourceColumn"/>.
    /// </summary>
    public async IAsyncEnumerable<object?[]> ReadServiceRowsAsync(
        string sourceTable,
        FlatTable table,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var columns = table.Columns;
        var list = string.Join(", ", columns.Select(c => c.SourceColumn));

        await using var command = Command($"SELECT {list} FROM {sourceTable} ORDER BY _id");
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var row = new object?[columns.Count];
            for (var i = 0; i < columns.Count; i++)
                row[i] = ReadTyped(reader, i, columns[i].ClrType);
            yield return row;
        }
    }

    /// <summary>Ids of every object of a scheme, ascending.</summary>
    public async Task<List<long>> ReadSchemeObjectIdsAsync(long schemeId, CancellationToken ct)
    {
        await using var command = Command("SELECT _id FROM _objects WHERE _id_scheme = @scheme ORDER BY _id");
        AddParameter(command, "@scheme", schemeId);

        var ids = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            ids.Add(reader.GetInt64(0));

        return ids;
    }

    /// <summary>
    /// Streams the objects of a scheme whose id lies in <c>[fromId, toId]</c>, in id order, each with
    /// all of its <c>_values</c> rows. An object without rows is still returned: it has a row in the
    /// scheme table.
    /// <para>
    /// The range keeps each read small. One statement over the whole scheme makes the server join and
    /// sort every <c>_values</c> row of the scheme before it returns the first one: minutes of
    /// silence on a few million rows, and a command timeout beyond that.
    /// </para>
    /// </summary>
    public async IAsyncEnumerable<(long ObjectId, List<FlatSourceValue> Values)> ReadSchemeObjectsAsync(
        long schemeId,
        long fromId,
        long toId,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // The range is stated on both sides of the join so that _values is reached through its
        // _id_object index instead of being scanned.
        await using var command = Command(
            "SELECT o._id, v._id, v._id_structure, v._array_parent_id, v._array_index, " +
            "v._String, v._Long, v._Guid, v._Double, v._DateTimeOffset, v._Boolean, v._ByteArray, " +
            "v._Numeric, v._ListItem, v._Object " +
            "FROM _objects o LEFT JOIN _values v ON v._id_object = o._id " +
            "AND v._id_object >= @fromId AND v._id_object <= @toId " +
            "WHERE o._id_scheme = @scheme AND o._id >= @fromId AND o._id <= @toId ORDER BY o._id");
        AddParameter(command, "@scheme", schemeId);
        AddParameter(command, "@fromId", fromId);
        AddParameter(command, "@toId", toId);

        long? currentId = null;
        var values = new List<FlatSourceValue>();

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var objectId = reader.GetInt64(0);
            if (currentId != objectId)
            {
                if (currentId.HasValue)
                    yield return (currentId.Value, values);

                currentId = objectId;
                values = [];
            }

            if (reader.IsDBNull(1))
                continue;

            // Same typed reads as ExportService.ExportValuesAsync.
            values.Add(new FlatSourceValue
            {
                Id = reader.GetInt64(1),
                StructureId = reader.GetInt64(2),
                ArrayParentId = reader.IsDBNull(3) ? null : reader.GetInt64(3),
                ArrayIndex = reader.IsDBNull(4) ? null : reader.GetString(4),
                String = reader.IsDBNull(5) ? null : reader.GetString(5),
                Long = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                Guid = reader.IsDBNull(7) ? null : reader.GetGuid(7),
                Double = reader.IsDBNull(8) ? null : reader.GetDouble(8),
                DateTimeOffset = reader.IsDBNull(9) ? null : _provider.ReadDateTimeOffset(reader, 9),
                Boolean = reader.IsDBNull(10) ? null : reader.GetBoolean(10),
                ByteArray = reader.IsDBNull(11) ? null : (byte[])reader.GetValue(11),
                Numeric = reader.IsDBNull(12) ? null : reader.GetDecimal(12),
                ListItem = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                Object = reader.IsDBNull(14) ? null : reader.GetInt64(14)
            });
        }

        if (currentId.HasValue)
            yield return (currentId.Value, values);
    }

    private object? ReadTyped(DbDataReader reader, int ordinal, Type type)
    {
        if (reader.IsDBNull(ordinal))
            return null;

        if (type == typeof(long)) return reader.GetInt64(ordinal);
        if (type == typeof(string)) return reader.GetString(ordinal);
        if (type == typeof(Guid)) return reader.GetGuid(ordinal);
        if (type == typeof(bool)) return reader.GetBoolean(ordinal);
        if (type == typeof(double)) return reader.GetDouble(ordinal);
        if (type == typeof(decimal)) return reader.GetDecimal(ordinal);
        if (type == typeof(DateTimeOffset)) return _provider.ReadDateTimeOffset(reader, ordinal);
        if (type == typeof(byte[])) return (byte[])reader.GetValue(ordinal);

        throw new NotSupportedException($"A service column of type '{type.Name}' cannot be read.");
    }

    private DbCommand Command(string sql)
    {
        var command = _provider.Connection.CreateCommand();
        command.Transaction = _transaction;
        command.CommandText = sql;
        return command;
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
