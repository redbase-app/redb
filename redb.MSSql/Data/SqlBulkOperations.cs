using Microsoft.Data.SqlClient;
using redb.Core.Data;
using redb.Core.Models.Entities;
using System.Data;
using System.Text;

namespace redb.MSSql.Data;

/// <summary>
/// MS SQL Server implementation of IBulkOperations using SqlBulkCopy.
/// Much faster than individual INSERTs for large datasets.
/// </summary>
public class SqlBulkOperations : IBulkOperations
{
    private readonly IRedbConnection _db;
    
    /// <summary>
    /// Create bulk operations handler.
    /// Uses the same connection as the context for transaction consistency.
    /// </summary>
    public SqlBulkOperations(IRedbConnection db)
    {
        _db = db ?? throw new ArgumentNullException(nameof(db));
    }

    /// <summary>
    /// Bulk insert objects using SqlBulkCopy.
    /// </summary>
    public async Task BulkInsertObjectsAsync(IEnumerable<RedbObjectRow> objects)
    {
        var objectsList = objects.ToList();
        if (objectsList.Count == 0) return;
        
        await _db.ExecuteAtomicAsync(async () =>
        {
            var conn = (SqlConnection)await _db.GetUnderlyingConnectionAsync();
            var transaction = (_db.CurrentTransaction as SqlRedbTransaction)?.SqlTransaction;
            
            using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = "_objects",
                BatchSize = 5000
            };
            
            MapObjectColumns(bulkCopy);
            var dataTable = CreateObjectsDataTable(objectsList);
            await bulkCopy.WriteToServerAsync(dataTable);
        });
    }

    /// <summary>
    /// Bulk insert values using SqlBulkCopy.
    /// </summary>
    public async Task BulkInsertValuesAsync(IEnumerable<RedbValue> values)
    {
        var valuesList = values.ToList();
        if (valuesList.Count == 0) return;
        
        await _db.ExecuteAtomicAsync(async () =>
        {
            var conn = (SqlConnection)await _db.GetUnderlyingConnectionAsync();
            var transaction = (_db.CurrentTransaction as SqlRedbTransaction)?.SqlTransaction;
            
            using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = "_values",
                BatchSize = 10000
            };
            
            MapValueColumns(bulkCopy);
            var dataTable = CreateValuesDataTable(valuesList);
            await bulkCopy.WriteToServerAsync(dataTable);
        });
    }

    /// <summary>
    /// Bulk update objects using MERGE statement.
    /// Single round-trip for entire batch instead of N individual UPDATEs.
    /// </summary>
    public async Task BulkUpdateObjectsAsync(IEnumerable<RedbObjectRow> objects)
    {
        var objectsList = objects.ToList();
        if (objectsList.Count == 0) return;
        
        // MSSQL parameter limit ~2100, 20 columns per object = max 100 objects per batch
        const int batchSize = 100;
        
        foreach (var batch in objectsList.Chunk(batchSize))
        {
            await ExecuteObjectsMergeAsync(batch);
        }
    }
    
    /// <summary>
    /// Execute MERGE for a batch of objects.
    /// Uses explicit CAST for varbinary columns to avoid type inference errors.
    /// </summary>
    private async Task ExecuteObjectsMergeAsync(RedbObjectRow[] batch)
    {
        var sb = new StringBuilder();
        var parameters = new List<object?>();
        
        sb.AppendLine("MERGE INTO _objects AS target");
        sb.AppendLine("USING (VALUES");
        
        for (int i = 0; i < batch.Length; i++)
        {
            var obj = batch[i];
            var offset = i * 20;
            
            if (i > 0) sb.AppendLine(",");
            // Cast varbinary columns explicitly to avoid type inference errors
            sb.Append($"    (@p{offset}, @p{offset + 1}, @p{offset + 2}, @p{offset + 3}, @p{offset + 4}, ");
            sb.Append($"@p{offset + 5}, @p{offset + 6}, @p{offset + 7}, @p{offset + 8}, @p{offset + 9}, ");
            sb.Append($"@p{offset + 10}, @p{offset + 11}, @p{offset + 12}, @p{offset + 13}, @p{offset + 14}, ");
            sb.Append($"@p{offset + 15}, @p{offset + 16}, CAST(@p{offset + 17} AS VARBINARY(MAX)), @p{offset + 18}, @p{offset + 19})");
            
            parameters.Add(obj.Id);
            parameters.Add((object?)obj.IdParent ?? DBNull.Value);
            parameters.Add(obj.IdScheme);
            parameters.Add((object?)obj.Name ?? DBNull.Value);
            parameters.Add(obj.IdOwner);
            parameters.Add(obj.IdWhoChange);
            parameters.Add(obj.DateModify);
            parameters.Add((object?)obj.DateBegin ?? DBNull.Value);
            parameters.Add((object?)obj.DateComplete ?? DBNull.Value);
            parameters.Add((object?)obj.Key ?? DBNull.Value);
            parameters.Add((object?)obj.ValueLong ?? DBNull.Value);
            parameters.Add((object?)obj.ValueString ?? DBNull.Value);
            parameters.Add((object?)obj.ValueGuid ?? DBNull.Value);
            parameters.Add((object?)obj.ValueBool ?? DBNull.Value);
            parameters.Add((object?)obj.ValueDouble ?? DBNull.Value);
            parameters.Add((object?)obj.ValueNumeric ?? DBNull.Value);
            parameters.Add((object?)obj.ValueDatetime ?? DBNull.Value);
            parameters.Add((object?)obj.ValueBytes ?? DBNull.Value);
            parameters.Add((object?)obj.Note ?? DBNull.Value);
            parameters.Add((object?)obj.Hash ?? DBNull.Value);
        }
        
        sb.AppendLine();
        sb.AppendLine(") AS source(_id, _id_parent, _id_scheme, _name, _id_owner, _id_who_change,");
        sb.AppendLine("            _date_modify, _date_begin, _date_complete, _key, _value_long, _value_string,");
        sb.AppendLine("            _value_guid, _value_bool, _value_double, _value_numeric, _value_datetime,");
        sb.AppendLine("            _value_bytes, _note, _hash)");
        sb.AppendLine("ON target._id = source._id");
        sb.AppendLine("WHEN MATCHED THEN UPDATE SET");
        sb.AppendLine("    _id_parent = source._id_parent,");
        sb.AppendLine("    _id_scheme = source._id_scheme,");
        sb.AppendLine("    _name = source._name,");
        sb.AppendLine("    _id_owner = source._id_owner,");
        sb.AppendLine("    _id_who_change = source._id_who_change,");
        sb.AppendLine("    _date_modify = source._date_modify,");
        sb.AppendLine("    _date_begin = source._date_begin,");
        sb.AppendLine("    _date_complete = source._date_complete,");
        sb.AppendLine("    _key = source._key,");
        sb.AppendLine("    _value_long = source._value_long,");
        sb.AppendLine("    _value_string = source._value_string,");
        sb.AppendLine("    _value_guid = source._value_guid,");
        sb.AppendLine("    _value_bool = source._value_bool,");
        sb.AppendLine("    _value_double = source._value_double,");
        sb.AppendLine("    _value_numeric = source._value_numeric,");
        sb.AppendLine("    _value_datetime = source._value_datetime,");
        sb.AppendLine("    _value_bytes = source._value_bytes,");
        sb.AppendLine("    _note = source._note,");
        sb.AppendLine("    _hash = source._hash;");
        
        await _db.ExecuteAsync(sb.ToString(), parameters.ToArray());
    }

    /// <summary>
    /// Bulk update values using MERGE statement.
    /// Single round-trip for entire batch instead of N individual UPDATEs.
    /// </summary>
    public async Task BulkUpdateValuesAsync(IEnumerable<RedbValue> values)
    {
        var valuesList = values.ToList();
        if (valuesList.Count == 0) return;
        
        // MSSQL parameter limit ~2100, 13 columns per value = max 160 values per batch
        const int batchSize = 150;
        
        foreach (var batch in valuesList.Chunk(batchSize))
        {
            await ExecuteValuesMergeAsync(batch);
        }
    }
    
    /// <summary>
    /// Execute MERGE for a batch of values.
    /// Uses explicit CAST for varbinary columns to avoid type inference errors.
    /// </summary>
    private async Task ExecuteValuesMergeAsync(RedbValue[] batch)
    {
        var sb = new StringBuilder();
        var parameters = new List<object?>();
        
        sb.AppendLine("MERGE INTO _values AS target");
        sb.AppendLine("USING (VALUES");
        
        for (int i = 0; i < batch.Length; i++)
        {
            var val = batch[i];
            var offset = i * 13;
            
            if (i > 0) sb.AppendLine(",");
            // Cast varbinary column (_ByteArray at offset+7) explicitly
            sb.Append($"    (@p{offset}, @p{offset + 1}, @p{offset + 2}, @p{offset + 3}, @p{offset + 4}, ");
            sb.Append($"@p{offset + 5}, @p{offset + 6}, CAST(@p{offset + 7} AS VARBINARY(MAX)), @p{offset + 8}, @p{offset + 9}, ");
            sb.Append($"@p{offset + 10}, @p{offset + 11}, @p{offset + 12})");
            
            parameters.Add(val.Id);
            parameters.Add((object?)val.String ?? DBNull.Value);
            parameters.Add((object?)val.Long ?? DBNull.Value);
            parameters.Add((object?)val.Guid ?? DBNull.Value);
            parameters.Add((object?)val.Double ?? DBNull.Value);
            parameters.Add((object?)val.DateTimeOffset ?? DBNull.Value);
            parameters.Add((object?)val.Boolean ?? DBNull.Value);
            parameters.Add((object?)val.ByteArray ?? DBNull.Value);
            parameters.Add((object?)val.Numeric ?? DBNull.Value);
            parameters.Add((object?)val.ListItem ?? DBNull.Value);
            parameters.Add((object?)val.Object ?? DBNull.Value);
            parameters.Add((object?)val.ArrayParentId ?? DBNull.Value);
            parameters.Add((object?)val.ArrayIndex ?? DBNull.Value);
        }
        
        sb.AppendLine();
        sb.AppendLine(") AS source(_id, _String, _Long, _Guid, _Double, _DateTimeOffset, _Boolean,");
        sb.AppendLine("            _ByteArray, _Numeric, _ListItem, _Object, _array_parent_id, _array_index)");
        sb.AppendLine("ON target._id = source._id");
        sb.AppendLine("WHEN MATCHED THEN UPDATE SET");
        sb.AppendLine("    [_String] = source._String,");
        sb.AppendLine("    [_Long] = source._Long,");
        sb.AppendLine("    [_Guid] = source._Guid,");
        sb.AppendLine("    [_Double] = source._Double,");
        sb.AppendLine("    [_DateTimeOffset] = source._DateTimeOffset,");
        sb.AppendLine("    [_Boolean] = source._Boolean,");
        sb.AppendLine("    [_ByteArray] = source._ByteArray,");
        sb.AppendLine("    [_Numeric] = source._Numeric,");
        sb.AppendLine("    [_ListItem] = source._ListItem,");
        sb.AppendLine("    [_Object] = source._Object,");
        sb.AppendLine("    [_array_parent_id] = source._array_parent_id,");
        sb.AppendLine("    [_array_index] = source._array_index;");
        
        await _db.ExecuteAsync(sb.ToString(), parameters.ToArray());
    }

    /// <summary>
    /// Bulk delete objects by IDs.
    /// </summary>
    public async Task BulkDeleteObjectsAsync(IEnumerable<long> objectIds)
    {
        var ids = objectIds.ToArray();
        if (ids.Length == 0) return;
        
        // MSSQL uses STRING_SPLIT instead of array parameter
        var idList = string.Join(",", ids);
        await _db.ExecuteAsync(
            $"DELETE FROM _objects WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))", 
            idList);
    }

    /// <summary>
    /// Bulk delete values by IDs.
    /// </summary>
    public async Task BulkDeleteValuesAsync(IEnumerable<long> valueIds)
    {
        var ids = valueIds.ToArray();
        if (ids.Length == 0) return;
        
        var idList = string.Join(",", ids);
        await _db.ExecuteAsync(
            $"DELETE FROM _values WHERE _id IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))", 
            idList);
    }

    /// <summary>
    /// Bulk delete values by object IDs.
    /// </summary>
    public async Task BulkDeleteValuesByObjectIdsAsync(IEnumerable<long> objectIds)
    {
        var ids = objectIds.ToArray();
        if (ids.Length == 0) return;
        
        var idList = string.Join(",", ids);
        await _db.ExecuteAsync(
            $"DELETE FROM _values WHERE _id_object IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))", 
            idList);
    }
    
    /// <summary>
    /// Bulk delete values by ListItem IDs.
    /// </summary>
    public async Task BulkDeleteValuesByListItemIdsAsync(IEnumerable<long> listItemIds)
    {
        var ids = listItemIds.ToArray();
        if (ids.Length == 0) return;
        
        var idList = string.Join(",", ids);
        await _db.ExecuteAsync(
            $"DELETE FROM _values WHERE _ListItem IN (SELECT CAST(value AS BIGINT) FROM STRING_SPLIT(@p0, ','))", 
            idList);
    }

    // === COLUMN MAPPINGS ===
    
    private static void MapObjectColumns(SqlBulkCopy bulkCopy)
    {
        bulkCopy.ColumnMappings.Add("_id", "_id");
        bulkCopy.ColumnMappings.Add("_id_parent", "_id_parent");
        bulkCopy.ColumnMappings.Add("_id_scheme", "_id_scheme");
        bulkCopy.ColumnMappings.Add("_name", "_name");
        bulkCopy.ColumnMappings.Add("_id_owner", "_id_owner");
        bulkCopy.ColumnMappings.Add("_id_who_change", "_id_who_change");
        bulkCopy.ColumnMappings.Add("_date_create", "_date_create");
        bulkCopy.ColumnMappings.Add("_date_modify", "_date_modify");
        bulkCopy.ColumnMappings.Add("_date_begin", "_date_begin");
        bulkCopy.ColumnMappings.Add("_date_complete", "_date_complete");
        bulkCopy.ColumnMappings.Add("_key", "_key");
        bulkCopy.ColumnMappings.Add("_value_long", "_value_long");
        bulkCopy.ColumnMappings.Add("_value_string", "_value_string");
        bulkCopy.ColumnMappings.Add("_value_guid", "_value_guid");
        bulkCopy.ColumnMappings.Add("_value_bool", "_value_bool");
        bulkCopy.ColumnMappings.Add("_value_double", "_value_double");
        bulkCopy.ColumnMappings.Add("_value_numeric", "_value_numeric");
        bulkCopy.ColumnMappings.Add("_value_datetime", "_value_datetime");
        bulkCopy.ColumnMappings.Add("_value_bytes", "_value_bytes");
        bulkCopy.ColumnMappings.Add("_note", "_note");
        bulkCopy.ColumnMappings.Add("_hash", "_hash");
    }
    
    private static void MapValueColumns(SqlBulkCopy bulkCopy)
    {
        bulkCopy.ColumnMappings.Add("_id", "_id");
        bulkCopy.ColumnMappings.Add("_id_structure", "_id_structure");
        bulkCopy.ColumnMappings.Add("_id_object", "_id_object");
        bulkCopy.ColumnMappings.Add("_String", "_String");
        bulkCopy.ColumnMappings.Add("_Long", "_Long");
        bulkCopy.ColumnMappings.Add("_Guid", "_Guid");
        bulkCopy.ColumnMappings.Add("_Double", "_Double");
        bulkCopy.ColumnMappings.Add("_DateTimeOffset", "_DateTimeOffset");
        bulkCopy.ColumnMappings.Add("_Boolean", "_Boolean");
        bulkCopy.ColumnMappings.Add("_ByteArray", "_ByteArray");
        bulkCopy.ColumnMappings.Add("_Numeric", "_Numeric");
        bulkCopy.ColumnMappings.Add("_ListItem", "_ListItem");
        bulkCopy.ColumnMappings.Add("_Object", "_Object");
        bulkCopy.ColumnMappings.Add("_array_parent_id", "_array_parent_id");
        bulkCopy.ColumnMappings.Add("_array_index", "_array_index");
    }

    // === DATA TABLE CREATION ===
    
    private static DataTable CreateObjectsDataTable(List<RedbObjectRow> objects)
    {
        var dt = new DataTable("_objects");
        
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_parent", typeof(long));
        dt.Columns.Add("_id_scheme", typeof(long));
        dt.Columns.Add("_name", typeof(string));
        dt.Columns.Add("_id_owner", typeof(long));
        dt.Columns.Add("_id_who_change", typeof(long));
        dt.Columns.Add("_date_create", typeof(DateTimeOffset));
        dt.Columns.Add("_date_modify", typeof(DateTimeOffset));
        dt.Columns.Add("_date_begin", typeof(DateTimeOffset));
        dt.Columns.Add("_date_complete", typeof(DateTimeOffset));
        dt.Columns.Add("_key", typeof(long));
        dt.Columns.Add("_value_long", typeof(long));
        dt.Columns.Add("_value_string", typeof(string));
        dt.Columns.Add("_value_guid", typeof(Guid));
        dt.Columns.Add("_value_bool", typeof(bool));
        dt.Columns.Add("_value_double", typeof(double));
        dt.Columns.Add("_value_numeric", typeof(decimal));
        dt.Columns.Add("_value_datetime", typeof(DateTimeOffset));
        dt.Columns.Add("_value_bytes", typeof(byte[]));
        dt.Columns.Add("_note", typeof(string));
        dt.Columns.Add("_hash", typeof(Guid));
        
        foreach (var obj in objects)
        {
            dt.Rows.Add(
                obj.Id,
                (object?)obj.IdParent ?? DBNull.Value,
                obj.IdScheme,
                (object?)obj.Name ?? DBNull.Value,
                obj.IdOwner,
                obj.IdWhoChange,
                obj.DateCreate,
                obj.DateModify,
                (object?)obj.DateBegin ?? DBNull.Value,
                (object?)obj.DateComplete ?? DBNull.Value,
                (object?)obj.Key ?? DBNull.Value,
                (object?)obj.ValueLong ?? DBNull.Value,
                (object?)obj.ValueString ?? DBNull.Value,
                (object?)obj.ValueGuid ?? DBNull.Value,
                (object?)obj.ValueBool ?? DBNull.Value,
                (object?)obj.ValueDouble ?? DBNull.Value,
                (object?)obj.ValueNumeric ?? DBNull.Value,
                (object?)obj.ValueDatetime ?? DBNull.Value,
                (object?)obj.ValueBytes ?? DBNull.Value,
                (object?)obj.Note ?? DBNull.Value,
                (object?)obj.Hash ?? DBNull.Value
            );
        }
        
        return dt;
    }
    
    private static DataTable CreateValuesDataTable(List<RedbValue> values)
    {
        var dt = new DataTable("_values");
        
        dt.Columns.Add("_id", typeof(long));
        dt.Columns.Add("_id_structure", typeof(long));
        dt.Columns.Add("_id_object", typeof(long));
        dt.Columns.Add("_String", typeof(string));
        dt.Columns.Add("_Long", typeof(long));
        dt.Columns.Add("_Guid", typeof(Guid));
        dt.Columns.Add("_Double", typeof(double));
        dt.Columns.Add("_DateTimeOffset", typeof(DateTimeOffset));
        dt.Columns.Add("_Boolean", typeof(bool));
        dt.Columns.Add("_ByteArray", typeof(byte[]));
        dt.Columns.Add("_Numeric", typeof(decimal));
        dt.Columns.Add("_ListItem", typeof(long));
        dt.Columns.Add("_Object", typeof(long));
        dt.Columns.Add("_array_parent_id", typeof(long));
        dt.Columns.Add("_array_index", typeof(string));
        
        foreach (var val in values)
        {
            dt.Rows.Add(
                val.Id,
                val.IdStructure,
                val.IdObject,
                (object?)val.String ?? DBNull.Value,
                (object?)val.Long ?? DBNull.Value,
                (object?)val.Guid ?? DBNull.Value,
                (object?)val.Double ?? DBNull.Value,
                (object?)val.DateTimeOffset ?? DBNull.Value,
                (object?)val.Boolean ?? DBNull.Value,
                (object?)val.ByteArray ?? DBNull.Value,
                (object?)val.Numeric ?? DBNull.Value,
                (object?)val.ListItem ?? DBNull.Value,
                (object?)val.Object ?? DBNull.Value,
                (object?)val.ArrayParentId ?? DBNull.Value,
                (object?)val.ArrayIndex ?? DBNull.Value
            );
        }
        
        return dt;
    }
}

