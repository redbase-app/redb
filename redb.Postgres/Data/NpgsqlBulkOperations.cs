using Npgsql;
using NpgsqlTypes;
using redb.Core.Data;
using redb.Core.Models.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace redb.Postgres.Data
{
    /// <summary>
    /// PostgreSQL implementation of IBulkOperations using COPY protocol.
    /// Much faster than individual INSERTs for large datasets.
    /// </summary>
    public class NpgsqlBulkOperations : IBulkOperations
    {
        private readonly IRedbConnection _db;
        
        /// <summary>
        /// Create bulk operations handler.
        /// Uses the same connection as the context for transaction consistency.
        /// </summary>
        public NpgsqlBulkOperations(IRedbConnection db)
        {
            _db = db ?? throw new ArgumentNullException(nameof(db));
        }
        
        // === DATE/TIME CONVERSION HELPERS ===
        
        /// <summary>
        /// Convert DateTimeOffset to UTC (required by Npgsql for timestamptz).
        /// </summary>
        private static DateTimeOffset ToUtc(DateTimeOffset value) => value.ToUniversalTime();
        
        /// <summary>
        /// Convert nullable DateTimeOffset to UTC.
        /// </summary>
        private static DateTimeOffset? ToUtc(DateTimeOffset? value) => value?.ToUniversalTime();

        /// <summary>
        /// Bulk insert objects using COPY protocol.
        /// </summary>
        public async Task BulkInsertObjectsAsync(IEnumerable<RedbObjectRow> objects)
        {
            var objectsList = objects.ToList();
            if (!objectsList.Any()) return;
            
            await _db.ExecuteAtomicAsync(async () =>
            {
                var conn = (NpgsqlConnection)await _db.GetUnderlyingConnectionAsync();
                await using var writer = await conn.BeginBinaryImportAsync(
                    "COPY _objects (_id, _id_parent, _id_scheme, _name, _id_owner, _id_who_change, " +
                    "_date_create, _date_modify, _date_begin, _date_complete, _key, " +
                    "_value_long, _value_string, _value_guid, _value_bool, _value_double, " +
                    "_value_numeric, _value_datetime, _value_bytes, _note, _hash) FROM STDIN (FORMAT BINARY)");
                
                foreach (var obj in objectsList)
                {
                    await writer.StartRowAsync();
                    await writer.WriteAsync(obj.Id, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, obj.IdParent, NpgsqlDbType.Bigint);
                    await writer.WriteAsync(obj.IdScheme, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, obj.Name, NpgsqlDbType.Text);
                    await writer.WriteAsync(obj.IdOwner, NpgsqlDbType.Bigint);
                    await writer.WriteAsync(obj.IdWhoChange, NpgsqlDbType.Bigint);
                    await writer.WriteAsync(ToUtc(obj.DateCreate), NpgsqlDbType.TimestampTz);
                    await writer.WriteAsync(ToUtc(obj.DateModify), NpgsqlDbType.TimestampTz);
                    await WriteNullableDateTimeAsync(writer, obj.DateBegin);
                    await WriteNullableDateTimeAsync(writer, obj.DateComplete);
                    await WriteNullableAsync(writer, obj.Key, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, obj.ValueLong, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, obj.ValueString, NpgsqlDbType.Text);
                    await WriteNullableAsync(writer, obj.ValueGuid, NpgsqlDbType.Uuid);
                    await WriteNullableAsync(writer, obj.ValueBool, NpgsqlDbType.Boolean);
                    await WriteNullableAsync(writer, obj.ValueDouble, NpgsqlDbType.Double);
                    await WriteNullableAsync(writer, obj.ValueNumeric, NpgsqlDbType.Numeric);
                    await WriteNullableDateTimeAsync(writer, obj.ValueDatetime);
                    await WriteNullableAsync(writer, obj.ValueBytes, NpgsqlDbType.Bytea);
                    await WriteNullableAsync(writer, obj.Note, NpgsqlDbType.Text);
                    await WriteNullableAsync(writer, obj.Hash, NpgsqlDbType.Uuid);
                }
                
                await writer.CompleteAsync();
            });
        }

        /// <summary>
        /// Bulk insert values using COPY protocol.
        /// </summary>
        public async Task BulkInsertValuesAsync(IEnumerable<RedbValue> values)
        {
            var valuesList = values.ToList();
            if (!valuesList.Any()) return;
            
            await _db.ExecuteAtomicAsync(async () =>
            {
                var conn = (NpgsqlConnection)await _db.GetUnderlyingConnectionAsync();
                await using var writer = await conn.BeginBinaryImportAsync(
                    "COPY _values (_id, _id_structure, _id_object, _String, _Long, _Guid, " +
                    "_Double, _DateTimeOffset, _Boolean, _ByteArray, _Numeric, _ListItem, _Object, " +
                    "_array_parent_id, _array_index) FROM STDIN (FORMAT BINARY)");
                
                foreach (var val in valuesList)
                {
                    await writer.StartRowAsync();
                    await writer.WriteAsync(val.Id, NpgsqlDbType.Bigint);
                    await writer.WriteAsync(val.IdStructure, NpgsqlDbType.Bigint);
                    await writer.WriteAsync(val.IdObject, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, val.String, NpgsqlDbType.Text);
                    await WriteNullableAsync(writer, val.Long, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, val.Guid, NpgsqlDbType.Uuid);
                    await WriteNullableAsync(writer, val.Double, NpgsqlDbType.Double);
                    await WriteNullableDateTimeAsync(writer, val.DateTimeOffset);
                    await WriteNullableAsync(writer, val.Boolean, NpgsqlDbType.Boolean);
                    await WriteNullableAsync(writer, val.ByteArray, NpgsqlDbType.Bytea);
                    await WriteNullableAsync(writer, val.Numeric, NpgsqlDbType.Numeric);
                    await WriteNullableAsync(writer, val.ListItem, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, val.Object, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, val.ArrayParentId, NpgsqlDbType.Bigint);
                    await WriteNullableAsync(writer, val.ArrayIndex, NpgsqlDbType.Text);
                }
                
                await writer.CompleteAsync();
            });
        }

        /// <summary>
        /// Bulk update objects using UPDATE FROM VALUES.
        /// Single round-trip for entire batch instead of N individual UPDATEs.
        /// </summary>
        public async Task BulkUpdateObjectsAsync(IEnumerable<RedbObjectRow> objects)
        {
            var objectsList = objects.ToList();
            if (!objectsList.Any()) return;
            
            // PostgreSQL has no hard parameter limit, but use reasonable batch size
            const int batchSize = 500;
            
            foreach (var batch in objectsList.Chunk(batchSize))
            {
                await ExecuteObjectsUpdateAsync(batch);
            }
        }

        /// <summary>
        /// Execute UPDATE FROM VALUES for a batch of objects.
        /// </summary>
        private async Task ExecuteObjectsUpdateAsync(RedbObjectRow[] batch)
        {
            var sb = new StringBuilder();
            var parameters = new List<object?>();
            
            sb.AppendLine("UPDATE _objects AS t SET");
            sb.AppendLine("    _id_parent = s._id_parent,");
            sb.AppendLine("    _id_scheme = s._id_scheme,");
            sb.AppendLine("    _name = s._name,");
            sb.AppendLine("    _id_owner = s._id_owner,");
            sb.AppendLine("    _id_who_change = s._id_who_change,");
            sb.AppendLine("    _date_modify = s._date_modify,");
            sb.AppendLine("    _date_begin = s._date_begin,");
            sb.AppendLine("    _date_complete = s._date_complete,");
            sb.AppendLine("    _key = s._key,");
            sb.AppendLine("    _value_long = s._value_long,");
            sb.AppendLine("    _value_string = s._value_string,");
            sb.AppendLine("    _value_guid = s._value_guid,");
            sb.AppendLine("    _value_bool = s._value_bool,");
            sb.AppendLine("    _value_double = s._value_double,");
            sb.AppendLine("    _value_numeric = s._value_numeric,");
            sb.AppendLine("    _value_datetime = s._value_datetime,");
            sb.AppendLine("    _value_bytes = s._value_bytes,");
            sb.AppendLine("    _note = s._note,");
            sb.AppendLine("    _hash = s._hash");
            sb.AppendLine("FROM (VALUES");
            
            for (int i = 0; i < batch.Length; i++)
            {
                var obj = batch[i];
                var offset = i * 20 + 1; // PostgreSQL uses $1, $2, etc.
                
                if (i > 0) sb.AppendLine(",");
                sb.Append($"    (${offset}::bigint, ${offset + 1}::bigint, ${offset + 2}::bigint, ${offset + 3}::text, ${offset + 4}::bigint, ");
                sb.Append($"${offset + 5}::bigint, ${offset + 6}::timestamptz, ${offset + 7}::timestamptz, ${offset + 8}::timestamptz, ");
                sb.Append($"${offset + 9}::bigint, ${offset + 10}::bigint, ${offset + 11}::text, ${offset + 12}::uuid, ");
                sb.Append($"${offset + 13}::boolean, ${offset + 14}::double precision, ${offset + 15}::numeric, ");
                sb.Append($"${offset + 16}::timestamptz, ${offset + 17}::bytea, ${offset + 18}::text, ${offset + 19}::uuid)");
                
                parameters.Add(obj.Id);
                parameters.Add(obj.IdParent);
                parameters.Add(obj.IdScheme);
                parameters.Add(obj.Name);
                parameters.Add(obj.IdOwner);
                parameters.Add(obj.IdWhoChange);
                parameters.Add(obj.DateModify);
                parameters.Add(obj.DateBegin);
                parameters.Add(obj.DateComplete);
                parameters.Add(obj.Key);
                parameters.Add(obj.ValueLong);
                parameters.Add(obj.ValueString);
                parameters.Add(obj.ValueGuid);
                parameters.Add(obj.ValueBool);
                parameters.Add(obj.ValueDouble);
                parameters.Add(obj.ValueNumeric);
                parameters.Add(obj.ValueDatetime);
                parameters.Add(obj.ValueBytes);
                parameters.Add(obj.Note);
                parameters.Add(obj.Hash);
            }
            
            sb.AppendLine();
            sb.AppendLine(") AS s(_id, _id_parent, _id_scheme, _name, _id_owner, _id_who_change,");
            sb.AppendLine("       _date_modify, _date_begin, _date_complete, _key, _value_long, _value_string,");
            sb.AppendLine("       _value_guid, _value_bool, _value_double, _value_numeric, _value_datetime,");
            sb.AppendLine("       _value_bytes, _note, _hash)");
            sb.AppendLine("WHERE t._id = s._id");
            
            await _db.ExecuteAsync(sb.ToString(), parameters.ToArray());
        }

        /// <summary>
        /// Bulk update values using UPDATE FROM VALUES.
        /// Single round-trip for entire batch instead of N individual UPDATEs.
        /// </summary>
        public async Task BulkUpdateValuesAsync(IEnumerable<RedbValue> values)
        {
            var valuesList = values.ToList();
            if (!valuesList.Any()) return;
            
            // PostgreSQL has no hard parameter limit, but use reasonable batch size
            const int batchSize = 500;
            
            foreach (var batch in valuesList.Chunk(batchSize))
            {
                await ExecuteValuesUpdateAsync(batch);
            }
        }
        
        /// <summary>
        /// Execute UPDATE FROM VALUES for a batch of values.
        /// </summary>
        private async Task ExecuteValuesUpdateAsync(RedbValue[] batch)
        {
            var sb = new StringBuilder();
            var parameters = new List<object?>();
            
            sb.AppendLine("UPDATE _values AS t SET");
            sb.AppendLine("    _string = s._string,");
            sb.AppendLine("    _long = s._long,");
            sb.AppendLine("    _guid = s._guid,");
            sb.AppendLine("    _double = s._double,");
            sb.AppendLine("    _datetimeoffset = s._datetimeoffset,");
            sb.AppendLine("    _boolean = s._boolean,");
            sb.AppendLine("    _bytearray = s._bytearray,");
            sb.AppendLine("    _numeric = s._numeric,");
            sb.AppendLine("    _listitem = s._listitem,");
            sb.AppendLine("    _object = s._object,");
            sb.AppendLine("    _array_parent_id = s._array_parent_id,");
            sb.AppendLine("    _array_index = s._array_index");
            sb.AppendLine("FROM (VALUES");
            
            for (int i = 0; i < batch.Length; i++)
            {
                var val = batch[i];
                var offset = i * 13 + 1; // PostgreSQL uses $1, $2, etc.
                
                if (i > 0) sb.AppendLine(",");
                sb.Append($"    (${offset}::bigint, ${offset + 1}::text, ${offset + 2}::bigint, ${offset + 3}::uuid, ");
                sb.Append($"${offset + 4}::double precision, ${offset + 5}::timestamptz, ${offset + 6}::boolean, ");
                sb.Append($"${offset + 7}::bytea, ${offset + 8}::numeric, ${offset + 9}::bigint, ");
                sb.Append($"${offset + 10}::bigint, ${offset + 11}::bigint, ${offset + 12}::text)");
                
                parameters.Add(val.Id);
                parameters.Add(val.String);
                parameters.Add(val.Long);
                parameters.Add(val.Guid);
                parameters.Add(val.Double);
                parameters.Add(val.DateTimeOffset);
                parameters.Add(val.Boolean);
                parameters.Add(val.ByteArray);
                parameters.Add(val.Numeric);
                parameters.Add(val.ListItem);
                parameters.Add(val.Object);
                parameters.Add(val.ArrayParentId);
                parameters.Add(val.ArrayIndex);
            }
            
            sb.AppendLine();
            sb.AppendLine(") AS s(_id, _string, _long, _guid, _double, _datetimeoffset, _boolean,");
            sb.AppendLine("       _bytearray, _numeric, _listitem, _object, _array_parent_id, _array_index)");
            sb.AppendLine("WHERE t._id = s._id");
            
            await _db.ExecuteAsync(sb.ToString(), parameters.ToArray());
        }

        /// <summary>
        /// Bulk delete objects by IDs.
        /// </summary>
        public async Task BulkDeleteObjectsAsync(IEnumerable<long> objectIds)
        {
            var ids = objectIds.ToArray();
            if (!ids.Any()) return;
            
            await _db.ExecuteAsync("DELETE FROM _objects WHERE _id = ANY($1)", ids);
        }

        /// <summary>
        /// Bulk delete values by IDs.
        /// </summary>
        public async Task BulkDeleteValuesAsync(IEnumerable<long> valueIds)
        {
            var ids = valueIds.ToArray();
            if (!ids.Any()) return;
            
            await _db.ExecuteAsync("DELETE FROM _values WHERE _id = ANY($1)", ids);
        }

        /// <summary>
        /// Bulk delete values by object IDs.
        /// </summary>
        public async Task BulkDeleteValuesByObjectIdsAsync(IEnumerable<long> objectIds)
        {
            var ids = objectIds.ToArray();
            if (!ids.Any()) return;
            
            await _db.ExecuteAsync("DELETE FROM _values WHERE _id_object = ANY($1)", ids);
        }
        
        /// <summary>
        /// Bulk delete values by ListItem IDs.
        /// </summary>
        public async Task BulkDeleteValuesByListItemIdsAsync(IEnumerable<long> listItemIds)
        {
            var ids = listItemIds.ToArray();
            if (!ids.Any()) return;
            
            await _db.ExecuteAsync("DELETE FROM _values WHERE _ListItem = ANY($1)", ids);
        }

        // === HELPER METHODS ===
        
        private static async Task WriteNullableAsync<T>(NpgsqlBinaryImporter writer, T? value, NpgsqlDbType dbType) 
            where T : struct
        {
            if (value.HasValue)
                await writer.WriteAsync(value.Value, dbType);
            else
                await writer.WriteNullAsync();
        }
        
        private static async Task WriteNullableAsync(NpgsqlBinaryImporter writer, string? value, NpgsqlDbType dbType)
        {
            if (value != null)
                await writer.WriteAsync(value, dbType);
            else
                await writer.WriteNullAsync();
        }
        
        private static async Task WriteNullableAsync(NpgsqlBinaryImporter writer, byte[]? value, NpgsqlDbType dbType)
        {
            if (value != null)
                await writer.WriteAsync(value, dbType);
            else
                await writer.WriteNullAsync();
        }
        
        /// <summary>
        /// Write nullable DateTimeOffset with UTC conversion (required by Npgsql).
        /// </summary>
        private static async Task WriteNullableDateTimeAsync(NpgsqlBinaryImporter writer, DateTimeOffset? value)
        {
            if (value.HasValue)
                await writer.WriteAsync(value.Value.ToUniversalTime(), NpgsqlDbType.TimestampTz);
            else
                await writer.WriteNullAsync();
        }
    }
}

