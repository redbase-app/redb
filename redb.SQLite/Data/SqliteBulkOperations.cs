using redb.Core.Data;
using redb.Core.Models.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace redb.SQLite.Data
{
    /// <summary>
    /// SQLite implementation of IBulkOperations.
    /// SQLite has no COPY/binary-import protocol — the fast path is a single
    /// transaction wrapping chunked multi-row INSERTs (one prepared statement
    /// per chunk). The big win versus N individual statements is the single
    /// transaction (one fsync instead of one per row). Parameter count per
    /// statement is chunked under SQLite's limit.
    /// </summary>
    public class SqliteBulkOperations : IBulkOperations
    {
        private readonly IRedbConnection _db;

        // Conservative cap on bound parameters per statement (SQLite default
        // SQLITE_MAX_VARIABLE_NUMBER is 999 on older builds, 32766 on 3.32+).
        private const int MaxParamsPerStatement = 900;

        public SqliteBulkOperations(IRedbConnection db)
        {
            _db = db ?? throw new ArgumentNullException(nameof(db));
        }

        // ===== INSERTS =====

        private static readonly string[] ObjectColumns =
        {
            "_id", "_id_parent", "_id_scheme", "_name", "_id_owner", "_id_who_change",
            "_date_create", "_date_modify", "_date_begin", "_date_complete", "_key",
            "_value_long", "_value_string", "_value_guid", "_value_bool", "_value_double",
            "_value_numeric", "_value_datetime", "_value_bytes", "_note", "_hash"
        };

        private static object?[] ObjectRowValues(RedbObjectRow o) => new object?[]
        {
            o.Id, o.IdParent, o.IdScheme, o.Name, o.IdOwner, o.IdWhoChange,
            o.DateCreate, o.DateModify, o.DateBegin, o.DateComplete, o.Key,
            o.ValueLong, o.ValueString, o.ValueGuid, o.ValueBool, o.ValueDouble,
            o.ValueNumeric, o.ValueDatetime, o.ValueBytes, o.Note, o.Hash
        };

        private static readonly string[] ValueColumns =
        {
            "_id", "_id_structure", "_id_object", "_String", "_Long", "_Guid",
            "_Double", "_DateTimeOffset", "_Boolean", "_ByteArray", "_Numeric",
            "_ListItem", "_Object", "_array_parent_id", "_array_index"
        };

        private static object?[] ValueRowValues(RedbValue v) => new object?[]
        {
            v.Id, v.IdStructure, v.IdObject, v.String, v.Long, v.Guid,
            v.Double, v.DateTimeOffset, v.Boolean, v.ByteArray, v.Numeric,
            v.ListItem, v.Object, v.ArrayParentId, v.ArrayIndex
        };

        public async Task BulkInsertObjectsAsync(IEnumerable<RedbObjectRow> objects)
        {
            var rows = objects.Select(ObjectRowValues).ToList();
            await BulkInsertAsync("_objects", ObjectColumns, rows);
        }

        public async Task BulkInsertValuesAsync(IEnumerable<RedbValue> values)
        {
            var rows = values.Select(ValueRowValues).ToList();
            await BulkInsertAsync("_values", ValueColumns, rows);
        }

        /// <summary>
        /// Insert <paramref name="rows"/> into <paramref name="table"/> using chunked
        /// multi-row INSERTs inside one transaction.
        /// </summary>
        private async Task BulkInsertAsync(string table, string[] columns, List<object?[]> rows)
        {
            if (rows.Count == 0) return;

            int colCount = columns.Length;
            int rowsPerChunk = Math.Max(1, MaxParamsPerStatement / colCount);
            string columnList = string.Join(", ", columns);

            await _db.ExecuteAtomicAsync(async () =>
            {
                foreach (var chunk in Chunk(rows, rowsPerChunk))
                {
                    var sb = new StringBuilder();
                    sb.Append("INSERT INTO ").Append(table).Append(" (").Append(columnList).Append(") VALUES ");

                    var parameters = new List<object?>(chunk.Count * colCount);
                    int p = 0;
                    for (int r = 0; r < chunk.Count; r++)
                    {
                        if (r > 0) sb.Append(", ");
                        sb.Append('(');
                        for (int c = 0; c < colCount; c++)
                        {
                            if (c > 0) sb.Append(", ");
                            sb.Append('$').Append(++p);
                            parameters.Add(chunk[r][c]);
                        }
                        sb.Append(')');
                    }

                    await _db.ExecuteAsync(sb.ToString(), parameters.ToArray()!);
                }
            });
        }

        // ===== UPDATES (per-row inside one transaction — simple and reliable) =====

        public async Task BulkUpdateObjectsAsync(IEnumerable<RedbObjectRow> objects)
        {
            var list = objects.ToList();
            if (list.Count == 0) return;

            // All columns except _id and _date_create (creation time is immutable).
            const string setSql =
                "_id_parent=$1, _id_scheme=$2, _name=$3, _id_owner=$4, _id_who_change=$5, " +
                "_date_modify=$6, _date_begin=$7, _date_complete=$8, _key=$9, _value_long=$10, " +
                "_value_string=$11, _value_guid=$12, _value_bool=$13, _value_double=$14, " +
                "_value_numeric=$15, _value_datetime=$16, _value_bytes=$17, _note=$18, _hash=$19";
            const string sql = "UPDATE _objects SET " + setSql + " WHERE _id=$20";

            await _db.ExecuteAtomicAsync(async () =>
            {
                foreach (var o in list)
                {
                    await _db.ExecuteAsync(sql,
                        o.IdParent, o.IdScheme, o.Name!, o.IdOwner, o.IdWhoChange,
                        o.DateModify, o.DateBegin!, o.DateComplete!, o.Key!, o.ValueLong!,
                        o.ValueString!, o.ValueGuid!, o.ValueBool!, o.ValueDouble!,
                        o.ValueNumeric!, o.ValueDatetime!, o.ValueBytes!, o.Note!, o.Hash!, o.Id);
                }
            });
        }

        public async Task BulkUpdateValuesAsync(IEnumerable<RedbValue> values)
        {
            var list = values.ToList();
            if (list.Count == 0) return;

            const string setSql =
                "_String=$1, _Long=$2, _Guid=$3, _Double=$4, _DateTimeOffset=$5, _Boolean=$6, " +
                "_ByteArray=$7, _Numeric=$8, _ListItem=$9, _Object=$10, _array_parent_id=$11, _array_index=$12";
            const string sql = "UPDATE _values SET " + setSql + " WHERE _id=$13";

            await _db.ExecuteAtomicAsync(async () =>
            {
                foreach (var v in list)
                {
                    await _db.ExecuteAsync(sql,
                        v.String!, v.Long!, v.Guid!, v.Double!, v.DateTimeOffset!, v.Boolean!,
                        v.ByteArray!, v.Numeric!, v.ListItem!, v.Object!, v.ArrayParentId!, v.ArrayIndex!, v.Id);
                }
            });
        }

        // ===== DELETES (chunked IN(...) — no PG ANY(array)) =====

        public Task BulkDeleteObjectsAsync(IEnumerable<long> objectIds)
            => DeleteByIdsAsync("DELETE FROM _objects WHERE _id IN ", objectIds);

        public Task BulkDeleteValuesAsync(IEnumerable<long> valueIds)
            => DeleteByIdsAsync("DELETE FROM _values WHERE _id IN ", valueIds);

        public Task BulkDeleteValuesByObjectIdsAsync(IEnumerable<long> objectIds)
            => DeleteByIdsAsync("DELETE FROM _values WHERE _id_object IN ", objectIds);

        public Task BulkDeleteValuesByListItemIdsAsync(IEnumerable<long> listItemIds)
            => DeleteByIdsAsync("DELETE FROM _values WHERE _ListItem IN ", listItemIds);

        private async Task DeleteByIdsAsync(string head, IEnumerable<long> ids)
        {
            var idList = ids.ToList();
            if (idList.Count == 0) return;

            await _db.ExecuteAtomicAsync(async () =>
            {
                foreach (var chunk in Chunk(idList, MaxParamsPerStatement))
                {
                    var sb = new StringBuilder(head).Append('(');
                    var parameters = new object?[chunk.Count];
                    for (int i = 0; i < chunk.Count; i++)
                    {
                        if (i > 0) sb.Append(", ");
                        sb.Append('$').Append(i + 1);
                        parameters[i] = chunk[i];
                    }
                    sb.Append(')');
                    await _db.ExecuteAsync(sb.ToString(), parameters!);
                }
            });
        }

        // ===== helpers =====

        private static IEnumerable<List<T>> Chunk<T>(IReadOnlyList<T> source, int size)
        {
            for (int i = 0; i < source.Count; i += size)
            {
                int n = Math.Min(size, source.Count - i);
                var bucket = new List<T>(n);
                for (int j = 0; j < n; j++) bucket.Add(source[i + j]);
                yield return bucket;
            }
        }
    }
}
