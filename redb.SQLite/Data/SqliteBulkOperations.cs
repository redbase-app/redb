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
            "_value_numeric", "_value_datetime", "_value_bytes", "_note", "_hash", "_value_unique"
        };

        private static object?[] ObjectRowValues(RedbObjectRow o) => new object?[]
        {
            o.Id, o.IdParent, o.IdScheme, o.Name, o.IdOwner, o.IdWhoChange,
            o.DateCreate, o.DateModify, o.DateBegin, o.DateComplete, o.Key,
            o.ValueLong, o.ValueString, o.ValueGuid, o.ValueBool, o.ValueDouble,
            o.ValueNumeric, o.ValueDatetime, o.ValueBytes, o.Note, o.Hash, o.ValueUnique
        };

        private static readonly string[] ValueColumns =
        {
            "_id", "_id_structure", "_id_object", "_String", "_Long", "_Guid",
            "_Double", "_DateTimeOffset", "_Boolean", "_ByteArray", "_Numeric",
            "_ListItem", "_Object", "_unique", "_array_parent_id", "_array_index"
        };

        private static object?[] ValueRowValues(RedbValue v) => new object?[]
        {
            v.Id, v.IdStructure, v.IdObject, v.String, v.Long, v.Guid,
            v.Double, v.DateTimeOffset, v.Boolean, v.ByteArray, v.Numeric,
            v.ListItem, v.Object, v.Unique, v.ArrayParentId, v.ArrayIndex
        };

        public async Task BulkInsertObjectsAsync(IEnumerable<RedbObjectRow> objects, CancellationToken cancellationToken = default)
        {
            var rows = objects.Select(ObjectRowValues).ToList();
            await BulkInsertAsync("_objects", ObjectColumns, rows, cancellationToken);
        }

        public async Task BulkInsertValuesAsync(IEnumerable<RedbValue> values, CancellationToken cancellationToken = default)
        {
            var rows = values.Select(ValueRowValues).ToList();
            await BulkInsertAsync("_values", ValueColumns, rows, cancellationToken);
        }

        /// <summary>
        /// Insert <paramref name="rows"/> into <paramref name="table"/> using chunked
        /// multi-row INSERTs inside one transaction.
        /// </summary>
        private async Task BulkInsertAsync(string table, string[] columns, List<object?[]> rows, CancellationToken cancellationToken)
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
                            // _hash is BLOB(16) written from the uuid TEXT parameter (SqliteHash).
                            var placeholder = "$" + (++p);
                            sb.Append(columns[c] is "_hash" or "_unique" ? SqliteHash.FromText(placeholder) : placeholder);
                            parameters.Add(chunk[r][c]);
                        }
                        sb.Append(')');
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                    await _db.ExecuteAsync(sb.ToString(), parameters.ToArray()!, cancellationToken);
                }
            });
        }

        // ===== UPDATES (per-row inside one transaction — simple and reliable) =====

        public async Task BulkUpdateObjectsAsync(IEnumerable<RedbObjectRow> objects, CancellationToken cancellationToken = default)
        {
            var list = objects.ToList();
            if (list.Count == 0) return;

            // All columns except _id and _date_create (creation time is immutable).
            const string setSql =
                "_id_parent=$1, _id_scheme=$2, _name=$3, _id_owner=$4, _id_who_change=$5, " +
                "_date_modify=$6, _date_begin=$7, _date_complete=$8, _key=$9, _value_long=$10, " +
                "_value_string=$11, _value_guid=$12, _value_bool=$13, _value_double=$14, " +
                "_value_numeric=$15, _value_datetime=$16, _value_bytes=$17, _note=$18, _hash=unhex(replace($19,'-','')), _value_unique=$20";
            const string sql = "UPDATE _objects SET " + setSql + " WHERE _id=$21";

            await _db.ExecuteAtomicAsync(async () =>
            {
                foreach (var o in list)
                {
                    await _db.ExecuteAsync(sql, new object[] { o.IdParent, o.IdScheme, o.Name!, o.IdOwner, o.IdWhoChange,
                        o.DateModify, o.DateBegin!, o.DateComplete!, o.Key!, o.ValueLong!,
                        o.ValueString!, o.ValueGuid!, o.ValueBool!, o.ValueDouble!,
                        o.ValueNumeric!, o.ValueDatetime!, o.ValueBytes!, o.Note!, o.Hash!, o.ValueUnique!, o.Id }, cancellationToken);
                }
            });
        }

        public async Task BulkUpdateValuesAsync(IEnumerable<RedbValue> values, CancellationToken cancellationToken = default)
        {
            var list = values.ToList();
            if (list.Count == 0) return;

            // F6 (perf wave 6): chunked UPDATE ... FROM (VALUES ...) instead of one statement per
            // row (each row was its own command through P/Invoke). SQLite names bare VALUES
            // columns column1..columnN; _unique keeps its uuid-text -> BLOB(16) conversion, same
            // as the old per-row form did with unhex(replace($11,'-','')).
            const int colCount = 14; // 13 data columns + _id
            int rowsPerChunk = Math.Max(1, MaxParamsPerStatement / colCount);
            const string head =
                "UPDATE _values SET " +
                "_String=u.column1, _Long=u.column2, _Guid=u.column3, _Double=u.column4, " +
                "_DateTimeOffset=u.column5, _Boolean=u.column6, _ByteArray=u.column7, " +
                "_Numeric=u.column8, _ListItem=u.column9, _Object=u.column10, " +
                "_unique=unhex(replace(u.column11,'-','')), _array_parent_id=u.column12, _array_index=u.column13 " +
                "FROM (VALUES ";
            const string tail = ") AS u WHERE _values._id = u.column14";

            await _db.ExecuteAtomicAsync(async () =>
            {
                foreach (var chunk in Chunk(list, rowsPerChunk))
                {
                    var sb = new StringBuilder(head);
                    var parameters = new List<object?>(chunk.Count * colCount);
                    int p = 0;
                    for (int r = 0; r < chunk.Count; r++)
                    {
                        var v = chunk[r];
                        if (r > 0) sb.Append(", ");
                        sb.Append('(');
                        for (int c = 0; c < colCount; c++)
                        {
                            if (c > 0) sb.Append(", ");
                            sb.Append('$').Append(++p);
                        }
                        sb.Append(')');
                        parameters.AddRange(new object?[]
                        {
                            v.String, v.Long, v.Guid, v.Double, v.DateTimeOffset, v.Boolean,
                            v.ByteArray, v.Numeric, v.ListItem, v.Object, v.Unique,
                            v.ArrayParentId, v.ArrayIndex, v.Id
                        });
                    }
                    sb.Append(tail);
                    cancellationToken.ThrowIfCancellationRequested();
                    await _db.ExecuteAsync(sb.ToString(), parameters.ToArray()!, cancellationToken);
                }
            });
        }

        // ===== DELETES (chunked IN(...) — no PG ANY(array)) =====

        public Task BulkDeleteObjectsAsync(IEnumerable<long> objectIds, CancellationToken cancellationToken = default)
            => DeleteByIdsAsync("DELETE FROM _objects WHERE _id IN ", objectIds, cancellationToken);

        public Task BulkDeleteValuesAsync(IEnumerable<long> valueIds, CancellationToken cancellationToken = default)
            => DeleteByIdsAsync("DELETE FROM _values WHERE _id IN ", valueIds, cancellationToken);

        public Task BulkDeleteValuesByObjectIdsAsync(IEnumerable<long> objectIds, CancellationToken cancellationToken = default)
            => DeleteByIdsAsync("DELETE FROM _values WHERE _id_object IN ", objectIds, cancellationToken);

        public Task BulkDeleteValuesByListItemIdsAsync(IEnumerable<long> listItemIds, CancellationToken cancellationToken = default)
            => DeleteByIdsAsync("DELETE FROM _values WHERE _ListItem IN ", listItemIds, cancellationToken);

        private async Task DeleteByIdsAsync(string head, IEnumerable<long> ids, CancellationToken cancellationToken)
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
