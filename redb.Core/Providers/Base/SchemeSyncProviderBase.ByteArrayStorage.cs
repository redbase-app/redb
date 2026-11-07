using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Entities;
using redb.Core.Utils;

namespace redb.Core.Providers.Base;

/// <summary>
/// Migration of <c>byte[]</c> properties to scalar storage (V4, work Б1 — owner decision 2026-08-31).
///
/// <para>
/// Before V4 the scheme sync classified a <c>byte[]</c> property as an ARRAY of Byte: a base row
/// with the array hash in <c>_Guid</c> plus one element row per byte in <c>_Long</c>. The scalar
/// machinery — the ByteArray type, the <c>_values._ByteArray</c> BLOB column, base64 in the JSON
/// builders, the Pro materializer — existed all along and was unreachable from class models. V4
/// classifies <c>byte[]</c> as the scalar it is; this file converts what the old classification
/// left behind, at synchronisation, self-healing and idempotent: the bytes are assembled from the
/// element rows in index order into the base row's <c>_ByteArray</c>, the element rows are deleted,
/// and the structure becomes ByteArray with no collection.
/// </para>
/// </summary>
public abstract partial class SchemeSyncProviderBase
{
    /// <summary>
    /// True when the structure is the pre-V4 shape of a <c>byte[]</c> property (Array of Byte) and
    /// the CLR side now wants the scalar ByteArray.
    /// </summary>
    private static bool IsLegacyByteArrayShape(RedbStructure structure, long newTypeId, bool isArray)
        => structure.IdType == RedbTypeIds.Byte
           && structure.CollectionType == RedbTypeIds.Array
           && newTypeId == RedbTypeIds.ByteArray
           && !isArray;

    /// <summary>
    /// Converts every object's per-byte element rows into one scalar <c>_ByteArray</c> value on the
    /// base row, then retypes the structure. Runs inside scheme synchronisation; a crash half-way
    /// leaves the structure untouched (type flips last), so the next start converts again — and a
    /// base row that already carries its bytes with no element rows left (the crash landed between
    /// the delete and the type flip, or a second node finished first) is recognised and kept.
    /// </summary>
    private async Task ConvertByteArrayStorageAsync(RedbStructure structure, CancellationToken cancellationToken = default)
    {
        // The old layout: base row with the array hash in _Guid; element rows with
        // _array_parent_id = base._id, _array_index = "0".."n", the byte in _Long.
        //
        // A base row is NOT "the row with no parent": a byte[] nested in a class (or in an
        // array-of-class element) has _array_parent_id = the class row, a row of ANOTHER structure,
        // and a pseudo _array_index. Membership tells the two apart: an element row points at a row
        // of THIS structure, a base row never does.
        var rows = await Context.QueryAsync<RedbValue>(Sql.Values_SelectByStructure(), new object[] { structure.Id }, cancellationToken);
        var ownIds = rows.Select(r => r.Id).ToHashSet();
        bool IsElement(RedbValue r) => r.ArrayParentId is { } parent && ownIds.Contains(parent);

        var elements = rows.Where(IsElement).ToLookup(r => r.ArrayParentId!.Value);
        var baseRows = rows.Where(r => !IsElement(r)).ToList();

        var converted = 0;
        var alreadyConverted = 0;
        foreach (var baseRow in baseRows)
        {
            var parts = elements[baseRow.Id]
                .OrderBy(e => int.TryParse(e.ArrayIndex, out var i) ? i : int.MaxValue)
                .ToList();

            if (parts.Count == 0 && baseRow.ByteArray != null)
            {
                // Bytes assembled and elements gone, only the type flip is missing: a previous run
                // got that far. Overwriting with an empty payload here is how a retry loses data.
                alreadyConverted++;
                continue;
            }

            var bytes = new byte[parts.Count];
            for (var i = 0; i < parts.Count; i++)
                bytes[i] = unchecked((byte)(parts[i].Long ?? 0));

            await Context.ExecuteAsync(Sql.Values_SetByteArrayScalar(), new object[] { bytes, baseRow.Id }, cancellationToken);
            converted++;
        }

        // Set-based: the element rows of a megabyte payload are a million ids, too many to inline.
        var removed = elements.Count == 0
            ? 0
            : await Context.ExecuteAsync(Sql.Values_DeleteByteArrayElements(), new object[] { structure.Id }, cancellationToken);

        Logger?.LogInformation(
            "REDB byte[] storage: structure {StructureId} ('{Name}') converted from Array-of-Byte to " +
            "scalar _ByteArray: {Objects} object value(s) assembled, {Kept} already converted, " +
            "{Elements} element row(s) removed.",
            structure.Id, structure.Name, converted, alreadyConverted, removed);
    }
}
