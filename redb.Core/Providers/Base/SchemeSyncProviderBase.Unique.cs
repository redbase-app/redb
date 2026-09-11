using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Attributes;
using redb.Core.Exceptions;
using redb.Core.Models;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Utils;

namespace redb.Core.Providers.Base;

/// <summary>
/// Unique-key side of scheme synchronisation: validating <c>[RedbUnique]</c>, keeping
/// <c>_structures._unique</c> in step with the attribute, and recomputing <c>_values._unique</c>
/// when the keys of a populated structure cannot be trusted.
///
/// <para>
/// Recomputation triggers (UNIQUE plan, work 2.8): the attribute appeared on a populated structure;
/// the structure's <c>_unique_version</c> is behind <see cref="UniqueKeyEncoder.Version"/>; rows exist
/// with a value but no key — the trace of a SQL-side writer (a type migration, a Pro data migration),
/// which releases keys instead of faking them; and the explicit
/// <see cref="RecomputeUniqueAsync{TProps}"/>. Duplicates are reported, never thrown: the first row
/// of a group keeps the key, the rest stay outside the index (§4.2 — otherwise "unique" would be
/// formally on and factually off, silently).
/// </para>
/// </summary>
public abstract partial class SchemeSyncProviderBase
{
    private static readonly HashSet<Type> AllowedUniqueKeyTypes =
    [
        typeof(string), typeof(char),
        typeof(long), typeof(int), typeof(short), typeof(byte),
        typeof(Guid), typeof(bool),
        typeof(double), typeof(float), typeof(decimal),
        typeof(DateTime), typeof(DateTimeOffset), typeof(DateOnly),
        typeof(TimeOnly), typeof(TimeSpan),
        // byte[] is a root scalar since Б1 (owner decision 2026-08-31): stored whole in _ByteArray.
        typeof(byte[]),
    ];

    /// <summary>
    /// Validates the attribute's placement and brings <c>_structures._unique</c> in line with it.
    /// Rejection happens here, at synchronisation — not at insert, where a misplaced key would
    /// degrade into an index that silently never fires (§4.4).
    /// </summary>
    private async Task SyncUniqueFlagAsync(
        Type ownerType, PropertyInfo property, RedbStructure structure,
        bool isArray, bool isDictionary, Type baseType, bool pathHasCollection, CancellationToken cancellationToken = default)
    {
        var uniqueAttribute = property.GetCustomAttribute<RedbUniqueAttribute>();
        var wantUnique = uniqueAttribute != null;
        // S3: a non-default Scope switches a collection key from the subtree-content reading
        // to ELEMENT keys; NULL in _structures._unique_scope means the default reading.
        var wantScope = uniqueAttribute?.Scope is { } s && s != UniqueScope.Default ? (long?)s : null;

        if (wantUnique)
        {
            if (wantScope != null && !isArray && !isDictionary)
                throw new RedbUniqueKeyDefinitionException(ownerType.Name, property.Name,
                    "Scope applies to collections only: on a scalar or a nested class the bare " +
                    "attribute already carries the strongest reading");
            // S2: a scalar inside a nested class is a legal key as long as no ancestor on the
            // path is a collection - on a collection-free path the object holds exactly one row
            // of the structure, so the per-structure index reads the same as for a root key.
            if (pathHasCollection)
                throw new RedbUniqueKeyDefinitionException(ownerType.Name, property.Name,
                    "the property sits inside an element of a collection of classes; every element " +
                    "of every object shares one structure, so uniqueness would mean one value " +
                    "across all elements of all objects");

            // S1: a class, array or dictionary property is a SUBTREE key - the canonical content
            // hash the storage maintains on the base row becomes the key, so "unique" reads as
            // "no two objects hold this exact content". Scalars keep the per-type canon rules.
            var isSubtreeKey = isArray || isDictionary || IsBusinessClass(baseType);
            if (!isSubtreeKey)
            {
                var t = Nullable.GetUnderlyingType(baseType) ?? baseType;
                if (t.IsEnum)
                    throw new RedbUniqueKeyDefinitionException(ownerType.Name, property.Name,
                        "an enum is stored as a list item reference and has no canonical value form");
                if (!AllowedUniqueKeyTypes.Contains(t))
                    throw new RedbUniqueKeyDefinitionException(ownerType.Name, property.Name,
                        $"type '{t.Name}' cannot be a key");
            }
        }

        var current = structure.Unique == true;
        if (current == wantUnique && structure.UniqueScope == wantScope)
            return;

        if (wantUnique)
        {
            // Version stays NULL until the keys are actually computed: EnsureUniqueKeysAsync sees the
            // missing stamp and recomputes, populated structure or not (§4.2). A SCOPE change also
            // clears the stamp and the stored keys: the same values key differently per scope.
            if (current && structure.UniqueScope != wantScope)
                await Context.ExecuteAsync(Sql.Values_ClearUniqueByStructure(), new object[] { structure.Id }, cancellationToken);
            await Context.ExecuteAsync(Sql.Structures_UpdateUnique(),
                new object[] { true, DBNull.Value, (object?)wantScope ?? DBNull.Value, structure.Id }, cancellationToken);
            structure.Unique = true;
            structure.UniqueVersion = null;
            structure.UniqueScope = wantScope;
        }
        else
        {
            // The attribute is gone: the flag and every stored key go with it — a key that no longer
            // guards anything must not keep rejecting values.
            await Context.ExecuteAsync(Sql.Structures_UpdateUnique(),
                new object[] { false, DBNull.Value, DBNull.Value, structure.Id }, cancellationToken);
            await Context.ExecuteAsync(Sql.Values_ClearUniqueByStructure(), new object[] { structure.Id }, cancellationToken);
            structure.Unique = false;
            structure.UniqueVersion = null;
            structure.UniqueScope = null;
        }
    }

    /// <summary>
    /// Runs after the structures of a scheme are in their final shape: recomputes the keys of every
    /// key structure whose stored keys cannot be trusted, and stamps the encoder version.
    /// </summary>
    private async Task EnsureUniqueKeysAsync(IRedbScheme scheme, IReadOnlyList<RedbStructure> structures, CancellationToken cancellationToken = default)
    {
        // S2: nested keys included - the validator only ever sets Unique on collection-free
        // paths, so every flagged structure holds one row per object, root or nested.
        foreach (var structure in structures.Where(s => s.Unique == true))
        {
            // S3: element-scoped keys live on the ELEMENT rows; the unhashed-row probe below is
            // keyed to non-element rows, so element scopes recompute on a version/scope change
            // and via the explicit RecomputeUniqueAsync - a SQL-side wipe of element keys heals
            // on the explicit call, not automatically.
            var isElementScoped = structure.UniqueScope != null;

            // S1: a collection's dbType is the ELEMENT type, but its key lives on the BASE row,
            // whose canonical content hash sits in _Guid; class nodes are typed Guid already.
            var column = structure.CollectionType != null
                ? "_Guid"
                : await GetTypedColumnAsync(structure.IdType);
            if (!isElementScoped && column == null)
                continue; // no canonical column - nothing to hash; the validator refuses such keys anyway

            var needsRecompute = structure.UniqueVersion != UniqueKeyEncoder.Version;
            if (!needsRecompute && !isElementScoped)
            {
                var unhashed = await Context.ExecuteScalarAsync<long?>(
                    Sql.Values_ExistsUnhashedByStructure(column!), new object[] { structure.Id }, cancellationToken);
                needsRecompute = unhashed.HasValue;
            }

            if (!needsRecompute)
                continue;

            var report = await RecomputeUniqueForStructureAsync(scheme, structure);
            if (report.HasDuplicates && Logger != null)
            {
                Logger.LogWarning(
                    "REDB unique key: scheme '{Scheme}', property '{Property}' has {Groups} duplicate group(s), " +
                    "{Losers} row(s) left OUTSIDE the unique index (their _unique is NULL). The first row of each " +
                    "group holds the key. Query them: SELECT * FROM _values WHERE _id_structure = {StructureId} " +
                    "AND _unique IS NULL AND {Column} IS NOT NULL. Uniqueness is enforced for every keyed row; " +
                    "the duplicates are the application's to resolve.",
                    report.SchemeName, report.PropertyName, report.Duplicates.Count, report.LosingRows,
                    structure.Id, column);
            }
        }
    }

    /// <summary>
    /// Recomputes <c>_values._unique</c> for one key structure from the stored typed values — the
    /// same representation the save path hashes, so both agree. Winners (first row per key, lowest
    /// id) get the key; duplicates stay NULL and are reported. Self-healing by design: a crash
    /// half-way leaves unhashed rows, which is itself a recompute trigger.
    /// </summary>
    private async Task<UniqueRecomputeReport> RecomputeUniqueForStructureAsync(IRedbScheme scheme, RedbStructure structure, CancellationToken cancellationToken = default)
    {
        // S3: element-scoped keys sit on the ELEMENT rows and, for the Collection scope, are
        // salted with the owning collection id - the same formula the save path runs.
        var isElementScoped = structure.UniqueScope != null;
        var collectionSalted = structure.UniqueScope == (long)UniqueScope.Collection;
        var rows = await Context.QueryAsync<RedbValue>(
            isElementScoped ? Sql.Values_SelectElementRowsByStructure() : Sql.Values_SelectRootScalarsByStructure(),
            new object[] { structure.Id }, cancellationToken);

        var hashed = 0;
        var winners = new List<(long ValueId, Guid Key)>();
        var toClear = new List<long>();
        var duplicates = new List<UniqueDuplicateGroup>();

        foreach (var group in rows
                     .Select(r => (Row: r, Key: UniqueKeyEncoder.Compute(r, collectionSalted ? r.ArrayParentId : null)))
                     .GroupBy(x => x.Key))
        {
            if (group.Key is null)
            {
                // NULL value: no key. Clear leftovers (e.g. the value was nulled by SQL).
                toClear.AddRange(group.Where(x => x.Row.Unique != null).Select(x => x.Row.Id));
                continue;
            }

            var ordered = group.OrderBy(x => x.Row.Id).ToList();
            var winner = ordered[0];
            hashed++;
            if (winner.Row.Unique != group.Key)
                winners.Add((winner.Row.Id, group.Key.Value));

            if (ordered.Count > 1)
            {
                toClear.AddRange(ordered.Skip(1).Where(x => x.Row.Unique != null).Select(x => x.Row.Id));
                duplicates.Add(new UniqueDuplicateGroup
                {
                    Key = group.Key.Value,
                    ValueIds = ordered.Select(x => x.Row.Id).ToList(),
                    ObjectIds = ordered.Select(x => x.Row.IdObject).ToList(),
                });
            }
        }

        // Clears first: an exchange of keys between rows must release before it takes.
        foreach (var id in toClear)
            await Context.ExecuteAsync(Sql.Values_UpdateUnique(), new object[] { DBNull.Value, id }, cancellationToken);

        foreach (var (valueId, key) in winners)
        {
            try
            {
                await Context.ExecuteAsync(Sql.Values_UpdateUnique(), new object[] { key, valueId }, cancellationToken);
            }
            catch (Exception ex) when (Data.DbErrorClassifier.IsUniqueViolation(ex))
            {
                // A concurrent writer took the key between our read and this write. The row stays
                // unhashed - outside the index, visible to the next recompute trigger.
                Logger?.LogWarning(ex,
                    "REDB unique key recompute: value {ValueId} of structure {StructureId} lost the key to a " +
                    "concurrent writer; the row stays outside the index until resolved.", valueId, structure.Id);
            }
        }

        await Context.ExecuteAsync(Sql.Structures_UpdateUnique(),
            new object[] { true, (long)UniqueKeyEncoder.Version, (object?)structure.UniqueScope ?? DBNull.Value, structure.Id }, cancellationToken);
        structure.UniqueVersion = UniqueKeyEncoder.Version;

        return new UniqueRecomputeReport
        {
            SchemeId = scheme.Id,
            SchemeName = scheme.Name,
            StructureId = structure.Id,
            PropertyName = structure.Name,
            TotalRows = rows.Count,
            HashedRows = hashed,
            Duplicates = duplicates,
        };
    }

    /// <summary>
    /// Explicit recomputation of the keys of one <c>[RedbUnique]</c> property (UNIQUE plan 2.8,
    /// trigger "г"). Same algorithm the automatic triggers use; returns the report instead of
    /// logging it.
    /// </summary>
    public async Task<UniqueRecomputeReport> RecomputeUniqueAsync<TProps>(string propertyName, CancellationToken cancellationToken = default) where TProps : class
    {
        var scheme = await GetSchemeByTypeAsync<TProps>()
            ?? throw new InvalidOperationException($"No scheme for '{typeof(TProps).Name}'; synchronise it first.");

        var structures = await Context.QueryAsync<RedbStructure>(Sql.Structures_SelectByScheme(), new object[] { scheme.Id }, cancellationToken);
        // S2: the name is a path - "Code" for a root key, "Identity.Passport" for a nested one.
        RedbStructure? structure = null;
        foreach (var segment in propertyName.Split('.'))
        {
            var parentId = structure?.Id;
            structure = structures.FirstOrDefault(s => s.IdParent == parentId && s.Name == segment);
            if (structure == null)
                throw new RedbUniqueKeyDefinitionException(typeof(TProps).Name, propertyName,
                    $"no property '{segment}' at this position of the path in the scheme");
        }
        if (structure!.Unique != true)
            throw new RedbUniqueKeyDefinitionException(typeof(TProps).Name, propertyName,
                "the property is not marked [RedbUnique]");

        return await RecomputeUniqueForStructureAsync(scheme, structure);
    }

    /// <summary>The <c>_values</c> typed column a type's canonical value lives in; null for non-scalars.</summary>
    private async Task<string?> GetTypedColumnAsync(long typeId, CancellationToken cancellationToken = default)
    {
        var type = await Context.QueryFirstOrDefaultAsync<RedbType>(Sql.ObjectStorage_SelectTypeById(), new object[] { typeId }, cancellationToken);
        return type?.DbType switch
        {
            "String" or "Text" => "_String",
            "Long" or "bigint" => "_Long",
            "Guid" => "_Guid",
            "Double" => "_Double",
            "DateTime" or "DateTimeOffset" => "_DateTimeOffset",
            "Boolean" => "_Boolean",
            "ByteArray" => "_ByteArray",
            "Numeric" => "_Numeric",
            _ => null,
        };
    }
}
