using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Utils;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// Unique-key side of the storage provider: the typed exception for a violated key, and the
    /// point lookup by key. The key itself is computed in the scalar value builders
    /// (<see cref="UniqueKeyEncoder"/>); the index lives in the database.
    /// </summary>
    public abstract partial class ObjectStorageProviderBase
    {
        /// <summary>
        /// Builds the typed exception for a unique violation the save just hit. Enrichment — which
        /// structure, scheme, property — is best-effort and runs after the transaction rolled back;
        /// a failure to enrich must never mask the violation itself.
        /// </summary>
        protected async Task<RedbUniqueViolationException> TranslateUniqueViolationAsync(Exception ex, long? schemeId)
        {
            var (constraint, detail, structureId, kind) = DbErrorClassifier.DescribeUniqueViolation(ex);

            string? schemeName = null;
            string? propertyName = null;
            try
            {
                if (structureId.HasValue)
                {
                    var structure = await _context.QueryFirstOrDefaultAsync<RedbStructure>(
                        Sql.Structures_SelectById(), structureId.Value);
                    if (structure != null)
                    {
                        propertyName = structure.Name;
                        schemeId = structure.IdScheme;
                    }
                }

                if (schemeId.HasValue)
                    schemeName = (await _schemeSync.GetSchemeByIdAsync(schemeId.Value))?.Name;
            }
            catch
            {
                // Best effort only: the ids we already have still tell the story.
            }

            return new RedbUniqueViolationException(schemeId, schemeName, structureId, propertyName, constraint, detail, ex, kind);
        }

        /// <summary>
        /// The object of <typeparamref name="TProps"/> whose <c>[RedbUnique]</c> property holds
        /// <paramref name="value"/>, or <c>null</c>. One index probe: the value is canonicalised and
        /// hashed exactly as the save path does it, and the partial unique index answers.
        /// A <c>null</c> value returns <c>null</c> — NULL never takes part in uniqueness.
        /// </summary>
        public async Task<RedbObject<TProps>?> GetByUniqueAsync<TProps>(
            Expression<Func<TProps, object?>> keyProperty, object? value, int depth = 10, CancellationToken cancellationToken = default)
            where TProps : class, new()
            => await GetByUniqueAsync<TProps>(GetKeyPropertyName(keyProperty), value, depth, cancellationToken);

        /// <inheritdoc cref="GetByUniqueAsync{TProps}(Expression{Func{TProps, object?}}, object?, int, CancellationToken)"/>
        public async Task<RedbObject<TProps>?> GetByUniqueAsync<TProps>(
            string propertyName, object? value, int depth = 10, CancellationToken cancellationToken = default)
            where TProps : class, new()
        {
            if (value is null)
                return null;

            var scheme = await _schemeSync.GetSchemeByTypeAsync<TProps>()
                ?? throw new InvalidOperationException(
                    $"No scheme for '{typeof(TProps).Name}'; synchronise it first.");

            var metadata = await GetStructuresWithMetadataAsync(scheme.Id);
            // S2: the name is a path - "Code" for a root key, "Identity.Passport" for a nested one.
            var structure = default(StructureMetadata);
            foreach (var segment in propertyName.Split('.'))
            {
                var parentId = structure?.Id;
                structure = metadata.FirstOrDefault(m => m.IdParent == parentId && m.Name == segment)
                    ?? throw new RedbUniqueKeyDefinitionException(typeof(TProps).Name, propertyName,
                        $"no property '{segment}' at this position of the path in the scheme");
            }
            if (!structure!.Unique)
                throw new RedbUniqueKeyDefinitionException(typeof(TProps).Name, propertyName,
                    "the property is not marked [RedbUnique]");

            // S3: a Collection-scoped key is unique only within its collection - a global
            // by-value probe has no meaning there.
            if (structure.UniqueScope == (long)Core.Attributes.UniqueScope.Collection)
                throw new RedbUniqueKeyDefinitionException(typeof(TProps).Name, propertyName,
                    "the key is scoped to its collection (Scope = Collection); a global lookup " +
                    "by element value has no meaning - query the collection instead");

            // The same canonicalisation the save path runs: value -> typed column -> canonical form -> hash.
            var probe = new RedbValue();
            if (structure.UniqueScope == (long)Core.Attributes.UniqueScope.Scheme)
            {
                // S3, Scheme-scoped ELEMENT key: the caller passes an element value. References
                // canonicalise by target id, exactly like the save path keyed the element rows.
                if (value is IRedbObject refValue)
                    probe.Object = refValue.Id;
                else if (value is IRedbListItem listItemValue)
                    probe.ListItem = listItemValue.Id;
                else
                    SetSimpleValueByType(probe, structure.DbType, value);
            }
            else
            {
                // S1: for a subtree key (a class, array or dictionary) the caller passes the
                // subtree VALUE; its canonical content hash - the same one the save wrote into
                // the base row's _Guid - is the probed value. Reference collections are out of
                // the by-value lookup: their stored hash combines the elements' persisted
                // hashes, which a detached value cannot reproduce.
                var probeDbType = structure.DbType;
                var probeValue = value;
                var isSubtreeKey = structure.CollectionType != null || structure.TypeSemantic == "Object";
                if (isSubtreeKey && value is not Guid)
                {
                    var contentHash = Utils.RedbHash.ComputeForProps(value);
                    if (contentHash is null)
                        return null;
                    probeValue = contentHash;
                    probeDbType = "Guid";
                }
                SetSimpleValueByType(probe, probeDbType, probeValue);
            }
            var key = UniqueKeyEncoder.Compute(probe);
            if (key is null)
                return null;

            var objectId = await _context.ExecuteScalarAsync<long?>(
                Sql.Values_SelectObjectIdByUnique(), new object[] { structure.Id, key.Value }, cancellationToken);

            return objectId.HasValue ? await LoadAsync<TProps>(objectId.Value, depth, cancellationToken) : null;
        }


        /// <summary>
        /// The single point where <see cref="SaveByUniqueAsync{TProps}"/> resolves "who owns this
        /// key right now" - one probe of <c>UIX__objects__scheme_unique</c>. Virtual as a
        /// deterministic interleave seam for the race tests (delete + recreate between the resolve
        /// and the save); production overrides are not expected.
        /// </summary>
        protected virtual Task<long?> ResolveObjectIdByUniqueKeyAsync(long schemeId, string? valueUnique, CancellationToken cancellationToken = default)
            => _context.ExecuteScalarAsync<long?>(
                Sql.ObjectStorage_SelectIdBySchemeValueUnique(), new object[] { schemeId, valueUnique! }, cancellationToken);

        /// <summary>
        /// Upsert by the object key (UNIQUE stage 1, P1): resolve the row by
        /// (scheme, <c>ValueUnique</c>), then save onto it - the values ride the ordinary save
        /// pipeline in the same transaction as the row. Not a single-statement native upsert on
        /// purpose: writing the _objects row via ON CONFLICT/MERGE would bypass the values
        /// pipeline; resolving first costs one indexed probe and reuses everything. A lost
        /// OBJECT-KEY race retries once onto the current winner's row - both the creation race
        /// (the key appeared between the lookup and our insert) and the vanish race (the resolved
        /// row was deleted and the key recreated elsewhere; BR-8, 2026-09-02) - which keeps a
        /// concurrent accept of one key idempotent: exactly one object per key, last writer's
        /// content. A [RedbUnique] PROPERTY violation is never retried: it is not the object
        /// key's business and surfaces as-is (<see cref="RedbUniqueViolationException.Kind"/>).
        /// </summary>
        public async Task<long> SaveByUniqueAsync<TProps>(RedbObject<TProps> obj, CancellationToken cancellationToken = default) where TProps : class, new()
        {
            if (string.IsNullOrEmpty(obj.ValueUnique))
                throw new RedbUniqueKeyDefinitionException(typeof(TProps).Name, nameof(Models.Contracts.IRedbObject.ValueUnique),
                    "SaveByUniqueAsync needs a non-empty ValueUnique to resolve the object by");

            var scheme = await _schemeSync.EnsureSchemeFromTypeAsync<TProps>();

            var existingId = await ResolveObjectIdByUniqueKeyAsync(scheme.Id, obj.ValueUnique, cancellationToken);
            if (existingId.HasValue)
                obj.id = existingId.Value;

            try
            {
                return await SaveAsync(obj, cancellationToken);
            }
            catch (RedbUniqueViolationException uve) when (uve.Kind != RedbUniqueViolationKind.Property)
            {
                // Lost an object-key race: either the key appeared between the lookup and our
                // insert (creation race), or the resolved row vanished and the key was recreated
                // elsewhere - our save then inserted via AutoSwitchToInsert and hit the key index
                // (BR-8, 2026-09-02). Take the current winner's row and write onto it, once.
                // Kind=Property never lands here: a field collision cannot be fixed by
                // re-resolving the object key.
                var winner = await ResolveObjectIdByUniqueKeyAsync(scheme.Id, obj.ValueUnique, cancellationToken);
                if (!winner.HasValue)
                    throw; // the key vanished again (deleted mid-race) - surface the original failure
                if (existingId.HasValue && winner.Value == existingId.Value)
                    throw; // the same row still owns the key: the violation is not our object key - do not loop

                obj.id = winner.Value;
                return await SaveAsync(obj, cancellationToken);
            }
        }

        private static string GetKeyPropertyName<TProps>(Expression<Func<TProps, object?>> keyProperty)
        {
            // S2: the selector may walk into a nested class - the member chain becomes the
            // dotted structure path ("Identity.Passport").
            var body = keyProperty.Body is UnaryExpression unary ? unary.Operand : keyProperty.Body;
            var segments = new Stack<string>();
            while (body is MemberExpression member)
            {
                segments.Push(member.Member.Name);
                body = member.Expression!;
            }
            if (segments.Count == 0 || body is not ParameterExpression)
                throw new ArgumentException(
                    "The expression must select a property path, e.g. p => p.Code or p => p.Identity.Passport.",
                    nameof(keyProperty));
            return string.Join(".", segments);
        }
    }
}
