using redb.Core.Providers;
using redb.Core.Data;
using redb.Core.Utils;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Configuration;
using redb.Core.Models.Security;
using redb.Core.Caching;
using redb.Core.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace redb.Core.Providers.Base
{
    /// <summary>
    /// BATCH SAVE - high-performance saving of multiple objects
    /// Supports two strategies: DeleteInsert (bulk operations) and ChangeTracking (EF diff)
    /// </summary>
    public abstract partial class ObjectStorageProviderBase
    {
        /// <summary>
        /// Convert IRedbObject to RedbObjectRow for bulk operations
        /// Uses 'as' optimization for direct access to RedbObject fields.
        /// </summary>
        protected RedbObjectRow ConvertToObjectRecord(IRedbObject obj)
        {
            var redbObj = obj as RedbObject;  // Optimization: direct access to fields

            var record = new RedbObjectRow
            {
                Id = redbObj?.id ?? obj.Id,
                IdScheme = redbObj?.scheme_id ?? obj.SchemeId,
                IdParent = (redbObj?.parent_id ?? obj.ParentId) == 0 ? null : (redbObj?.parent_id ?? obj.ParentId),
                IdOwner = redbObj?.owner_id ?? obj.OwnerId,
                IdWhoChange = redbObj?.who_change_id ?? obj.WhoChangeId,
                Name = redbObj?.name ?? obj.Name,
                Hash = redbObj?.hash ?? obj.Hash,
                ValueUnique = redbObj?.value_unique ?? obj.ValueUnique,
                DateBegin = redbObj?.date_begin ?? obj.DateBegin,
                DateComplete = redbObj?.date_complete ?? obj.DateComplete,
                Key = redbObj?.key ?? obj.Key,
                ValueLong = redbObj?.value_long ?? obj.ValueLong,
                ValueString = redbObj?.value_string ?? obj.ValueString,
                ValueGuid = redbObj?.value_guid ?? obj.ValueGuid,
                ValueBool = redbObj?.value_bool ?? obj.ValueBool,
                ValueDouble = redbObj?.value_double ?? obj.ValueDouble,
                ValueNumeric = redbObj?.value_numeric ?? obj.ValueNumeric,
                ValueDatetime = redbObj?.value_datetime ?? obj.ValueDatetime,
                ValueBytes = redbObj?.value_bytes ?? obj.ValueBytes,
                Note = redbObj?.note ?? obj.Note
            };

            // Handle creation/modification dates based on configuration
            if (_configuration.AutoSetModifyDate)
            {
                if (record.Id == 0)
                {
                    record.DateCreate = DateTimeOffset.UtcNow;
                }
                else
                {
                    // For existing objects - take the old value
                    var existingDate = redbObj?.date_create ?? obj.DateCreate;
                    // If the date was not set (MinValue), use the current one
                    record.DateCreate = existingDate == DateTimeOffset.MinValue ? DateTimeOffset.UtcNow : existingDate;
                }
                record.DateModify = DateTimeOffset.UtcNow;
            }
            else
            {
                var dateCreate = redbObj?.date_create ?? obj.DateCreate;
                // If the date was not set (MinValue), use the current one
                record.DateCreate = dateCreate == DateTimeOffset.MinValue ? DateTimeOffset.UtcNow : dateCreate;

                var dateModify = redbObj?.date_modify ?? obj.DateModify;
                record.DateModify = dateModify == DateTimeOffset.MinValue ? DateTimeOffset.UtcNow : dateModify;
            }

            return record;
        }

        // ===== PUBLIC BATCH SAVE METHODS =====

        /// <summary>
        /// BATCH SAVE: Save multiple objects (new + updates)
        /// Uses _securityContext to get the user
        /// </summary>
        public async Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, CancellationToken cancellationToken = default)
        {
            var effectiveUser = _securityContext.GetEffectiveUser();
            return await SaveAsync(objects, effectiveUser, cancellationToken);
        }

        /// <summary>
        /// BATCH SAVE with explicit user
        /// Supports two strategy modes:
        /// - DeleteInsert: delete ALL values → BulkInsert/BulkUpdate of objects → BulkInsert of values
        /// - ChangeTracking: tree-based diff → two-phase EF SaveChanges
        /// </summary>
        public async Task<List<long>> SaveAsync(IEnumerable<IRedbObject> objects, IRedbUser user, CancellationToken cancellationToken = default)
        {
            // CT-4 (решение владельца 2026-09-04, «как у EF»): контракт «один scope - один
            // поток» защищён операционным детектором уровня EF DbContext. Командный страж
            // соединения ловит только НАЛОЖИВШИЕСЯ команды; два перемежающихся async-сохранения
            // могли молча перемешать pending-состояние ChangeTracking и транзакции. Вход -
            // синхронно, до первого await, поэтому второй параллельный заход детерминированно
            // получает исключение.
            if (System.Threading.Interlocked.CompareExchange(ref _saveOperationInFlight, 1, 0) != 0)
                throw new InvalidOperationException(
                    "A second SaveAsync started on this IRedbService scope before a previous one " +
                    "completed. Один scope - это одно соединение и одно операционное состояние " +
                    "(как DbContext в EF): параллельные сохранения ведите через отдельные scoped-сервисы.");
            try
            {
                return await SaveBatchCoreAsync(objects, user, cancellationToken);
            }
            finally
            {
                System.Threading.Interlocked.Exchange(ref _saveOperationInFlight, 0);
            }
        }

        private int _saveOperationInFlight;

        private sealed class IdHashRow
        {
            public long Id { get; set; }
            public Guid? Hash { get; set; }
        }

        /// <summary>
        /// F1 (CT hash shortcut): ids of existing objects whose freshly recomputed content hash
        /// equals the persisted <c>_objects._hash</c>. Root hashes come free with the phase-1
        /// existence check; one extra SELECT runs only when the collected graph brought nested
        /// existing objects. An object with no in-memory hash or no persisted hash is never listed
        /// (legacy hashes computed by an older canon simply fail the comparison and take the full
        /// path, migrating on save).
        /// </summary>
        private async Task<ISet<long>?> SelectUnchangedByHashAsync(
            List<IRedbObject> allObjectsToSave, Dictionary<long, Guid?> dbHashById, CancellationToken cancellationToken)
        {
            var candidates = allObjectsToSave.Where(o => o.Id > 0 && o.Hash.HasValue).ToList();
            if (candidates.Count == 0) return null;

            var missingIds = candidates.Where(o => !dbHashById.ContainsKey(o.Id))
                .Select(o => o.Id).Distinct().ToArray();
            if (missingIds.Length > 0)
            {
                var rows = await _context.QueryAsync<IdHashRow>(
                    Sql.ObjectStorage_SelectIdHashPairs(), new object[] { missingIds }, cancellationToken);
                foreach (var row in rows)
                    dbHashById[row.Id] = row.Hash;
            }

            var unchanged = new HashSet<long>();
            foreach (var obj in candidates)
            {
                if (dbHashById.TryGetValue(obj.Id, out var dbHash) &&
                    dbHash.HasValue && dbHash.Value == obj.Hash!.Value)
                    unchanged.Add(obj.Id);
            }
            return unchanged.Count > 0 ? unchanged : null;
        }

        private async Task<List<long>> SaveBatchCoreAsync(IEnumerable<IRedbObject> objects, IRedbUser user, CancellationToken cancellationToken)
        {
            var objList = objects.ToList();
            if (objList.Count == 0) return new List<long>();

            // === PHASE 1: VALIDATION AND PREPARATION (BATCH OPTIMIZATION) ===

            // STEP 1: Hash recomputation (in-memory, no DB queries)
            // Supports: RedbObject<TProps> with Props, RedbObject<TProps> with Props=null, RedbObject (non-generic)
            foreach (var obj in objList)
                RecomputeHash(obj);

            // STEP 2: BATCH existence check (1 query instead of N!)
            // F1: under ChangeTracking the same query also returns the persisted hash of each
            // existing root, so the hash shortcut costs no extra round-trip for the typical save.
            // Other strategies keep the id-only query - they never consult the hashes.
            var ctShortcutActive = _configuration.PropsSaveStrategy == PropsSaveStrategy.ChangeTracking
                && _configuration.AutoRecomputeHash;
            var existingIds = objList.Where(o => o.Id != 0).Select(o => o.Id).ToArray();
            var dbHashById = new Dictionary<long, Guid?>(existingIds.Length);
            HashSet<long> existingIdsSet;
            if (existingIds.Length == 0)
            {
                existingIdsSet = new HashSet<long>();
            }
            else if (ctShortcutActive)
            {
                var idHashRows = await _context.QueryAsync<IdHashRow>(
                    Sql.ObjectStorage_SelectIdHashPairs(), new object[] { existingIds }, cancellationToken);
                foreach (var row in idHashRows)
                    dbHashById[row.Id] = row.Hash;
                existingIdsSet = dbHashById.Keys.ToHashSet();
            }
            else
            {
                var loadedIds = await _context.QueryScalarListAsync<long>(
                    Sql.ObjectStorage_SelectExistingIds(), new object[] { existingIds }, cancellationToken);
                existingIdsSet = loadedIds.ToHashSet();
            }

            // Processing of missing objects
            foreach (var obj in objList.Where(o => o.Id != 0 && !existingIdsSet.Contains(o.Id)))
            {
                switch (_configuration.MissingObjectStrategy)
                {
                    case MissingObjectStrategy.AutoSwitchToInsert:
                        obj.Id = 0;
                        break;
                    case MissingObjectStrategy.ReturnNull:
                        throw new InvalidOperationException($"Object with id {obj.Id} not found. Strategy: ReturnNull is not supported.");
                    case MissingObjectStrategy.ThrowException:
                    default:
                        throw new InvalidOperationException($"Object with id {obj.Id} not found.");
                }
            }

            // STEP 3: BATCH auto-detection of schemes (caching by type)
            var schemeCache = new Dictionary<Type, long>();
            long? objectSchemeId = null; // Cache for non-generic RedbObject scheme

            foreach (var obj in objList.Where(o => o.SchemeId == 0 && _configuration.AutoSyncSchemesOnSave))
            {
                var objType = obj.GetType();
                if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(RedbObject<>))
                {
                    var propsType = objType.GetGenericArguments()[0];

                    // Check cache
                    if (schemeCache.TryGetValue(propsType, out var cachedSchemeId))
                    {
                        obj.SchemeId = cachedSchemeId;
                        continue;
                    }

                    // Load/sync the scheme (select generic overload without parameters)
                    var getSchemeMethod = typeof(ISchemeSyncProvider)
                        .GetMethods()
                        .FirstOrDefault(m => m.Name == nameof(ISchemeSyncProvider.GetSchemeByTypeAsync)
                                          && m.IsGenericMethod
                                          && m.GetParameters() is [{ ParameterType.Name: nameof(CancellationToken) }])?
                        .MakeGenericMethod(propsType);

                    if (getSchemeMethod != null)
                    {
                        var existingSchemeTask = (Task?)getSchemeMethod.Invoke(_schemeSync, new object[] { cancellationToken });
                        if (existingSchemeTask != null)
                        {
                            await existingSchemeTask.ConfigureAwait(false);
                            var existingScheme = existingSchemeTask.GetType().GetProperty("Result")?.GetValue(existingSchemeTask) as IRedbScheme;

                            if (existingScheme != null)
                            {
                                obj.SchemeId = existingScheme.Id;
                                schemeCache[propsType] = existingScheme.Id;
                            }
                            else if (_configuration.AutoSyncSchemesOnSave)
                            {
                                var syncMethod = typeof(ISchemeSyncProvider)
                                    .GetMethods()
                                    .FirstOrDefault(m => m.Name == nameof(ISchemeSyncProvider.SyncSchemeAsync)
                                                      && m.IsGenericMethod
                                                      && m.GetParameters() is [{ ParameterType.Name: nameof(CancellationToken) }])?
                                    .MakeGenericMethod(propsType);

                                if (syncMethod != null)
                                {
                                    var syncTask = (Task?)syncMethod.Invoke(_schemeSync, new object[] { cancellationToken });
                                    if (syncTask != null)
                                    {
                                        await syncTask.ConfigureAwait(false);
                                        var scheme = syncTask.GetType().GetProperty("Result")?.GetValue(syncTask) as IRedbScheme;
                                        if (scheme != null)
                                        {
                                            obj.SchemeId = scheme.Id;
                                            schemeCache[propsType] = scheme.Id;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                else if (objType == typeof(RedbObject))
                {
                    // Non-generic RedbObject - use Object scheme
                    if (objectSchemeId == null)
                    {
                        var objectScheme = await _schemeSync.EnsureObjectSchemeAsync("RedbObject");
                        objectSchemeId = objectScheme.Id;
                    }
                    obj.SchemeId = objectSchemeId.Value;
                }
            }

            // STEP 4: BATCH permissions check
            if (_configuration.DefaultCheckPermissionsOnSave)
            {
                // Load unique schemes once
                var uniqueSchemeIds = objList.Select(o => o.SchemeId).Distinct().ToArray();
                var schemesList = await _context.QueryAsync<RedbScheme>(
                    Sql.ObjectStorage_SelectSchemesByIds(), new object[] { uniqueSchemeIds }, cancellationToken);
                var schemesDict = schemesList.ToDictionary(s => s.Id, s => s);

                // Check INSERT permissions for new objects
                var newObjectsByScheme = objList.Where(o => o.Id == 0).GroupBy(o => o.SchemeId);
                foreach (var group in newObjectsByScheme)
                {
                    if (schemesDict.TryGetValue(group.Key, out var schemeContract))
                    {
                        var canInsert = await _permissionProvider.CanUserInsertScheme(schemeContract, user);
                        if (!canInsert)
                        {
                            throw new UnauthorizedAccessException($"User {user.Id} has no create permission for objects in scheme {group.Key}");
                        }
                    }
                }

                // Check UPDATE permissions for existing objects
                foreach (var obj in objList.Where(o => o.Id != 0))
                {
                    var canUpdate = await _permissionProvider.CanUserEditObject(obj, user);
                    if (!canUpdate)
                    {
                        throw new UnauthorizedAccessException($"User {user.Id} has no update permission for object {obj.Id}");
                    }
                }
            }

            // === PHASE 2: COLLECTION OF ALL OBJECTS (MAIN + NESTED) ===
            var allObjectsToSave = new List<IRedbObject>();
            var allValuesToSave = new List<RedbValue>();
            var processedObjectIds = new HashSet<long>();
            var referenceStubs = new List<IRedbObject>();

            foreach (var obj in objList)
            {
                await CollectAllObjectsRecursively(obj, allObjectsToSave, processedObjectIds, referenceStubs);
            }

            await ResolveReferenceHashesAsync(referenceStubs, cancellationToken);

            // Interceptors, EF-style (discussion #12, review pass): Saving runs right after the
            // graph is collected and BEFORE ids, hashes and the unchanged-set are computed - so an
            // interceptor may MUTATE Props (EF's own SavingChanges contract) and everything below
            // is derived from what it left. Ran later, an edit here would desynchronize the stored
            // hash from the content and let the F1 shortcut silently skip the edited object. The
            // price is EF's too: objects this save creates still show Id == 0 (temporary-key
            // semantics) - their final ids arrive in SavedAsync. An exception here cancels the
            // save cleanly: no id was burned, nothing touched the database.
            InterceptorChangeSink = null;
            List<IRedbObject>? newObjectRefs = null;
            if (HasSaveInterceptors)
            {
                if (_configuration.PropsSaveStrategy == PropsSaveStrategy.ChangeTracking)
                    InterceptorChangeSink = new List<Interception.RedbValueChange>();
                // "Created by this save" = arrived without an id; the references keep carrying
                // their final ids once assigned below (SavedAsync reads them then).
                newObjectRefs = allObjectsToSave.Where(o => o.Id == 0).ToList();

                await InvokeSavingInterceptorsAsync(new Interception.RedbSavingContext
                {
                    RootObjects = objList,
                    AllObjects = allObjectsToSave,
                    EffectiveUser = user,
                    Strategy = _configuration.PropsSaveStrategy,
                    NewObjects = newObjectRefs,
                }, cancellationToken);
            }

            // === PHASE 3: BATCH PROCESSING (REUSING PROTECTED METHODS) ===
            await AssignMissingIds(allObjectsToSave, user, cancellationToken);

            // V4 (L.2): hashes are final only once ids exist - the parent's hash carries "id:hash" of
            // every reference, so an object created in this save (id 0 until now) would leave "0:"
            // in its parent's persisted hash: unreproducible on reload, a props-cache miss for ever
            // and a hash shift on the first re-save. Children first, then their parents (the
            // collector is pre-order); an existing nested object re-saved through its parent gets
            // its content hash refreshed the same way - its row is rewritten anyway (review).
            if (_configuration.AutoRecomputeHash)
            {
                for (var i = allObjectsToSave.Count - 1; i >= 0; i--)
                    RecomputeHash(allObjectsToSave[i]);
            }
            await EnsureSchemesForAllTypes(allObjectsToSave, cancellationToken);

            // F1 (CT hash shortcut, perf wave 1): under ChangeTracking, existing objects whose
            // recomputed hash equals the persisted one skip the whole value pipeline - building
            // value records, the existing-values SELECT, both trees and the diff. Their _objects
            // row is still updated as before (name/date_modify live their own life). Guarded by
            // AutoRecomputeHash: with recomputation off the in-memory hash cannot be trusted.
            // Objects without a hash (either side) never shortcut.
            ISet<long>? ctUnchangedByHash = null;
            if (ctShortcutActive)
                ctUnchangedByHash = await SelectUnchangedByHashAsync(allObjectsToSave, dbHashById, cancellationToken);


            await ProcessAllObjectsPropertiesRecursively(allObjectsToSave, allValuesToSave, ctUnchangedByHash, cancellationToken);

            // === PHASE 4: STRATEGY SELECTION AND TRANSACTIONAL SAVE ===
            // Wrapped in deadlock retry: ORDER BY _id prevents most deadlocks,
            // but cross-table edge cases may still occur in cluster environments.
            var strategy = _configuration.PropsSaveStrategy;

            // EF pattern: if already in transaction (explicit or ambient TransactionScope) — execute inline
            var diagIsIn = _context.IsInTransaction;
            // var diagAmbient = System.Transactions.Transaction.Current?.TransactionInformation.LocalIdentifier ?? "<null>";
            // System.Console.WriteLine(
            //     $"[Diag-TX-SAVE] DECISION IsInTransaction={diagIsIn} ambient={diagAmbient} " +
            //     $"branch={(diagIsIn ? "INLINE" : "BEGIN-NEW")} thread={System.Environment.CurrentManagedThreadId} " +
            //     $"objsToSave={allObjectsToSave.Count}");
            if (diagIsIn)
            {
                // Lock existing objects within the caller's transaction
                var existingObjectIds = allObjectsToSave.Where(o => o.Id > 0).Select(o => o.Id).ToArray();
                if (existingObjectIds.Any())
                {
                    await _context.ExecuteAsync(
                        Sql.ObjectStorage_LockObjectsForUpdate(), new object[] { existingObjectIds }, cancellationToken);
                }

                // The caller owns the transaction, and a unique violation must not leave it aborted
                // (PostgreSQL 25P02: every follow-up statement of a catch-and-recover pattern died,
                // SaveByUniqueAsync's own retry included - bug report п.2, 2026-09-02). The batch
                // runs under a savepoint; the violation rolls back TO it - the transaction stays
                // alive - and only then the typed exception leaves. Other failures propagate as
                // before: they are not the documented recoverable case.
                await _context.ExecuteAsync(Sql.Transaction_SavepointBegin(), System.Array.Empty<object>(), cancellationToken);
                try
                {
                    await ExecuteBatchByStrategy(strategy, allObjectsToSave, allValuesToSave, ctUnchangedByHash, cancellationToken);
                    if (Sql.Transaction_SavepointRelease() is { } release)
                        await _context.ExecuteAsync(release, System.Array.Empty<object>(), cancellationToken);
                }
                catch (Exception ex) when (Data.DbErrorClassifier.IsUniqueViolation(ex))
                {
                    // Roll back BEFORE translating: the translation itself queries the database.
                    // Deliberately WITHOUT ct (s3.4): the rollback must land even on a cancelled token.
                    await _context.ExecuteAsync(Sql.Transaction_SavepointRollback());
                    throw await TranslateUniqueViolationAsync(ex, null);
                }
            }
            else
            {
                // Top-level: wrap in DeadlockRetry + Transaction + FOR UPDATE
                await DeadlockRetryHelper.ExecuteWithRetryAsync(async () =>
                {
                    await using var transaction = await _context.BeginTransactionAsync(cancellationToken: cancellationToken);

                    try
                    {
                        // FOR UPDATE: lock ALL existing objects to prevent race condition
                        var existingObjectIds = allObjectsToSave.Where(o => o.Id > 0).Select(o => o.Id).ToArray();
                        if (existingObjectIds.Any())
                        {
                            await _context.ExecuteAsync(
                                Sql.ObjectStorage_LockObjectsForUpdate(), new object[] { existingObjectIds }, cancellationToken);
                        }

                        await ExecuteBatchByStrategy(strategy, allObjectsToSave, allValuesToSave, ctUnchangedByHash, cancellationToken);

                        // The point of no return (s3.3): the LAST ct read - after a successful commit
                        // nothing below ever consults the token again.
                        cancellationToken.ThrowIfCancellationRequested();
                        await transaction.CommitAsync();
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();

                        if (Data.DbErrorClassifier.IsUniqueViolation(ex))
                            throw await TranslateUniqueViolationAsync(ex, null);

                        throw;
                    }
                }, cancellationToken: cancellationToken);
            }

            // === PHASE 5: CACHE UPDATE ===
            if (_configuration.EnablePropsCache && PropsCache.Instance != null)
            {
                foreach (var savedObj in allObjectsToSave)
                {
                    if (savedObj.Hash.HasValue)
                    {
                        var objType = savedObj.GetType();
                        if (objType.IsGenericType && objType.GetGenericTypeDefinition() == typeof(RedbObject<>))
                        {
                            var propsType = objType.GetGenericArguments()[0];
                            var setMethod = typeof(GlobalPropsCache).GetMethod("Set")?.MakeGenericMethod(propsType);
                            setMethod?.Invoke(PropsCache, new[] { savedObj });
                        }
                    }
                }
            }

            var savedIds = objList.Select(o => o.Id).ToList();

            // Saved runs AFTER the commit: the data is in, and (EF-style) an exception from an
            // interceptor propagates to the caller - but cannot un-commit anything.
            if (HasSaveInterceptors)
            {
                var changes = InterceptorChangeSink;
                InterceptorChangeSink = null;
                await InvokeSavedInterceptorsAsync(new Interception.RedbSavedContext
                {
                    RootObjects = objList,
                    AllObjects = allObjectsToSave,
                    EffectiveUser = user,
                    Strategy = _configuration.PropsSaveStrategy,
                    SavedIds = savedIds,
                    // The references captured before id assignment now carry their final ids.
                    NewObjectIds = newObjectRefs!.Select(o => o.Id).ToHashSet(),
                    UnchangedByHash = ctUnchangedByHash as IReadOnlySet<long>,
                    Changes = changes,
                }, CancellationToken.None); // past the commit - cancelling the notification would lie (s3.3)
            }

            return savedIds;
        }

        /// <summary>
        /// The object hash as the save writes it: from Props when they are loaded, from the base
        /// fields when the object has none (Props null, or the non-generic RedbObject).
        /// </summary>
        private static void RecomputeHash(IRedbObject obj)
        {
            var objType = obj.GetType();
            var isGeneric = objType.IsGenericType &&
                            objType.GetGenericTypeDefinition() == typeof(RedbObject<>);

            if (isGeneric)
            {
                // Raw Props: a collected object is loaded by definition, but never risk the getter.
                var propsValue = objType.GetMethod("GetPropsDirectly")?.Invoke(obj, null);
                if (propsValue != null)
                {
                    var currentHash = RedbHash.ComputeFor(obj);
                    if (currentHash.HasValue)
                        obj.Hash = currentHash.Value;
                }
                else
                {
                    obj.Hash = RedbHash.ComputeForBaseFields(obj);
                }
            }
            else
            {
                obj.Hash = RedbHash.ComputeForBaseFields(obj);
            }
        }

        /// <summary>
        /// V4 (L.2): a reference written by hand as <c>{ id = x }</c> carries no hash, while the
        /// parent hashes "id:hash" of every reference and writes that hash into the reference row
        /// (<c>_Guid</c>). One query resolves the persisted hashes, so the first save writes the
        /// same parent hash every reload will compute - no first-resave shift, no perpetual
        /// props-cache miss (review).
        /// </summary>
        private async Task ResolveReferenceHashesAsync(List<IRedbObject> referenceStubs, CancellationToken cancellationToken)
        {
            if (referenceStubs.Count == 0) return;

            var ids = referenceStubs.Select(s => s.Id).Distinct().ToArray();
            var rows = await _context.QueryAsync<RedbObjectRow>(Sql.ObjectStorage_SelectObjectsByIds(), new object[] { ids }, cancellationToken);
            var hashById = new Dictionary<long, Guid?>();
            foreach (var row in rows)
                hashById[row.Id] = row.Hash;

            foreach (var stub in referenceStubs)
            {
                if (hashById.TryGetValue(stub.Id, out var hash))
                    stub.Hash = hash;
            }
        }

        /// <summary>
        /// Executes batch save by strategy. OpenSource: only DeleteInsert.
        /// Pro: override to support ChangeTracking.
        /// F1: <paramref name="ctUnchangedByHash"/> lists existing objects excluded from the value
        /// pipeline by the hash shortcut - only the ChangeTracking strategy consults it (their
        /// _objects row still updates; their values were never built and must not be diffed).
        /// </summary>
        protected virtual async Task ExecuteBatchByStrategy(
            PropsSaveStrategy strategy,
            List<IRedbObject> allObjectsToSave,
            List<RedbValue> allValuesToSave,
            ISet<long>? ctUnchangedByHash = null,
            CancellationToken cancellationToken = default)
        {
            if (strategy == PropsSaveStrategy.ChangeTracking)
            {
                throw new NotSupportedException(
                    "PropsSaveStrategy.ChangeTracking (batch) is not implemented in this provider. " +
                    "Use PropsSaveStrategy.DeleteInsert.");
            }

            await SaveBatchWithDeleteInsertStrategy(allObjectsToSave, allValuesToSave, cancellationToken);
        }

        // ===== DELETEINSERT STRATEGY =====

        /// <summary>
        /// DeleteInsert batch strategy: delete ALL values, BulkInsert/BulkUpdate of objects, BulkInsert of values.
        /// </summary>
        protected async Task SaveBatchWithDeleteInsertStrategy(
            List<IRedbObject> allObjectsToSave,
            List<RedbValue> allValuesToSave,
            CancellationToken cancellationToken = default)
        {
            // Step 1: Determine which objects ACTUALLY exist in the DB
            var allIds = allObjectsToSave.Select(o => o.Id).ToArray();
            var existingIdsInDb = allIds.Any()
                ? await _context.QueryScalarListAsync<long>(Sql.ObjectStorage_SelectExistingIds(), new object[] { allIds }, cancellationToken)
                : [];
            var existingIdsSet = existingIdsInDb.ToHashSet();

            // Step 2: Delete all old values
            if (existingIdsInDb.Any())
            {
                await _context.Bulk.BulkDeleteValuesByObjectIdsAsync(existingIdsInDb, cancellationToken);
            }

            // Step 3: Separation and BulkInsert of new objects
            var newObjects = allObjectsToSave.Where(o => !existingIdsSet.Contains(o.Id)).ToList();
            var existingObjects = allObjectsToSave.Where(o => existingIdsSet.Contains(o.Id)).ToList();

            if (newObjects.Any())
            {
                var newRecords = newObjects.Select(ConvertToObjectRecord).ToList();
                await _context.Bulk.BulkInsertObjectsAsync(newRecords, cancellationToken);
            }

            // Step 4: BulkUpdate of existing objects
            if (existingObjects.Any())
            {
            // P7 (UNIQUE stage 1): release the keys of EVERY existing object in the batch before the
            // row updates run - an in-batch key exchange otherwise trips the unique index mid-way.
            // Every existing object, not only those taking a key: the one giving its key up (new key
            // NULL) must release it before the one taking it is written. The statement touches rows
            // that hold a key only. Same transaction as the updates, so no outside reader sees a gap.
            var existingIdsToRelease = existingObjects.Select(o => o.Id).ToList();
            if (existingIdsToRelease.Count > 0)
                await _context.ExecuteAsync(Sql.ObjectStorage_ClearValueUniqueByIds(existingIdsToRelease), System.Array.Empty<object>(), cancellationToken);

                var existingRecords = existingObjects.Select(ConvertToObjectRecord).ToList();
                await _context.Bulk.BulkUpdateObjectsAsync(existingRecords, cancellationToken);
            }

            // Step 5: BulkInsert of all values
            if (allValuesToSave.Any())
            {
                var deduplicated = DeduplicateValueInserts(allValuesToSave, "SaveBatchWithDeleteInsertStrategy");
                var sortedValues = ValuesTopologicalSort.SortByFkDependency(deduplicated);
                await _context.Bulk.BulkInsertValuesAsync(sortedValues, cancellationToken);
            }
        }

    }
}

