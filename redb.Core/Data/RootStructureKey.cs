using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Core.Utils;

namespace redb.Core.Data
{
    /// <summary>
    /// The unique key of the root structures of a scheme: one field name once per scheme. The key of
    /// <c>_structures (_id_scheme, _name, _id_parent)</c> lets a root field in twice on PostgreSQL and SQLite, where two
    /// NULL parents are distinct, and nodes syncing one scheme at once used to do exactly that (SQL Server counts NULLs
    /// as equal and refused it). The SQL is the dialect's (the "root structure key" group of <see cref="ISqlDialect"/>);
    /// this class decides.
    ///
    /// <para>
    /// A database written before the schema lock may hold such duplicates already, and the key cannot be built over
    /// them. A duplicate nothing points to (no value, no child with a value, no structure overriding it, no migration
    /// record) is removed with its subtree; one with values is left alone, and the key waits until the duplicates are
    /// resolved by hand (UPGRADING.md, "Duplicate root structures"). Run under the schema lock, in one transaction.
    /// </para>
    /// </summary>
    public static class RootStructureKey
    {
        /// <summary>Name of the unique key, as the dialects create it.</summary>
        public const string IndexName = "UX__structures__root";

        /// <summary>
        /// Removes the empty root duplicates and builds the key when none with values remain. Called only for a dialect
        /// whose <see cref="ISqlDialect.Structures_RootKeyExists"/> is not null.
        /// </summary>
        /// <param name="context">The context of the service; joins its transaction or opens one.</param>
        /// <param name="sql">The dialect.</param>
        /// <param name="hasMigrationsTable">Whether <c>_migrations</c> exists; its rows point to structures.</param>
        /// <param name="logger">Where removals are reported.</param>
        /// <param name="cancellationToken">Cancellation.</param>
        /// <returns>True when the key exists afterwards.</returns>
        public static async Task<bool> EnsureAsync(
            IRedbContext context, ISqlDialect sql, bool hasMigrationsTable, ILogger? logger,
            CancellationToken cancellationToken = default)
        {
            IRedbTransaction? own = context.IsInTransaction
                ? null
                : await context.BeginTransactionAsync(cancellationToken: cancellationToken);
            try
            {
                var created = await EnsureInTransactionAsync(context, sql, hasMigrationsTable, logger, cancellationToken);
                if (own != null)
                    await own.CommitAsync();
                return created;
            }
            catch
            {
                if (own != null)
                    await own.RollbackAsync();
                throw;
            }
            finally
            {
                if (own != null)
                    await own.DisposeAsync();
            }
        }

        private static async Task<bool> EnsureInTransactionAsync(
            IRedbContext context, ISqlDialect sql, bool hasMigrationsTable, ILogger? logger, CancellationToken cancellationToken)
        {
            var roots = await context.QueryAsync<RedbStructure>(
                sql.Structures_SelectRootDuplicates(), Array.Empty<object>(), cancellationToken);
            // PostgreSQL: the foreign key of _values cascades on delete, so a node of the previous version writing into
            // a duplicate between the look and the delete would lose its value. Locked first, its insert waits and then
            // fails on the key. SQLite: no lock statement, the schema lock is the only writer.
            var lockSubtree = sql.Structures_LockSubtree();

            var unresolved = new List<string>();
            var touchedSchemes = new HashSet<long>();
            foreach (var group in roots.GroupBy(r => (r.IdScheme, r.Name)))
            {
                var used = new List<RedbStructure>();
                var empty = new List<RedbStructure>();
                foreach (var root in group)
                {
                    if (lockSubtree != null)
                        await context.ExecuteAsync(lockSubtree, new object[] { root.Id }, cancellationToken);
                    // Asked after the lock, in a statement of its own: it sees every value committed before the lock.
                    var referenced = await context.ExecuteScalarAsync<long>(
                        sql.Structures_SubtreeIsReferenced(hasMigrationsTable), new object[] { root.Id }, cancellationToken);
                    (referenced == 1 ? used : empty).Add(root);
                }

                // One copy stays: the one with values, or the first one when none has any.
                var keep = used.Count > 0 ? used : new List<RedbStructure> { empty[0] };
                var remove = empty.Where(e => !keep.Contains(e)).ToList();
                var schemeName = await SchemeNameAsync(context, sql, group.Key.IdScheme, cancellationToken);

                foreach (var duplicate in remove)
                {
                    await context.ExecuteAsync(sql.Structures_DeleteSubtreeMetadataCache(), new object[] { duplicate.Id }, cancellationToken);
                    // The children go with it (ON DELETE CASCADE of _id_parent); none of them holds a value.
                    await context.ExecuteAsync(sql.Structures_DeleteById(), new object[] { duplicate.Id }, cancellationToken);
                    touchedSchemes.Add(group.Key.IdScheme);
                }
                if (remove.Count > 0)
                    logger?.LogInformation(
                        "REDB schema upgrade: removed {Count} empty duplicate(s) of root field '{Field}' of scheme '{Scheme}' " +
                        "(id {SchemeId}): structure id(s) {Ids}; kept {Kept}.",
                        remove.Count, group.Key.Name, schemeName, group.Key.IdScheme,
                        string.Join(", ", remove.Select(r => r.Id)), string.Join(", ", keep.Select(k => k.Id)));

                if (used.Count > 1)
                    unresolved.Add($"'{schemeName}' (id {group.Key.IdScheme}) field '{group.Key.Name}': structures " +
                                   string.Join(", ", used.Select(u => u.Id)));
            }

            foreach (var schemeId in touchedSchemes)
            {
                // The nodes compare the hash with their cached structure tree: a new one makes them reload it.
                var structures = await context.QueryAsync<RedbStructure>(
                    sql.Structures_SelectBySchemeShort(), new object[] { schemeId }, cancellationToken);
                await context.ExecuteAsync(sql.Schemes_UpdateHash(),
                    new object[] { SchemeHashCalculator.ComputeSchemeStructureHash(structures), schemeId }, cancellationToken);
                await context.ExecuteAsync(sql.Schemes_SyncMetadataCache(), new object[] { schemeId }, cancellationToken);
            }

            if (unresolved.Count > 0)
            {
                // Each remaining duplicate is logged as an error by the schema check of the start that follows
                // (FindDuplicatesAsync), at every start until resolved.
                logger?.LogInformation(
                    "REDB schema upgrade: the unique key of root structures ({Index}) is not built while duplicates holding " +
                    "values remain: {Duplicates}.", IndexName, string.Join("; ", unresolved));
                return false;
            }

            await context.ExecuteAsync(sql.Structures_CreateRootKey(), Array.Empty<object>(), cancellationToken);
            logger?.LogInformation("REDB schema upgrade: the unique key of root structures ({Index}) is built.", IndexName);
            return true;
        }

        /// <summary>
        /// The root duplicates in the database, one problem per field (read only). Asked only while the key is missing:
        /// with it, there can be none.
        /// </summary>
        public static async Task<IReadOnlyList<Models.RedbSchemaProblem>> FindDuplicatesAsync(
            IRedbContext context, ISqlDialect sql, CancellationToken cancellationToken = default)
        {
            var roots = await context.QueryAsync<RedbStructure>(
                sql.Structures_SelectRootDuplicates(), Array.Empty<object>(), cancellationToken);

            var problems = new List<Models.RedbSchemaProblem>();
            foreach (var group in roots.GroupBy(r => (r.IdScheme, r.Name)))
            {
                var schemeName = await SchemeNameAsync(context, sql, group.Key.IdScheme, cancellationToken);
                problems.Add(DuplicateProblem(group.Key.IdScheme, schemeName, group.Key.Name, group.Select(r => r.Id).ToList()));
            }
            return problems;
        }

        /// <summary>The problem, and its logged text, for one root field found more than once.</summary>
        public static Models.RedbSchemaProblem DuplicateProblem(long schemeId, string schemeName, string field, IReadOnlyList<long> ids)
            => new(Models.RedbSchemaProblemKind.DuplicateRootStructure, schemeId, schemeName, field, ids,
                $"REDB schema problem: root field '{field}' of scheme '{schemeName}' (id {schemeId}) exists {ids.Count} times " +
                $"(structures {string.Join(", ", ids)}), and objects may hold values on more than one copy. The unique key " +
                $"of root structures ({IndexName}) is not built until it is resolved. Resolve it by hand as described in " +
                "UPGRADING.md, section \"Duplicate root structures\"; this error repeats at every start until then.");

        private static async Task<string> SchemeNameAsync(IRedbContext context, ISqlDialect sql, long schemeId, CancellationToken cancellationToken)
            => (await context.QueryFirstOrDefaultAsync<RedbScheme>(sql.Schemes_SelectById(), new object[] { schemeId }, cancellationToken))
                ?.Name ?? "";
    }
}
