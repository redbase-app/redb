using redb.Core;
using redb.Core.Data;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Services syncing one scheme at the same moment, as the nodes of a Tsak cluster do when they load modules that share
/// a Props class: a scheme new to the database (odd rounds) and a scheme an older version left without a field (even
/// rounds: the field and the nested part are deleted first). Without the schema lock they inserted the same structure
/// and the second failed on the unique key of _structures.
/// </summary>
public static class SchemeSyncRace
{
    private const string SchemeName = "schema_lock_sync_probe";

    /// <summary>Runs the rounds; returns the failures, each with its round.</summary>
    public static async Task<List<string>> RunAsync(IReadOnlyList<IRedbService> nodes, IRedbContext context, int rounds)
    {
        var failures = new List<string>();
        for (var round = 0; round < rounds; round++)
        {
            await nodes[0].SyncSchemeAsync<SchemaLockSyncProbeProps>();
            var schemeId = await context.ExecuteScalarAsync<long>($"SELECT _id FROM _schemes WHERE _name = '{SchemeName}'");
            if (round % 2 == 1)
            {
                // A new scheme: the database has never seen it.
                await context.ExecuteAsync($"DELETE FROM _structures WHERE _id_scheme = {schemeId} AND _id_parent IS NOT NULL");
                await context.ExecuteAsync($"DELETE FROM _structures WHERE _id_scheme = {schemeId}");
                await context.ExecuteAsync($"DELETE FROM _scheme_metadata_cache WHERE _scheme_id = {schemeId}");
                await context.ExecuteAsync($"DELETE FROM _schemes WHERE _id = {schemeId}");
            }
            else
            {
                // A scheme of the previous version of the module: no Note, no Part.
                await context.ExecuteAsync(
                    $"DELETE FROM _structures WHERE _id_scheme = {schemeId} AND _id_parent IS NOT NULL");
                await context.ExecuteAsync(
                    $"DELETE FROM _structures WHERE _id_scheme = {schemeId} AND _name IN ('Note', 'Part')");
            }

            // Released together, so the syncs really meet.
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var syncs = nodes.Select(node => Task.Run(async () =>
            {
                await start.Task;
                try
                {
                    await node.SyncSchemeAsync<SchemaLockSyncProbeProps>();
                    return null;
                }
                catch (Exception ex)
                {
                    return $"round {round}: {ex.GetType().Name}: {ex.Message}";
                }
            })).ToList();
            start.SetResult();
            failures.AddRange((await Task.WhenAll(syncs)).Where(f => f != null).Select(f => f!));

            var newId = await context.ExecuteScalarAsync<long>($"SELECT _id FROM _schemes WHERE _name = '{SchemeName}'");
            var structures = await context.ExecuteScalarAsync<long>(
                $"SELECT CAST(COUNT(*) AS BIGINT) FROM _structures WHERE _id_scheme = {newId}");
            var cached = await context.ExecuteScalarAsync<long>(
                $"SELECT CAST(COUNT(*) AS BIGINT) FROM _scheme_metadata_cache WHERE _scheme_id = {newId}");
            // Title, Count, Note, Part and the two fields of Part.
            if (structures != 6)
                failures.Add($"round {round}: {structures} structures instead of 6");
            if (cached != structures)
                failures.Add($"round {round}: {cached} metadata cache rows for {structures} structures");
        }
        return failures;
    }
}
