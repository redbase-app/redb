using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using redb.Core.Data;
using redb.Core.Models.Configuration;

namespace redb.Core.Query.Base;

/// <summary>
/// V4 (LAZY Л2): runs one query execution under a per-query <c>WithLazyReferences</c> override.
/// The override rides the same session flag the builders read (<see cref="ISqlDialect.Session_SetLazyRefs"/>):
/// armed before the execution, restored to the global option after it. One implementation for the
/// flat and the tree providers, so the two cannot drift in how they arm, degrade and restore.
/// </summary>
internal static class LazyReferencesQueryScope
{
    public static async Task<T> RunAsync<T>(
        IRedbContext context,
        ISqlDialect sql,
        RedbServiceConfiguration configuration,
        ILogger? logger,
        bool? lazyOverride,
        Func<Task<T>> execute)
    {
        var toggled = false;
        if (lazyOverride.HasValue && lazyOverride.Value != configuration.EnableLazyReferences)
        {
            // A provider without the channel (Pro SQLite has no session mechanism and may run
            // without the native extension) degrades to the GLOBAL option - loudly, not silently.
            try
            {
                await context.ExecuteScalarAsync<object>(sql.Session_SetLazyRefs(), lazyOverride.Value ? 1 : 0);
                toggled = true;
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex,
                    "WithLazyReferences: the per-query channel is unavailable on this provider; falling back to the global option (recorded L2 boundary).");
            }
        }

        try
        {
            return await execute();
        }
        finally
        {
            if (toggled)
            {
                // The restore must never replace the execution's own exception: inside an aborted
                // transaction PostgreSQL refuses every statement (25P02) and would mask the real
                // fault - and rolls the flag back with the transaction anyway. MSSQL keeps
                // SESSION_CONTEXT outside transactions, so the restore is attempted and logged.
                try
                {
                    await context.ExecuteScalarAsync<object>(sql.Session_SetLazyRefs(), configuration.EnableLazyReferences ? 1 : 0);
                }
                catch (Exception ex)
                {
                    logger?.LogWarning(ex,
                        "WithLazyReferences: could not restore the session flag to the global option after the query.");
                }
            }
        }
    }
}
