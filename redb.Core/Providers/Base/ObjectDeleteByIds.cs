using System.Threading;
using System.Threading.Tasks;
using redb.Core.Data;
using redb.Core.Query;

namespace redb.Core.Providers.Base;

/// <summary>
/// The delete of objects by ids (<see cref="ISqlDialect.ObjectStorage_DeleteByIds"/>) and the number of the given objects
/// it deleted. Where the delete's rows-affected misses some (SQLite: a child the <c>_id_parent</c> cascade removed with
/// its parent before the statement reached it), the dialect's <see cref="ISqlDialect.ObjectStorage_CountObjectsByIds"/>
/// counts them in the same transaction, before the delete.
/// </summary>
internal static class ObjectDeleteByIds
{
    /// <summary>
    /// Run again on a deadlock when the delete is the caller's whole unit (no transaction on the context); inside the
    /// caller's transaction the deadlock leaves to the caller (<see cref="DeadlockRetryHelper"/>).
    /// </summary>
    public static Task<int> RunAsync(IRedbContext context, ISqlDialect sql, long[] ids, CancellationToken cancellationToken)
        => DeadlockRetryHelper.ExecuteWithRetryAsync(context, () => RunOnceAsync(context, sql, ids, cancellationToken),
            cancellationToken: cancellationToken);

    private static async Task<int> RunOnceAsync(IRedbContext context, ISqlDialect sql, long[] ids, CancellationToken cancellationToken)
    {
        var countSql = sql.ObjectStorage_CountObjectsByIds();
        if (countSql == null)
            return await context.ExecuteAsync(sql.ObjectStorage_DeleteByIds(), new object[] { ids }, cancellationToken);

        var counted = 0;
        await context.ExecuteAtomicAsync(async () =>
        {
            counted = checked((int)await context.ExecuteScalarAsync<long>(countSql, new object[] { ids }, cancellationToken));
            await context.ExecuteAsync(sql.ObjectStorage_DeleteByIds(), new object[] { ids }, cancellationToken);
        }, cancellationToken);
        return counted;
    }
}
