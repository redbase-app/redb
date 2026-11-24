using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>Row-lock holders for <see cref="ConcurrentDeleteCountTestsBase"/>: a session of its own per provider.</summary>
internal static class RowHolders
{
    private static string Cs(string name) =>
        new ConfigurationBuilder().SetBasePath(AppContext.BaseDirectory).AddJsonFile("appsettings.json").Build().GetConnectionString(name)!;

    /// <summary>SQL Server: UPDLOCK on the row; the waiter is found in sys.dm_exec_requests by its blocking session.</summary>
    public static async Task<ConcurrentDeleteCountTestsBase.IRowHolder> SqlServerAsync(long objectId)
    {
        var cs = new SqlConnectionStringBuilder(Cs("MSSql")) { Pooling = false }.ConnectionString;
        var conn = new SqlConnection(cs);
        await conn.OpenAsync();
        var tx = (SqlTransaction)await conn.BeginTransactionAsync();
        await using (var hold = new SqlCommand("SELECT _id FROM _objects WITH (UPDLOCK, ROWLOCK) WHERE _id = @id", conn, tx))
        {
            hold.Parameters.AddWithValue("@id", objectId);
            await hold.ExecuteScalarAsync();
        }
        int spid;
        await using (var me = new SqlCommand("SELECT @@SPID", conn, tx))
            spid = Convert.ToInt32(await me.ExecuteScalarAsync());

        return new Holder(
            async timeout =>
            {
                await using var probe = new SqlConnection(cs);
                await probe.OpenAsync();
                await using var cmd = new SqlCommand("SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id = @spid", probe);
                cmd.Parameters.AddWithValue("@spid", spid);
                return await PollAsync(async () => Convert.ToInt32(await cmd.ExecuteScalarAsync()) > 0, timeout);
            },
            async sql =>
            {
                await using (var run = new SqlCommand(sql, conn, tx))
                    await run.ExecuteNonQueryAsync();
                await tx.CommitAsync();
            },
            async () => { await tx.DisposeAsync(); await conn.DisposeAsync(); });
    }

    /// <summary>PostgreSQL: SELECT ... FOR UPDATE; the waiter is found through pg_blocking_pids.</summary>
    public static async Task<ConcurrentDeleteCountTestsBase.IRowHolder> PostgresAsync(long objectId)
    {
        var cs = new NpgsqlConnectionStringBuilder(Cs("Postgres")) { Pooling = false }.ConnectionString;
        var conn = new NpgsqlConnection(cs);
        await conn.OpenAsync();
        var tx = await conn.BeginTransactionAsync();
        await using (var hold = new NpgsqlCommand("SELECT _id FROM _objects WHERE _id = $1 FOR UPDATE", conn, tx))
        {
            hold.Parameters.AddWithValue(objectId);
            await hold.ExecuteScalarAsync();
        }
        int pid;
        await using (var me = new NpgsqlCommand("SELECT pg_backend_pid()", conn, tx))
            pid = Convert.ToInt32(await me.ExecuteScalarAsync());

        return new Holder(
            async timeout =>
            {
                await using var probe = new NpgsqlConnection(cs);
                await probe.OpenAsync();
                await using var cmd = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", probe);
                cmd.Parameters.AddWithValue(pid);
                return await PollAsync(async () => Convert.ToInt64(await cmd.ExecuteScalarAsync()) > 0, timeout);
            },
            async sql =>
            {
                await using (var run = new NpgsqlCommand(sql, conn, tx))
                    await run.ExecuteNonQueryAsync();
                await tx.CommitAsync();
            },
            async () => { await tx.DisposeAsync(); await conn.DisposeAsync(); });
    }

    private static async Task<bool> PollAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var until = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < until)
        {
            if (await condition()) return true;
            await Task.Delay(50);
        }
        return false;
    }

    private sealed class Holder(Func<TimeSpan, Task<bool>> wait, Func<string, Task> commit, Func<Task> dispose)
        : ConcurrentDeleteCountTestsBase.IRowHolder
    {
        public async Task WaitUntilSomeoneWaitsAsync(TimeSpan timeout)
        {
            if (!await wait(timeout))
                throw new TimeoutException("precondition: the delete never waited on the held row lock");
        }

        public Task ExecuteAndCommitAsync(string sql) => commit(sql);

        public async ValueTask DisposeAsync() => await dispose();
    }
}
