using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.MsSql;

/// <summary>
/// sp_mark_for_deletion runs under SET XACT_ABORT ON (trash review, 2026-09-14). Without it a client
/// cancel or a command timeout arriving inside the procedure's transaction bypasses CATCH and leaves the
/// transaction open on the session - after the connection returns to the pool as well - so every later
/// statement of that session runs inside it and is rolled back with it. The pin blocks the procedure on
/// a row lock held by another session, lets the command time out right there, and checks the session.
/// </summary>
[Collection("MsSql")]
public class MsSqlSoftDeleteProcedureTests
{
    private readonly MsSqlFixture _fixture;

    public MsSqlSoftDeleteProcedureTests(MsSqlFixture fixture) => _fixture = fixture;

    private static string ConnString() =>
        new SqlConnectionStringBuilder(
            new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .Build()
                .GetConnectionString("MSSql")!)
        { Pooling = false }.ConnectionString;

    [Fact]
    public async Task MarkForDeletion_TimedOutInsideItsTransaction_LeavesNoTransactionOnTheSession()
    {
        await _fixture.Redb.SyncSchemeAsync<TrashTargetProps>();
        var id = await _fixture.Redb.SaveAsync(new RedbObject<TrashTargetProps>
            { name = "trash-xact-abort-probe", Props = new TrashTargetProps { Title = "probe" } });
        var userId = _fixture.Redb.SecurityContext.GetEffectiveUser().Id;

        await using var holder = new SqlConnection(ConnString());
        await holder.OpenAsync();
        await using var held = (SqlTransaction)await holder.BeginTransactionAsync();
        await using (var hold = new SqlCommand("UPDATE [dbo].[_objects] SET [_note] = N'held' WHERE [_id] = @id", holder, held))
        {
            hold.Parameters.AddWithValue("@id", id);
            await hold.ExecuteNonQueryAsync();
        }

        await using var session = new SqlConnection(ConnString());
        await session.OpenAsync();
        await using (var mark = new SqlCommand(
            "DECLARE @trash BIGINT, @marked BIGINT; EXEC [dbo].[sp_mark_for_deletion] @ids, @user, NULL, @trash OUTPUT, @marked OUTPUT;",
            session) { CommandTimeout = 2 })
        {
            mark.Parameters.AddWithValue("@ids", id.ToString());
            mark.Parameters.AddWithValue("@user", userId);
            var act = async () => await mark.ExecuteNonQueryAsync();
            await act.Should().ThrowAsync<SqlException>("the procedure waits on the held row until the command times out");
        }

        await using (var probe = new SqlCommand("SELECT @@TRANCOUNT", session))
            Convert.ToInt32(await probe.ExecuteScalarAsync()).Should().Be(0,
                "a timeout inside the procedure must roll its transaction back, not leave it on the session");

        await held.RollbackAsync();
        (await _fixture.Redb.Context.ExecuteScalarAsync<long?>($"SELECT _id_scheme FROM _objects WHERE _id = {id}"))
            .Should().NotBe(-10, "the timed-out mark changed nothing");
        await _fixture.Redb.DeleteAsync(id);
    }
}
