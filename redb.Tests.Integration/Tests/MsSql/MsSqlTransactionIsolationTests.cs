using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.MsSql;

[Collection("MsSql")]
public class MsSqlTransactionIsolationTests : TransactionIsolationTestsBase
{
    public MsSqlTransactionIsolationTests(MsSqlFixture fixture) : base(fixture.Redb) { }

    protected override string? ReadLevelSql => """
        SELECT CASE transaction_isolation_level
                   WHEN 1 THEN 'read uncommitted' WHEN 2 THEN 'read committed'
                   WHEN 3 THEN 'repeatable read' WHEN 4 THEN 'serializable'
                   WHEN 5 THEN 'snapshot' END
        FROM sys.dm_exec_sessions WHERE session_id = @@SPID
        """;
}
