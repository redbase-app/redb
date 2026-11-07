using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.Postgres;

[Collection("Postgres")]
public class PostgresListItemObjectLoaderLeakTests : PostgresListItemObjectLoaderLeakTestsBase
{
    protected override bool IsPro => false;
}
