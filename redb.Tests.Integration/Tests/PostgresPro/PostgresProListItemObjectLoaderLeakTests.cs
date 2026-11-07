using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.PostgresPro;

[Collection("PostgresPro")]
public class PostgresProListItemObjectLoaderLeakTests : PostgresListItemObjectLoaderLeakTestsBase
{
    protected override bool IsPro => true;
}
