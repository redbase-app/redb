using Microsoft.Extensions.DependencyInjection;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Tests.Base;

namespace redb.Tests.Integration.Tests.SqlitePro;

public class SqliteProSchemaNewerTests : SqliteSchemaNewerTestsBase
{
    protected override void AddRedbServices(IServiceCollection services, string connectionString)
        => services.AddRedbPro(options => options.UseSqlite(connectionString));
}
