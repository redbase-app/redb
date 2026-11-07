using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core.Pro.Extensions;
using redb.SQLite.Data;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.SqlitePro;

/// <summary>
/// The same pre-V4 file, opened by a Pro build. Pro used to skip the whole deployment pass on the
/// grounds that its queries are generated in C# - true of the module's functions, but the pass also
/// carries the schema upgrades, so a pre-V4 file stayed without _values._unique and every start
/// died on the first read of a scheme. Each test gets its own file (the base class owns the path),
/// so this runs beside the Free suite.
/// </summary>
public class SqliteProUniqueUpgradeTests : redb.Tests.Integration.Tests.Sqlite.SqliteUniqueUpgradeTests
{
    protected override ServiceProvider BuildServices(string connectionString)
    {
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        // UseSqlite resolves to the Pro overload through this file's using of redb.SQLite.Pro.Extensions.
        services.AddRedbPro(options => options.UseSqlite(connectionString));
        return services.BuildServiceProvider();
    }
}
