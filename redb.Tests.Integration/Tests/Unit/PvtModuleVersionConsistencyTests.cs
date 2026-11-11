using System.Reflection;
using System.Text.RegularExpressions;
using redb.Core.Query;
using redb.MSSql.Pro.Sql;
using redb.MSSql.Sql;
using redb.Postgres.Pro.Sql;
using redb.Postgres.Sql;
using redb.SQLite.Pro.Sql;
using redb.SQLite.Sql;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The version a dialect requires of the database module is the version the shipped module reports. Start-up compares
/// the two for equality: when they differ, every start applies the module again, and a start that may not change the
/// schema (AutoApplyDatabaseUpgrades off, or a role without the rights) stops with RedbSchemaOutdatedException for good.
/// SQL Server shipped that way from 4.6.1 to 4.7.0: the dialect asked for 0.2.40, the module still answered 0.2.39.
/// No database is needed: the module text is read from the assembly (PostgreSQL, SQL Server) or from the source of the
/// native extension (SQLite).
/// </summary>
public class PvtModuleVersionConsistencyTests
{
    private static readonly Regex ModuleVersionFunction = new(
        @"FUNCTION\s+(?:\[?dbo\]?\.)?\[?pvt_module_version\]?[\s\S]*?RETURN\s+N?'(?<v>\d+\.\d+\.\d+)'",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex NativeModuleVersion = new(
        @"#define\s+PVT_MODULE_VERSION\s+""(?<v>\d+\.\d+\.\d+)""", RegexOptions.CultureInvariant);

    [Fact]
    public void SqlServer_ModuleReportsTheVersionTheDialectRequires()
    {
        var shipped = VersionInBundle(typeof(redb.MSSql.RedbService).Assembly, "redb.MSSql.sql.v2-pvt.pvt_bundle.sql");

        Required(new MsSqlDialect()).Should().Be(shipped);
        Required(new ProMsSqlDialect()).Should().Be(shipped);
    }

    [Fact]
    public void PostgreSql_ModuleReportsTheVersionTheDialectRequires()
    {
        var shipped = VersionInBundle(typeof(redb.Postgres.RedbService).Assembly, "redb.Postgres.sql.v2-pvt.pvt_bundle.sql");

        Required(new PostgreSqlDialect()).Should().Be(shipped);
        Required(new ProPostgreSqlDialect()).Should().Be(shipped);
    }

    [Fact]
    public void Sqlite_NativeExtensionReportsTheVersionTheDialectRequires()
    {
        var source = FindNativeSource();
        source.Should().NotBeNull("the source of the native extension (redb.SQLite/native/src/redb_pvt.c) is part of the tree the tests run from");
        var match = NativeModuleVersion.Match(File.ReadAllText(source!));
        match.Success.Should().BeTrue("redb_pvt.c defines PVT_MODULE_VERSION");
        var shipped = match.Groups["v"].Value;

        Required(new SqliteDialect()).Should().Be(shipped);
        Required(new ProSqliteDialect()).Should().Be(shipped);
    }

    private static string Required(ISqlDialect dialect)
    {
        var required = dialect.Query_PvtRequiredVersion();
        required.Should().NotBeNullOrWhiteSpace($"{dialect.GetType().Name} names the module version it needs");
        return required!;
    }

    private static string VersionInBundle(Assembly assembly, string resourceName)
    {
        using var stream = assembly.GetManifestResourceStream(resourceName);
        stream.Should().NotBeNull($"{assembly.GetName().Name} embeds {resourceName}");
        using var reader = new StreamReader(stream!);
        var match = ModuleVersionFunction.Match(reader.ReadToEnd());
        match.Success.Should().BeTrue($"{resourceName} defines pvt_module_version with a RETURN of the version");
        return match.Groups["v"].Value;
    }

    /// <summary>redb_pvt.c in the public mirror layout (redb.SQLite at the top) or in the development one (redb.DB/src).</summary>
    private static string? FindNativeSource()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "redb.SQLite", "native", "src", "redb_pvt.c");
            if (!File.Exists(candidate))
                candidate = Path.Combine(dir.FullName, "redb.DB", "src", "redb.SQLite", "native", "src", "redb_pvt.c");
            if (File.Exists(candidate)) return candidate;
        }
        return null;
    }
}
