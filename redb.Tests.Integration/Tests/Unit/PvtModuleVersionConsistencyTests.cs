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
        @"FUNCTION\s+(?:\[?dbo\]?\.)?\[?pvt_module_version\]?[\s\S]*?RETURN\s+N?'(?<v>\d+(?:\.\d+){2,3})'",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex NativeModuleVersion = new(
        @"#define\s+PVT_MODULE_VERSION\s+""(?<v>\d+\.\d+\.\d+)""", RegexOptions.CultureInvariant);

    // pvt_module_compat: '<oldest module served>|<package version>' (PostgreSQL: SELECT '...'::text, SQL Server: RETURN N'...').
    private static readonly Regex ModuleCompatFunction = new(
        @"FUNCTION\s+(?:\[?dbo\]?\.)?\[?pvt_module_compat\]?[\s\S]*?(?:RETURN|SELECT)\s+N?'(?<min>\d+(?:\.\d+){2,3})\|(?<pkg>\d+\.\d+\.\d+)'",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>
    /// The module states what it serves, and it serves at least the package it ships with: the oldest module version it
    /// names is not above its own.
    /// </summary>
    [Theory]
    [InlineData("redb.MSSql.sql.v2-pvt.pvt_bundle.sql", typeof(redb.MSSql.RedbService))]
    [InlineData("redb.Postgres.sql.v2-pvt.pvt_bundle.sql", typeof(redb.Postgres.RedbService))]
    public void Module_StatesWhatItServes_AndServesItsOwnPackage(string resourceName, Type serviceType)
    {
        using var stream = serviceType.Assembly.GetManifestResourceStream(resourceName);
        using var reader = new StreamReader(stream!);
        var text = reader.ReadToEnd();

        var compat = ModuleCompatFunction.Match(text);
        compat.Success.Should().BeTrue($"{resourceName} defines pvt_module_compat as '<module>|<package>'");
        var own = ModuleVersionFunction.Match(text).Groups["v"].Value;

        new Version(compat.Groups["min"].Value).Should().BeLessThanOrEqualTo(new Version(own));
        redb.Core.Data.ModuleCompatibility.Serves($"{compat.Groups["min"].Value}|{compat.Groups["pkg"].Value}", own, out var pkg)
            .Should().BeTrue();
        pkg.Should().Be(compat.Groups["pkg"].Value);
    }

    // The guard at the head of the module: the own version it compares a deployed module with
    // (PostgreSQL: _own constant text := '...', SQL Server: @own nvarchar(50) = N'...').
    private static readonly Regex GuardOwnVersion = new(
        @"(?:_own\s+constant\s+text\s*:=|@own\s+nvarchar\(50\)\s*=)\s*N?'(?<v>\d+(?:\.\d+){2,3})'",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>
    /// The guard that stops a hand-run older script over a newer module compares with the version the same script
    /// stamps (a guard left at an old version would let the script take back the modules between the two), and it
    /// stands before the first statement that changes anything.
    /// </summary>
    [Theory]
    [InlineData("redb.MSSql.sql.v2-pvt.pvt_bundle.sql", typeof(redb.MSSql.RedbService))]
    [InlineData("redb.Postgres.sql.v2-pvt.pvt_bundle.sql", typeof(redb.Postgres.RedbService))]
    public void Guard_ComparesWithTheVersionTheScriptStamps_BeforeAnyChange(string resourceName, Type serviceType)
    {
        using var stream = serviceType.Assembly.GetManifestResourceStream(resourceName);
        using var reader = new StreamReader(stream!);
        var text = reader.ReadToEnd();

        var guard = GuardOwnVersion.Match(text);
        guard.Success.Should().BeTrue($"{resourceName} starts with the guard against a newer module");
        guard.Groups["v"].Value.Should().Be(ModuleVersionFunction.Match(text).Groups["v"].Value);
        guard.Index.Should().BeLessThan(text.IndexOf("ALTER TABLE", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("0.7.25", "0.7.24", true)]
    [InlineData("0.7.24", "0.7.24", false)]
    [InlineData("0.7.23", "0.7.24", false)]
    [InlineData("0.10.0", "0.9.9", true)]
    [InlineData("0.0.0-test", "0.7.24", false)]
    [InlineData(null, "0.7.24", false)]
    [InlineData("garbage", "0.7.24", false)]
    public void IsNewer_ComparesVersionsAsNumbers(string? deployed, string required, bool newer)
        => redb.Core.Data.ModuleCompatibility.IsNewer(deployed, required).Should().Be(newer);

    [Theory]
    [InlineData("0.7.24|4.8.0", "0.7.24", true, "4.8.0")]
    [InlineData("0.7.20|4.8.0", "0.7.24", true, "4.8.0")]
    [InlineData("0.7.30|4.9.0", "0.7.24", false, "4.9.0")]
    [InlineData(null, "0.7.24", false, null)]
    [InlineData("0.7.20", "0.7.24", true, null)]
    [InlineData("x|4.9.0", "0.7.24", false, "4.9.0")]
    public void Serves_WhenTheOldestServedModuleIsNotAboveTheOwn(string? compat, string required, bool serves, string? package)
    {
        redb.Core.Data.ModuleCompatibility.Serves(compat, required, out var named).Should().Be(serves);
        named.Should().Be(package);
    }

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
