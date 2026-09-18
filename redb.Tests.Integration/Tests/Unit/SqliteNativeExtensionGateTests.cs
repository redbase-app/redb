using redb.SQLite.Data;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The native SQLite extension is versioned like the PostgreSQL and SQL Server modules: a build that does not report
/// the version the dialect requires is refused at initialization, naming the file. Before the gate a stale
/// <c>redbsqlite.dll</c> next to the application loaded silently and answered in an old shape - list item JSON in the
/// old form, a regex filter failing with an unrelated error.
/// </summary>
public class SqliteNativeExtensionGateTests
{
    [Fact]
    public void AStaleExtension_IsRefused_NamingBothVersionsAndTheFile()
    {
        var act = () => SqliteNativeExtension.EnsureVersion(@"C:\app\redbsqlite.dll", deployed: "0.6.5", required: "0.6.6");

        act.Should().Throw<InvalidOperationException>().Which.Message
            .Should().Contain("0.6.5").And.Contain("0.6.6").And.Contain(@"C:\app\redbsqlite.dll");
    }

    [Fact]
    public void AnExtensionWithoutTheVersionFunction_IsRefused()
    {
        var act = () => SqliteNativeExtension.EnsureVersion(@"C:\app\redbsqlite.dll", deployed: null, required: "0.6.6");

        act.Should().Throw<InvalidOperationException>().Which.Message
            .Should().Contain("pvt_module_version").And.Contain("0.6.6");
    }

    [Fact]
    public void TheMatchingExtension_Passes()
    {
        var act = () => SqliteNativeExtension.EnsureVersion(@"C:\app\redbsqlite.dll", deployed: "0.6.6", required: "0.6.6");

        act.Should().NotThrow();
    }
}
