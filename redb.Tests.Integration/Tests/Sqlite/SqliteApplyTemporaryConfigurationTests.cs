using redb.Core;
using redb.Core.Extensions;
using redb.Tests.Integration.Fixtures;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// ApplyTemporary (review CFG-1). The builder form built on the live configuration, so the change was applied
/// before the snapshot meant to undo it was taken, and the scope "restored" the changed values. The configuration
/// form applied and restored a hand-written list of fourteen settings out of thirty-six. Any provider would do;
/// SQLite is the cheapest host.
/// </summary>
[Collection("Sqlite")]
public class SqliteApplyTemporaryConfigurationTests
{
    private readonly IRedbService _redb;

    public SqliteApplyTemporaryConfigurationTests(SqliteFixture fixture) => _redb = fixture.Redb;

    [Fact]
    public void TheBuilderForm_IsUndoneWhenTheScopeEnds()
    {
        var before = _redb.Configuration.DefaultCheckPermissionsOnDelete;
        before.Should().BeTrue("precondition: the default checks permissions on delete");

        using (_redb.ApplyTemporary(b => b.WithoutPermissionChecks()))
            _redb.Configuration.DefaultCheckPermissionsOnDelete.Should().BeFalse();

        _redb.Configuration.DefaultCheckPermissionsOnDelete.Should().Be(before);
    }

    [Fact]
    public void TheConfigurationForm_AppliesEveryBehaviourSetting_AndKeepsTheConnection()
    {
        var connection = _redb.Configuration.ConnectionString;
        var prefilter = _redb.Configuration.EnablePvtPrefilter;
        var temporary = _redb.Configuration.Clone();
        temporary.EnablePvtPrefilter = !prefilter;
        temporary.ConnectionString = null;

        using (_redb.ApplyTemporary(temporary))
        {
            _redb.Configuration.EnablePvtPrefilter.Should().Be(!prefilter, "a setting outside the old list of fourteen applies too");
            _redb.Configuration.ConnectionString.Should().Be(connection, "a temporary scope changes behaviour, not the connection");
        }

        _redb.Configuration.EnablePvtPrefilter.Should().Be(prefilter);
    }
}
