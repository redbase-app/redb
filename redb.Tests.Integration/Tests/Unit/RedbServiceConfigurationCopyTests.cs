using System.Reflection;
using redb.Core.Models.Configuration;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Copying a configuration (review CFG-1). Clone listed its settings by hand and had fallen behind: five of them
/// (AutoApplyDatabaseUpgrades, StringCollation, EnablePvtPrefilter, PropsSaveStrategy,
/// DefaultCheckPermissionsOnQuery) came back at their defaults, so a configuration "fixed" by the validator could
/// switch automatic schema upgrades back on. Every writable setting is set here to a value that is not its default
/// and must survive the copy - a setting added later without being copied fails this test. No database required.
/// </summary>
public class RedbServiceConfigurationCopyTests
{
    private static readonly PropertyInfo[] Settings = typeof(RedbServiceConfiguration)
        .GetProperties(BindingFlags.Public | BindingFlags.Instance)
        .Where(p => p.CanRead && p.CanWrite)
        .ToArray();

    /// <summary>The identity of a connection, as opposed to the behaviour settings a temporary scope may change.</summary>
    private static readonly string[] Identity =
        [nameof(RedbServiceConfiguration.ConnectionString), nameof(RedbServiceConfiguration.CacheDomain)];

    private static RedbServiceConfiguration WithEverySettingChanged()
    {
        var config = new RedbServiceConfiguration();
        foreach (var p in Settings)
            p.SetValue(config, NotTheDefault(p, p.GetValue(config)));
        return config;
    }

    private static object? NotTheDefault(PropertyInfo p, object? current) => p.PropertyType switch
    {
        var t when t == typeof(bool) => !(bool)current!,
        var t when t == typeof(int) => 7,        // valid for every ranged int setting, the default of none
        var t when t == typeof(long) => 7L,
        var t when t == typeof(TimeSpan) => TimeSpan.FromMinutes(3),
        var t when t == typeof(string) => "und-x-icu",   // a valid collation name, and a valid anything else
        var t when t.IsEnum => Enum.GetValues(t).Cast<object>().First(v => !v.Equals(current)),
        var t when t == typeof(JsonSerializationOptions) => new JsonSerializationOptions
        {
            WriteIndented = !((JsonSerializationOptions)current!).WriteIndented,
            UseUnsafeRelaxedJsonEscaping = !((JsonSerializationOptions)current!).UseUnsafeRelaxedJsonEscaping
        },
        var t => throw new NotSupportedException(
            $"Setting {p.Name} of type {t.Name}: teach this test a non-default value for the type.")
    };

    private static void ShouldMatch(RedbServiceConfiguration copy, RedbServiceConfiguration source, IEnumerable<PropertyInfo> settings)
    {
        foreach (var p in settings)
        {
            if (p.PropertyType == typeof(JsonSerializationOptions))
            {
                copy.JsonOptions.Should().NotBeSameAs(source.JsonOptions, "the copy owns its JSON options");
                copy.JsonOptions.Should().BeEquivalentTo(source.JsonOptions, $"{p.Name} is copied");
                continue;
            }
            p.GetValue(copy).Should().Be(p.GetValue(source), $"{p.Name} is copied");
        }
    }

    [Fact]
    public void Clone_CopiesEverySetting()
    {
        var source = WithEverySettingChanged();

        ShouldMatch(source.Clone(), source, Settings);
    }

    [Fact]
    public void CopyBehaviourFrom_CopiesEverySettingButTheConnectionIdentity()
    {
        var source = WithEverySettingChanged();
        var target = new RedbServiceConfiguration { ConnectionString = "Data Source=mine", CacheDomain = "mine" };

        target.CopyBehaviourFrom(source);

        ShouldMatch(target, source, Settings.Where(p => !Identity.Contains(p.Name)));
        target.ConnectionString.Should().Be("Data Source=mine");
        target.CacheDomain.Should().Be("mine");
    }
}
