namespace redb.Tests.Integration.Fixtures;

/// <summary>
/// Knobs the Pro fixtures read from the environment, so the whole suite can be replayed under a
/// different engine configuration without editing code.
/// </summary>
internal static class ProTestOptions
{
    /// <summary>
    /// REDB_PVT_PREFILTER=1|true|on|yes enables the PVT prefilter. Off by default, matching
    /// <c>RedbServiceConfiguration.EnablePvtPrefilter</c>.
    ///
    /// The prefilter is built as a superset and must not change a single result, so BOTH passes are
    /// expected to be green and identical. A test that passes with it off and fails with it on is a
    /// bug in the prefilter, never a reason to relax the test.
    /// </summary>
    public static bool PvtPrefilter =>
        (Environment.GetEnvironmentVariable("REDB_PVT_PREFILTER") ?? string.Empty).Trim().ToLowerInvariant()
            is "1" or "true" or "on" or "yes";
}
