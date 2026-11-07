using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

// ───────────────────────────────────────────────
// A dictionary field declared through the interface (DataLoss p.3)
// ───────────────────────────────────────────────

[RedbScheme("DictionaryInterfaceProbe")]
public class DictionaryInterfaceProbeProps
{
    public string? Label { get; set; }

    /// <summary>
    /// Declared as the interface on purpose. The scheme builder accepts <c>IDictionary&lt;K,V&gt;</c>, so
    /// the structure becomes a dictionary, and the RUNTIME value decides whether the save path can
    /// enumerate it into rows.
    /// </summary>
    public IDictionary<string, string>? Tags { get; set; }
}