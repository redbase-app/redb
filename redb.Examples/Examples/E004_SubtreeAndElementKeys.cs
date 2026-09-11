using System.Diagnostics;
using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Unique keys beyond a root scalar (V4). Props are stored flat, one row per value, and the
/// storage already maintains a canonical hash for every nested class and collection - so
/// <c>[RedbUnique]</c> reaches four more shapes with the same database-enforced index:
///
/// 1. A scalar INSIDE a nested class - a lambda path (<c>d => d.Identity!.Serial</c>) is the key.
/// 2. A whole SUBTREE - the content of a nested class, in canonical form, is the key.
/// 3. A whole COLLECTION - the ordered list is the key; the same numbers in another order differ.
/// 4. ELEMENTS of a collection with a scope: <c>UniqueScope.Collection</c> forbids duplicates
///    inside one object's collection, <c>UniqueScope.Scheme</c> makes every element value unique
///    across all objects of the scheme (and probeable by <c>GetByUniqueAsync</c>).
///
/// Plus <c>[RedbTags]</c>: a free-form marker written to the scheme and structure metadata for
/// applications and extensions to read back.
/// </summary>
[ExampleMeta("E004", "Unique Keys - nested, subtree and element keys, [RedbTags]", "CRUD",
    ExampleTier.Free, 4, "Unique", "RedbUnique", "UniqueScope", "RedbTags", "GetByUniqueAsync",
    RelatedApis = ["IRedbService.GetByUniqueAsync", "IRedbService.SaveAsync", "IRedbService.GetSchemeByTypeAsync"])]
public class E004_SubtreeAndElementKeys : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        await redb.SyncSchemeAsync<DeviceProps>();

        // Unique per run so the example is rerunnable on a shared database.
        var run = Guid.NewGuid().ToString("N")[..8];
        var sw = Stopwatch.StartNew();

        // 1. Key on a scalar inside a nested class: same rules as a root key.
        var serial = $"SN-{run}";
        var alphaId = await redb.SaveAsync(Device("alpha", d => d.Identity = new DeviceIdentity { Serial = serial, Vendor = "Acme" }));
        var nestedDup = await Rejected(() => redb.SaveAsync(Device("alpha-dup", d => d.Identity = new DeviceIdentity { Serial = serial, Vendor = "Other" })));
        var bySerial = await redb.GetByUniqueAsync<DeviceProps>(d => d.Identity!.Serial, serial);

        // 2. Subtree key: the CONTENT of the nested class is the key. Same content - rejected;
        //    one field different - a different key. The lookup canonicalises the probe object
        //    the same way the save did.
        var region = $"eu-{run}";
        var betaId = await redb.SaveAsync(Device("beta", d => d.Config = new DeviceConfig { Region = region, Tier = 2 }));
        var subtreeDup = await Rejected(() => redb.SaveAsync(Device("beta-dup", d => d.Config = new DeviceConfig { Region = region, Tier = 2 })));
        await redb.SaveAsync(Device("beta-tier3", d => d.Config = new DeviceConfig { Region = region, Tier = 3 }));
        var byConfig = await redb.GetByUniqueAsync<DeviceProps>(d => d.Config, new DeviceConfig { Region = region, Tier = 2 });

        // 3. Subtree key on a collection: the ordered list is the key.
        var basePort = Random.Shared.Next(10_000, 60_000);
        List<long> ports = [basePort, basePort + 1, basePort + 2];
        await redb.SaveAsync(Device("gamma", d => d.Ports = ports));
        var listDup = await Rejected(() => redb.SaveAsync(Device("gamma-dup", d => d.Ports = [.. ports])));
        await redb.SaveAsync(Device("gamma-reversed", d => d.Ports = [basePort + 2, basePort + 1, basePort]));

        // 4. Element keys, Collection scope: no duplicate label inside ONE device. Another device
        //    may carry the same label - the scope is the collection, not the scheme.
        await redb.SaveAsync(Device("delta", d => d.Labels = ["rack-1", "spare"]));
        await redb.SaveAsync(Device("delta-twin", d => d.Labels = ["rack-1"]));
        var elementDup = await Rejected(() => redb.SaveAsync(Device("delta-dup", d => d.Labels = ["rack-1", "rack-1"])));

        // 5. Element keys, Scheme scope: a MAC address belongs to one device in the whole scheme,
        //    and the holder is found by the element value.
        var mac = $"02:{run[..2]}:{run[2..4]}:{run[4..6]}:00:01";
        var epsilonId = await redb.SaveAsync(Device("epsilon", d => d.MacAddresses = [mac, $"02:{run[..2]}:00:00:00:02"]));
        var macDup = await Rejected(() => redb.SaveAsync(Device("epsilon-dup", d => d.MacAddresses = [mac])));
        var byMac = await redb.GetByUniqueAsync<DeviceProps>(d => d.MacAddresses, mac);

        // 6. [RedbTags]: the markers are metadata - read them back from the synced scheme.
        var scheme = await redb.GetSchemeByTypeAsync<DeviceProps>();
        var schemeTags = scheme?.Tags;
        var macTags = scheme?.GetStructureByName(nameof(DeviceProps.MacAddresses))?.Tags;

        sw.Stop();

        // The example is a contract probe: a duplicate that slipped through, a lookup that missed
        // or a tag that did not land is a FAIL, not a line in the report.
        var problems = new List<string>();
        if (!nestedDup.StartsWith("rejected")) problems.Add("nested key: duplicate was not rejected");
        if (bySerial?.Id != alphaId) problems.Add("nested key: lookup by lambda path missed");
        if (!subtreeDup.StartsWith("rejected")) problems.Add("subtree key: duplicate content was not rejected");
        if (byConfig?.Id != betaId) problems.Add("subtree key: lookup by content missed");
        if (!listDup.StartsWith("rejected")) problems.Add("collection key: duplicate list was not rejected");
        if (!elementDup.StartsWith("rejected")) problems.Add("element key (Collection): duplicate inside one device was not rejected");
        if (!macDup.StartsWith("rejected")) problems.Add("element key (Scheme): duplicate across devices was not rejected");
        if (byMac?.Id != epsilonId) problems.Add("element key (Scheme): lookup by element value missed");
        if (schemeTags != "examples,e004") problems.Add($"[RedbTags] on the scheme did not land: '{schemeTags}'");
        if (macTags != "network") problems.Add($"[RedbTags] on the structure did not land: '{macTags}'");
        if (problems.Count > 0)
            return Fail("E004", "Unique Keys - nested, subtree and element keys, [RedbTags]", ExampleTier.Free, sw.ElapsedMilliseconds, string.Join("; ", problems));

        return Ok("E004", "Unique Keys - nested, subtree and element keys, [RedbTags]", ExampleTier.Free, sw.ElapsedMilliseconds, 9,
            [
                $"Nested key Identity.Serial = {serial}: holder #{alphaId}, duplicate {nestedDup}, lookup by lambda path found #{bySerial?.Id}",
                $"Subtree key Config {{{region}, tier 2}}: holder #{betaId}, duplicate {subtreeDup}, tier 3 saved as a different key, lookup by content found #{byConfig?.Id}",
                $"Collection key Ports [{string.Join(",", ports)}]: duplicate {listDup}, reversed order saved as a different key",
                $"Element keys, Collection scope: 'rack-1' in two devices is fine, twice in one device {elementDup}",
                $"Element keys, Scheme scope: MAC {mac} held by #{epsilonId}, duplicate {macDup}, lookup by element found #{byMac?.Id}",
                $"[RedbTags]: scheme '{schemeTags}', structure MacAddresses '{macTags}'"
            ]);
    }

    private static RedbObject<DeviceProps> Device(string title, Action<DeviceProps> configure)
    {
        var props = new DeviceProps { Title = title };
        configure(props);
        return new RedbObject<DeviceProps> { name = $"e004-{title}", Props = props };
    }

    /// <summary>Runs a save that must fail on a key and reports which key fired.</summary>
    private static async Task<string> Rejected(Func<Task<long>> save)
    {
        try
        {
            await save();
            return "NOT rejected - this line must never print";
        }
        catch (RedbUniqueViolationException ex)
        {
            return $"rejected ({ex.Kind}, {ex.PropertyName})";
        }
    }
}
