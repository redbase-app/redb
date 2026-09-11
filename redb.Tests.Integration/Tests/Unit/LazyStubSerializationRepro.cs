using System.Text.Json;
using redb.Core.Models.Entities;
using redb.Core.Serialization;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// In-memory guard for stub serialization (V4 Л1, L.4) — no database. Born as the bisection that
/// caught an StJ stack overflow (a [JsonIgnore]+private-bridge variant); kept because it pins the
/// converter contract at the unit level: a stub with and without a graph, with redb Options and
/// with default options.
/// </summary>
public class LazyStubSerializationRepro
{
    private static RedbObject<LazyNodeProps> RootWithStub()
    {
        var stub = new RedbObject<LazyNodeProps> { id = 5, scheme_id = 100, hash = Guid.NewGuid() };
        return new RedbObject<LazyNodeProps>
        {
            id = 1,
            scheme_id = 100,
            Props = new LazyNodeProps { Label = "root", Next = stub }
        };
    }

    [Fact]
    public void A_RootWithStub_RedbOptions()
    {
        var json = JsonSerializer.Serialize(RootWithStub(), SystemTextJsonRedbSerializer.Options);
        json.Should().Contain("\"id\":5");
    }

    [Fact]
    public void B_RootWithStub_DefaultOptions()
    {
        var json = JsonSerializer.Serialize(RootWithStub());
        json.Should().Contain("root");
    }

    [Fact]
    public void C_StubAlone_RedbOptions()
    {
        var stub = new RedbObject<LazyNodeProps> { id = 5, scheme_id = 100 };
        var json = JsonSerializer.Serialize(stub, SystemTextJsonRedbSerializer.Options);
        json.Should().Contain("\"id\":5");
    }

    [Fact]
    public void D_RootWithoutRefs_RedbOptions()
    {
        var root = new RedbObject<LazyNodeProps> { id = 1, Props = new LazyNodeProps { Label = "solo" } };
        var json = JsonSerializer.Serialize(root, SystemTextJsonRedbSerializer.Options);
        json.Should().Contain("solo");
    }
}
