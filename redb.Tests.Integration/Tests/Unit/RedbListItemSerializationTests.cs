using System.Text.Json;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// <see cref="RedbListItem.Object"/> is a lazy loader behind a property getter. A serializer walking
/// public properties must not wake it: that is how an audit log or an HTTP response ends up issuing
/// database loads through whatever scope the item happens to be bound to (prod tsum, 2026-09).
/// </summary>
[Collection("RedbListItemSerialization")]
public class RedbListItemSerializationTests
{
    [Fact]
    public void SystemTextJson_DoesNotWakeTheLazyObjectLoader()
    {
        var calls = 0;
        RedbListItem.SetGlobalObjectLoader(_ =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult<IRedbObject?>(null);
        });

        var item = new RedbListItem { Id = 1, IdList = 2, Value = "v", IdObject = 42 };
        var json = JsonSerializer.Serialize(item);

        calls.Should().Be(0, "serialization is not a request to load the linked object");
        json.Should().NotContain("\"object\"");
        json.Should().Contain("\"id_object\":42");
    }
}
