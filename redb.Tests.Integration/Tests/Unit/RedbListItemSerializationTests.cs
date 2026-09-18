using System.Text.Json;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// <see cref="RedbListItem.Object"/> is a lazy load behind a property getter. A serializer walking
/// public properties must not wake it: that is how an audit log or an HTTP response ends up issuing
/// database loads (prod tsum, 2026-09).
/// <para>
/// No redb scope is live here: a read of Object would throw (a lazy load runs on the reader's scope, owner decision
/// 2026-09-15) or, under a configured fresh scope, leave the item loaded - both are caught.
/// </para>
/// </summary>
[Collection("RedbListItemSerialization")]
public class RedbListItemSerializationTests
{
    [Fact]
    public void SystemTextJson_DoesNotWakeTheLazyObjectLoader()
    {
        var item = new RedbListItem { Id = 1, IdList = 2, Value = "v", IdObject = 42 };

        var json = JsonSerializer.Serialize(item);

        item.IsObjectLoaded.Should().BeFalse("serialization is not a request to load the linked object");
        json.Should().NotContain("\"object\"");
        json.Should().Contain("\"id_object\":42");
    }
}
