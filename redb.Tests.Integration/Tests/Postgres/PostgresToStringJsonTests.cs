using System.Threading;
using System.Text.Json;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// RedbObject.ToString() returns the object's canonical JSON (discussion #12, item 2). The logic
/// is provider-independent (one serializer for all), so one provider carries the suite; the
/// database round-trip test is the contract check against the real <c>get_object_json</c>.
/// </summary>
[Collection("Postgres")]
public class PostgresToStringJsonTests
{
    private readonly IRedbService _redb;

    public PostgresToStringJsonTests(PostgresFixture fixture) => _redb = fixture.Redb;

    [Fact]
    public void ToString_RoundTripsThroughTheSerializer()
    {
        // In-memory only: the string must be parseable by the same serializer back into an
        // equivalent object - that is what "canonical JSON" means.
        var obj = TestDataFactory.CreateSimple("ToString-probe", 12.5m);
        obj.id = 424242;
        obj.scheme_id = 111;

        var json = obj.ToString();

        var parsed = new redb.Core.Serialization.SystemTextJsonRedbSerializer()
            .Deserialize<SimpleProps>(json);
        parsed.id.Should().Be(424242);
        parsed.name.Should().Be(obj.name);
        parsed.Props.Title.Should().Be("ToString-probe");
        parsed.Props.Price.Should().Be(12.5m);
    }

    [Fact]
    public async Task ToString_MatchesWhatTheDatabaseHolds()
    {
        // The contract check: parse get_object_json(id) and parse ToString() - the Props must be
        // identical. Not compared as raw strings (field order and number formatting may differ);
        // equality through the shared serializer is the honest form of "the same contract".
        var obj = TestDataFactory.CreateSimple("ToString-db-probe", 77.25m);
        obj.id = await _redb.SaveAsync(obj);

        var loaded = await _redb.LoadAsync<SimpleProps>(obj.id, depth: 1);
        var dbJson = await _redb.Context.ExecuteScalarAsync<string>(
            $"SELECT get_object_json({obj.id}, 1)::text");

        var serializer = new redb.Core.Serialization.SystemTextJsonRedbSerializer();
        var fromDb = serializer.Deserialize<SimpleProps>(dbJson!);
        var fromToString = serializer.Deserialize<SimpleProps>(loaded!.ToString());

        fromToString.id.Should().Be(fromDb.id);
        fromToString.hash.Should().Be(fromDb.hash);
        fromToString.Props.Title.Should().Be(fromDb.Props.Title);
        fromToString.Props.Price.Should().Be(fromDb.Props.Price);
        fromToString.Props.Count.Should().Be(fromDb.Props.Count);
        fromToString.Props.CreatedAt.Should().Be(fromDb.Props.CreatedAt);
    }

    [Fact]
    public void ToString_OnATree_UsesTheCanonicalView_NoCycles_NoLoad()
    {
        // Review find: the stub write converter matches exactly RedbObject<>, so a
        // TreeRedbObject<T> serialized by its runtime type would fall into reflection - walking
        // the Parent/Children cycle and the lazy Props getter. ToString must serialize the
        // canonical RedbObject<T> view instead.
        var parent = new TreeRedbObject<SimpleProps> { id = 1, name = "parent", _lazyLoader = new ExplodingLoader() };
        var child = new TreeRedbObject<SimpleProps> { id = 2, name = "child", Parent = parent, _lazyLoader = new ExplodingLoader() };
        parent.Children.Add(child);

        var json = child.ToString();

        json.Should().NotContain("toStringError");
        json.Should().NotContain("\"parent\"", "tree navigation is not part of the canonical view");
        using var doc = JsonDocument.Parse(json);
        doc.RootElement.GetProperty("id").GetInt64().Should().Be(2);
    }

    [Fact]
    public void ToString_OnALazyStub_NeverLoads()
    {
        // A stub carries base fields and an attached loader; stringifying it must not reach the
        // database (EF's lazy proxies famously do exactly that from the debugger). The exploding
        // loader proves it: any load attempt fails the test.
        var stub = new RedbObject<SimpleProps>
        {
            id = 515151,
            scheme_id = 111,
            name = "stub-probe",
            _lazyLoader = new ExplodingLoader(),
        };
        stub.IsPropsLoaded.Should().BeFalse("the probe must actually be an unloaded stub");

        var json = stub.ToString();

        json.Should().Contain("\"id\":515151");
        json.Should().NotContain("toStringError");
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.TryGetProperty("properties", out var props))
            props.ValueKind.Should().Be(JsonValueKind.Null, "an unloaded stub has no properties to show");
    }

    private sealed class ExplodingLoader : ILazyPropsLoader
    {
        public TProps? LoadProps<TProps>(long objectId, long schemeId) where TProps : class, new()
            => throw new InvalidOperationException("ToString must never trigger lazy loading");
        public Task<TProps?> LoadPropsAsync<TProps>(long objectId, long schemeId, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new InvalidOperationException("ToString must never trigger lazy loading");
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new InvalidOperationException("ToString must never trigger lazy loading");
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new InvalidOperationException("ToString must never trigger lazy loading");
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new InvalidOperationException("ToString must never trigger lazy loading");
        public Task LoadPropsForManyAsync<TProps>(List<RedbObject<TProps>> objects, HashSet<long>? projectedStructureIds, int? propsDepth, CancellationToken cancellationToken = default) where TProps : class, new()
            => throw new InvalidOperationException("ToString must never trigger lazy loading");
    }
}
