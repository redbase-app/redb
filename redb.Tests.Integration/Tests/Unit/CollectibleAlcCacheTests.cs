using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Text.Json;
using redb.Core.Caching;
using redb.Core.Models.Entities;
using redb.Core.Pro.Materialization;
using redb.Core.Providers.Base;
using redb.Core.Serialization;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// A module of Tsak lives in a collectible AssemblyLoadContext and must unload: no process-wide redb cache may keep a
/// type of it, a member of such a type or an instance of one alive once the context unloads. Each test touches one
/// cache with a type loaded into a fresh collectible context, unloads it and checks the context is collected. The
/// test assembly itself is loaded into the context, so its types are distinct from the ones of the default context
/// while redb.Core and redb.Core.Pro resolve from the default context, as for a real module.
/// </summary>
[Collection("ClrSchemeTypeIndex")]
public class CollectibleAlcCacheTests
{
    // A running redb service starts the watch; these tests use the caches without one.
    public CollectibleAlcCacheTests() => CollectibleContextCleanup.EnsureWatching();

    private static Type LoadPropsType(AssemblyLoadContext alc)
        => alc.LoadFromAssemblyPath(typeof(NamingPinnedProps).Assembly.Location).GetType(typeof(NamingPinnedProps).FullName!)!;

    private static void AssertCollected(WeakReference weakAlc, string cache)
    {
        for (var i = 0; i < 20 && weakAlc.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        weakAlc.IsAlive.Should().BeFalse($"{cache} must not keep the unloaded module's context alive");
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference TouchAndUnload(string name, Action<Type> touch)
    {
        var alc = new AssemblyLoadContext(name, isCollectible: true);
        touch(LoadPropsType(alc));
        var weak = new WeakReference(alc);
        alc.Unload();
        return weak;
    }

    private static object NewRedbObject(Type propsType, long id)
    {
        var obj = (RedbObject)Activator.CreateInstance(typeof(RedbObject<>).MakeGenericType(propsType))!;
        obj.id = id;
        obj.hash = Guid.NewGuid();
        return obj;
    }

    [Fact]
    public void MetadataCache_SchemeIdToType_DoesNotPinTheModule()
    {
        var cache = new GlobalMetadataCache("alc-probe-metadata");
        var weak = TouchAndUnload("alc-metadata", type =>
        {
            cache.RegisterClrType("alc.probe.metadata", 9_100_001, type);
            cache.GetClrType(9_100_001).Should().BeSameAs(type);
            cache.GetSchemeIdByClrType(type).Should().Be(9_100_001);
        });

        AssertCollected(weak, "GlobalMetadataCache (scheme_id <-> Type)");
        cache.GetClrType(9_100_001).Should().BeNull("the type is gone with its context");
    }

    [Fact]
    public void SavePipeline_TypePredicates_DoNotPinTheModule()
    {
        var predicates = new[] { "IsRedbObjectType", "IsRedbObjectArrayType", "IsDictionaryWithRedbObjectValue", "IsBusinessClassType" }
            .Select(n => typeof(ObjectStorageProviderBase).GetMethod(n, BindingFlags.NonPublic | BindingFlags.Static)
                         ?? throw new InvalidOperationException($"{n} not found"))
            .ToArray();

        var weak = TouchAndUnload("alc-save-predicates", type =>
        {
            var redbObjectType = typeof(RedbObject<>).MakeGenericType(type);
            foreach (var probe in new[] { type, redbObjectType, typeof(List<>).MakeGenericType(redbObjectType),
                                          typeof(Dictionary<,>).MakeGenericType(typeof(string), redbObjectType) })
                foreach (var predicate in predicates)
                    predicate.Invoke(null, [probe]);
        });

        AssertCollected(weak, "the type predicates of the save pipeline");
    }

    [Fact]
    public void ProMaterializer_BuildMethodCache_DoesNotPinTheModule()
    {
        var getBuildMethod = typeof(ProPropsMaterializer).GetMethod("GetBuildMethod", BindingFlags.NonPublic | BindingFlags.Static)
                             ?? throw new InvalidOperationException("GetBuildMethod not found");

        var weak = TouchAndUnload("alc-materializer", type =>
            ((MethodInfo)getBuildMethod.Invoke(null, [type])!).GetGenericArguments().Should().Equal(type));

        AssertCollected(weak, "the MethodInfo cache of ProPropsMaterializer");
    }

    [Fact]
    public void SerializerOptions_DoNotPinTheModule()
    {
        var weak = TouchAndUnload("alc-serializer", type =>
        {
            var redbObjectType = typeof(RedbObject<>).MakeGenericType(type);
            var json = JsonSerializer.Serialize(NewRedbObject(type, 1), redbObjectType, SystemTextJsonRedbSerializer.Options);
            JsonSerializer.Deserialize(json, redbObjectType, SystemTextJsonRedbSerializer.Options).Should().NotBeNull();
        });

        AssertCollected(weak, "the static JsonSerializerOptions of the serializer");
    }

    [Fact]
    public void SystemTextJsonOwnCaches_DoNotPinTheModule()
    {
        // System.Text.Json caches outside any options instance too (contexts shared by equal options, emitted member
        // accessors): a module serializing its Props with options of its own must unload as well.
        var weak = TouchAndUnload("alc-stj", type =>
        {
            var json = JsonSerializer.Serialize(Activator.CreateInstance(type), type, new JsonSerializerOptions());
            JsonSerializer.Deserialize(json, type, new JsonSerializerOptions()).Should().NotBeNull();
        });

        AssertCollected(weak, "the process-wide caches of System.Text.Json");
    }

    [Fact]
    public void PropsCache_DropsTheModulesObjects_OnUnload()
    {
        var props = new GlobalPropsCache("alc-probe-props");
        props.Initialize(new MemoryRedbObjectCache());
        var set = typeof(GlobalPropsCache).GetMethod(nameof(GlobalPropsCache.Set))!;
        const long id = 9_200_001;

        var weak = TouchAndUnload("alc-props-cache", type =>
        {
            set.MakeGenericMethod(type).Invoke(props, [NewRedbObject(type, id)]);
            props.Instance!.GetStats().TotalEntries.Should().Be(1);
        });

        AssertCollected(weak, "the props cache");
        props.Instance!.GetStats().TotalEntries.Should().Be(0, "the objects of an unloading context leave the cache");
        props.Clear();
    }
}
