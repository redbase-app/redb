using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using redb.Core.Caching;
using redb.Core.Exceptions;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// C1 — collectible-ALC safety of the process-global <see cref="ClrSchemeTypeIndex"/>.
/// The index must (a) never pin a hot-reloaded plugin's <see cref="Type"/> (weak references, so the ALC
/// can unload) and (b) treat the same logical type reloaded into a new ALC as a reload, not a name
/// conflict. See docs/CACHING_FIXES_PLAN.md (C1).
/// <para>
/// Runs in its own collection: the index is process-global static, so these tests must not race the
/// scheme-naming suite that also drives it.
/// </para>
/// </summary>
[Collection("ClrSchemeTypeIndex")]
public class ClrSchemeTypeIndexAlcTests
{
    // Loading is factored into a no-inline method so no JIT-local keeps the ALC/type alive past Unload().
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference RegisterTypeFromCollectibleAlc(string schemeName)
    {
        var alc = new AssemblyLoadContext("c1-leak-probe", isCollectible: true);
        var asm = alc.LoadFromAssemblyPath(typeof(NamingPinnedProps).Assembly.Location);
        var type = asm.GetType(typeof(NamingPinnedProps).FullName!)!;

        // The index stores only a WeakReference<Type>, so this registration must not keep the ALC alive.
        ClrSchemeTypeIndex.Register(schemeName, type);

        var weak = new WeakReference(alc);
        alc.Unload();
        return weak;
    }

    [Fact]
    public void UnloadedAlc_IsCollected_IndexDoesNotPinIt()
    {
        ClrSchemeTypeIndex.Clear();
        try
        {
            var weakAlc = RegisterTypeFromCollectibleAlc("c1.leak");

            for (var i = 0; i < 15 && weakAlc.IsAlive; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            weakAlc.IsAlive.Should().BeFalse(
                "a strong Type reference in the static index would pin the collectible ALC forever (memory leak on every hot-reload)");

            // The dead entry must resolve to null (pruned lazily), not throw.
            ClrSchemeTypeIndex.Resolve("c1.leak").Should().BeNull();
        }
        finally
        {
            ClrSchemeTypeIndex.Clear();
        }
    }

    [Fact]
    public void SameTypeReloadedInNewAlc_IsNotReportedAsConflict()
    {
        ClrSchemeTypeIndex.Clear();
        AssemblyLoadContext? alc = null;
        try
        {
            var loc = typeof(NamingPinnedProps).Assembly.Location;
            var fullName = typeof(NamingPinnedProps).FullName!;

            // Same logical type (identical FullName, same explicit Name "naming.pinned") loaded into a
            // separate ALC — exactly what a Tsak module hot-reload produces. The old code compared only
            // (owner != type && same Name) and would flag this as a false conflict; the reload guard
            // (same FullName across ALCs = reload) must suppress it.
            alc = new AssemblyLoadContext("c1-reload-probe", isCollectible: true);
            var reloaded = alc.LoadFromAssemblyPath(loc).GetType(fullName)!;

            reloaded.Should().NotBeSameAs(typeof(NamingPinnedProps), "must be a distinct Type instance from another ALC");
            reloaded.FullName.Should().Be(fullName);

            // A rescan now sees both the default-ALC type and the reloaded one under "naming.pinned".
            ClrSchemeTypeIndex.EnsureFresh();

            // Must NOT throw — this is a reload, not a genuine two-distinct-types clash.
            var act = () => ClrSchemeTypeIndex.ThrowIfNameConflict("naming.pinned", reloaded);
            act.Should().NotThrow<RedbSchemeNameConflictException>();

            // And the name still resolves to a live type.
            ClrSchemeTypeIndex.Resolve("naming.pinned").Should().NotBeNull();
        }
        finally
        {
            ClrSchemeTypeIndex.Clear();
            alc?.Unload();
        }
    }
}
