using redb.Core.Models.Entities;
using redb.Core.Utils;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The scheme structure hash drives every cache invalidation (metadata cache, structure tree, Pro
/// registry): a marker that changes what the builders emit must change the hash. V4 added two such
/// markers - the unique-key flag (UNIQUE §4.10) and the lazy-reference marker (LAZY §6).
/// </summary>
public class SchemeHashCalculatorTests
{
    private static Guid HashOf(bool? unique, bool? lazy) =>
        SchemeHashCalculator.ComputeSchemeStructureHash(new List<RedbStructure>
        {
            new() { Id = 1, IdScheme = 7, IdType = RedbTypeIds.String, Name = "Code", Order = 1, Unique = unique, Lazy = lazy },
            new() { Id = 2, IdScheme = 7, IdType = RedbTypeIds.Object, Name = "Next", Order = 2 },
        });

    [Fact]
    public void UniqueFlag_IsPartOfTheHash()
    {
        HashOf(true, null).Should().NotBe(HashOf(false, null), "a key appearing or disappearing must invalidate the caches");
        HashOf(true, null).Should().NotBe(HashOf(null, null));
    }

    [Fact]
    public void LazyMarker_IsPartOfTheHash()
    {
        HashOf(null, true).Should().NotBe(HashOf(null, false), "virtual appearing or disappearing must invalidate the caches");
        HashOf(null, true).Should().NotBe(HashOf(null, null));
    }

    [Fact]
    public void SameStructures_SameHash()
    {
        HashOf(true, true).Should().Be(HashOf(true, true));
    }
}
