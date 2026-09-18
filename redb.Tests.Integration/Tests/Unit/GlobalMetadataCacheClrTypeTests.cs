using redb.Core.Caching;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Tests.Unit;

public class GlobalMetadataCacheClrTypeTests
{
    /// <summary>
    /// A scheme this domain already holds and no loaded type answers to is a scheme without a CLR type. Asking the
    /// provider for it again loaded the same scheme on every call: a list of 200 items of such a scheme was 200 scheme
    /// queries. No provider is given here - a call into it is the failure.
    /// </summary>
    [Fact]
    public async Task AKnownSchemeWithoutAClrType_IsAnsweredFromTheCache()
    {
        var cache = new GlobalMetadataCache($"unit-clr-{Guid.NewGuid():N}");
        var scheme = new RedbScheme { Id = 424242, Name = $"unit.no.such.type.{Guid.NewGuid():N}" };
        cache.CacheScheme(scheme);

        (await cache.ResolveClrTypeAsync(scheme.Id, schemeProvider: null!)).Should().BeNull();
        cache.ResolveClrType(scheme.Id, schemeProvider: null!).Should().BeNull();
    }
}
