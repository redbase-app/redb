using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A round trip of collections of nullable value types: dictionaries of int?, long?, decimal?, DateTime?, bool? and
/// Guid?, arrays and lists of nullable numbers and flags, and a dictionary of a class with nullable fields. Each holds
/// a null among its values; the null keeps its key or its position, every other value reads back as saved.
/// </summary>
public abstract class NullableCollectionsTestsBase
{
    protected readonly IRedbService Redb;

    protected NullableCollectionsTestsBase(IRedbService redb) => Redb = redb;

    private static readonly DateTime At = new(2026, 11, 18, 10, 30, 0, DateTimeKind.Utc);
    private static readonly Guid Id = Guid.Parse("6f1c2a4e-0b7d-4c55-9a3e-2f8d1b9c7e01");

    private static NullableCollectionsProps Sample(string tag) => new()
    {
        Tag = tag,
        Ints = new() { ["a"] = 1, ["n"] = null, ["z"] = -7 },
        Longs = new() { ["a"] = 5_000_000_000L, ["n"] = null },
        Decimals = new() { ["a"] = 12.345m, ["n"] = null },
        Dates = new() { ["a"] = At, ["n"] = null },
        Flags = new() { ["t"] = true, ["f"] = false, ["n"] = null },
        Guids = new() { ["a"] = Id, ["n"] = null },
        IntArray = [1, null, 3],
        LongList = [null, 5_000_000_000L],
        DecimalList = [1.5m, null, -2.25m],
        FlagArray = [true, null, false],
        Entries = new()
        {
            ["x"] = new NullableCollectionsEntry { Name = "x", Score = 4, At = At, Marks = [2, null, 5] },
            ["y"] = new NullableCollectionsEntry { Name = "y", Score = null, At = null }
        }
    };

    private static DateTime? Utc(DateTime? d) => d?.Kind == DateTimeKind.Local ? d.Value.ToUniversalTime() : d;

    private async Task<NullableCollectionsProps> RoundTripAsync()
    {
        await Redb.SyncSchemeAsync<NullableCollectionsProps>();
        var tag = "nc-" + Guid.NewGuid().ToString("N")[..8];
        var id = await Redb.SaveAsync(new RedbObject<NullableCollectionsProps> { name = tag, Props = Sample(tag) });
        var loaded = await Redb.LoadAsync<NullableCollectionsProps>(id);
        loaded.Should().NotBeNull();
        return loaded!.Props;
    }

    [Fact]
    public async Task DictionariesOfNullableValueTypes_ReadBackAsSaved_TheNullEntryKept()
    {
        var p = await RoundTripAsync();
        var expected = Sample(p.Tag);

        p.Ints.Should().BeEquivalentTo(expected.Ints);
        p.Longs.Should().BeEquivalentTo(expected.Longs);
        p.Decimals.Should().BeEquivalentTo(expected.Decimals);
        p.Dates!.ToDictionary(kv => kv.Key, kv => Utc(kv.Value)).Should().BeEquivalentTo(expected.Dates);
        p.Flags.Should().BeEquivalentTo(expected.Flags);
        p.Guids.Should().BeEquivalentTo(expected.Guids);
    }

    [Fact]
    public async Task ArraysAndListsOfNullableValueTypes_ReadBackAsSaved_TheNullInPlace()
    {
        var p = await RoundTripAsync();

        p.IntArray.Should().Equal(1, null, 3);
        p.LongList.Should().Equal(null, 5_000_000_000L);
        p.DecimalList.Should().Equal(1.5m, null, -2.25m);
        p.FlagArray.Should().Equal(true, null, false);
    }

    [Fact]
    public async Task DictionaryOfAClassWithNullableFields_ReadsBackAsSaved()
    {
        var p = await RoundTripAsync();

        p.Entries.Should().HaveCount(2);
        p.Entries!["x"].Score.Should().Be(4);
        Utc(p.Entries["x"].At).Should().Be(At);
        p.Entries["y"].Score.Should().BeNull();
        p.Entries["y"].At.Should().BeNull();
        p.Entries["x"].Marks.Should().Equal(2, null, 5);
        p.Entries["y"].Marks.Should().BeNull();
    }
}
