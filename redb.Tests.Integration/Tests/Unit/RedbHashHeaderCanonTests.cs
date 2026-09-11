using redb.Core.Models.Entities;
using redb.Core.Utils;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Full-object-hash plan (2026-09-11): <c>_hash</c> covers the header of the row, not only the
/// Props. What is in and what is out is a contract, pinned here without a database:
/// every header column takes part; the audit trio (date_create, date_modify, who_change) and the
/// identity (id, scheme id) do not - they are stamped outside the hash computation and a hash
/// that carried them could never be reproduced from a reloaded row; header dates canonicalise
/// at second precision in UTC, so a value survives the round trip through any of the three
/// databases.
/// </summary>
public sealed class RedbHashHeaderCanonTests
{
    private static RedbObject<SimpleProps> Baseline() => new()
    {
        id = 42, scheme_id = 7, parent_id = 3, owner_id = 1, who_change_id = 1,
        name = "n", note = "note", key = 5,
        value_long = 1, value_string = "s", value_guid = Guid.Parse("11111111-1111-1111-1111-111111111111"),
        value_bool = true, value_double = 1.5, value_numeric = 2.25m,
        value_datetime = new DateTimeOffset(2026, 9, 11, 12, 0, 0, TimeSpan.Zero),
        value_bytes = [1, 2, 3], value_unique = "K-1",
        date_begin = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
        date_complete = new DateTimeOffset(2026, 2, 1, 0, 0, 0, TimeSpan.Zero),
        date_create = new DateTimeOffset(2026, 9, 1, 0, 0, 0, TimeSpan.Zero),
        date_modify = new DateTimeOffset(2026, 9, 2, 0, 0, 0, TimeSpan.Zero),
        Props = new SimpleProps { Title = "p", Count = 1 }
    };

    private static Guid HashOf(Action<RedbObject<SimpleProps>> mutate)
    {
        var obj = Baseline();
        mutate(obj);
        return RedbHash.ComputeFor(obj)!.Value;
    }

    public static IEnumerable<object[]> HeaderMutations() =>
    [
        [nameof(RedbObject.name), (Action<RedbObject<SimpleProps>>)(o => o.name = "other")],
        [nameof(RedbObject.note), (Action<RedbObject<SimpleProps>>)(o => o.note = "other")],
        [nameof(RedbObject.parent_id), (Action<RedbObject<SimpleProps>>)(o => o.parent_id = 4)],
        [nameof(RedbObject.owner_id), (Action<RedbObject<SimpleProps>>)(o => o.owner_id = 2)],
        [nameof(RedbObject.key), (Action<RedbObject<SimpleProps>>)(o => o.key = 6)],
        [nameof(RedbObject.value_long), (Action<RedbObject<SimpleProps>>)(o => o.value_long = 2)],
        [nameof(RedbObject.value_string), (Action<RedbObject<SimpleProps>>)(o => o.value_string = "t")],
        [nameof(RedbObject.value_guid), (Action<RedbObject<SimpleProps>>)(o => o.value_guid = Guid.NewGuid())],
        [nameof(RedbObject.value_bool), (Action<RedbObject<SimpleProps>>)(o => o.value_bool = false)],
        [nameof(RedbObject.value_double), (Action<RedbObject<SimpleProps>>)(o => o.value_double = 2.5)],
        [nameof(RedbObject.value_numeric), (Action<RedbObject<SimpleProps>>)(o => o.value_numeric = 3.25m)],
        [nameof(RedbObject.value_datetime), (Action<RedbObject<SimpleProps>>)(o => o.value_datetime = o.value_datetime!.Value.AddSeconds(1))],
        [nameof(RedbObject.value_bytes), (Action<RedbObject<SimpleProps>>)(o => o.value_bytes = [1, 2, 4])],
        [nameof(RedbObject.value_unique), (Action<RedbObject<SimpleProps>>)(o => o.value_unique = "K-2")],
        [nameof(RedbObject.date_begin), (Action<RedbObject<SimpleProps>>)(o => o.date_begin = o.date_begin!.Value.AddDays(1))],
        [nameof(RedbObject.date_complete), (Action<RedbObject<SimpleProps>>)(o => o.date_complete = null)],
    ];

    [Theory]
    [MemberData(nameof(HeaderMutations))]
    public void EveryHeaderColumn_TakesPart(string column, Action<RedbObject<SimpleProps>> mutate)
    {
        HashOf(mutate).Should().NotBe(HashOf(_ => { }),
            $"a change of {column} on another node must move _hash, or the props cache serves the stale header");
    }

    [Fact]
    public void Props_StillTakePart()
    {
        HashOf(o => o.Props.Count = 2).Should().NotBe(HashOf(_ => { }));
    }

    [Fact]
    public void AuditAndIdentity_DoNotTakePart()
    {
        var baseline = HashOf(_ => { });
        HashOf(o => o.date_create = o.date_create.AddDays(1)).Should().Be(baseline,
            "date_create is stamped on the row outside the hash computation");
        HashOf(o => o.date_modify = o.date_modify.AddDays(1)).Should().Be(baseline,
            "date_modify is stamped on every save; with it in the hash no resave could ever be a no-op");
        HashOf(o => o.who_change_id = 9).Should().Be(baseline, "who_change is audit, not content");
        HashOf(o => o.id = 43).Should().Be(baseline, "the id is identity, and 0 until the save assigns it");
        HashOf(o => o.scheme_id = 8).Should().Be(baseline,
            "the scheme id is assigned after the batch recomputes hashes - in the hash it would cost every new object its first hits");
        HashOf(o => o.hash = Guid.NewGuid()).Should().Be(baseline, "the hash never hashes itself");
    }

    [Fact]
    public void HeaderDates_CanonAtSecondsInUtc()
    {
        var baseline = HashOf(_ => { });
        HashOf(o => o.value_datetime = o.value_datetime!.Value.AddMilliseconds(999)).Should().Be(baseline,
            "sub-second digits do not survive every database (SQLite REAL Julian keeps ~40 microseconds) - the canon stops at seconds");
        HashOf(o => o.value_datetime = o.value_datetime!.Value.ToOffset(TimeSpan.FromHours(3))).Should().Be(baseline,
            "the same instant in another offset is the same value - MSSQL keeps the offset, PostgreSQL returns UTC");
        HashOf(o => o.date_begin = o.date_begin!.Value.AddTicks(5)).Should().Be(baseline);
    }

    [Fact]
    public void BaseFieldsHash_IsTheHeaderCanon()
    {
        // An object without Props hashes its header alone - the same canon, so a typed object
        // with null Props and a non-generic RedbObject agree with each other.
        var a = Baseline(); a.Props = null!;
        var b = Baseline(); b.Props = null!; b.name = "other";
        RedbHash.ComputeForBaseFields(a).Should().NotBe(RedbHash.ComputeForBaseFields(b));
        var c = Baseline(); c.Props = null!; c.date_modify = c.date_modify.AddDays(1);
        RedbHash.ComputeForBaseFields(a).Should().Be(RedbHash.ComputeForBaseFields(c));
    }
}
