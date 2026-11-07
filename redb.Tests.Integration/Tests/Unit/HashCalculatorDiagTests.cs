using redb.Core.Utils;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// DIAGNOSTIC — does RedbHash.ComputeFor actually distinguish a token whose only difference is Status?
/// If Valid and Redeemed hash to the SAME value, the cache's hash-validation can never see the redeem,
/// and LoadAsync returns a stale snapshot — matching the DIAG report (hit=True, dbHash stable).
/// No DB — pure calculator check. Mirrors redb.Identity TokenProps shape.
/// </summary>
public sealed class HashCalculatorDiagTests
{
    // Same shape as redb.Identity.Core TokenProps.
    private sealed class TokenLikeProps
    {
        public long ApplicationObjectId { get; set; }
        public long AuthorizationObjectId { get; set; }
        public string? Status { get; set; }
        public string? Type { get; set; }
        public string? ReferenceId { get; set; }
        public string? Payload { get; set; }
        public Dictionary<string, string>? Properties { get; set; }
        public string? ActorSubject { get; set; }
    }

    private static TokenLikeProps Token(string status) => new()
    {
        ApplicationObjectId = 111,
        AuthorizationObjectId = 222,
        Status = status,
        Type = "access_token",
        ReferenceId = "ref-abc",
        Payload = "{\"jwt\":\"x\"}",
        Properties = new Dictionary<string, string> { ["scope"] = "openid", ["k"] = "v" },
        ActorSubject = "sub-1",
    };

    [Fact]
    public void Status_ChangesHash()
    {
        var valid = RedbHash.ComputeForProps(Token("valid"));
        var redeemed = RedbHash.ComputeForProps(Token("redeemed"));

        (valid != redeemed).Should().BeTrue(
            "changing only Status MUST change the hash — otherwise the cache can never see a redeem");
    }

    [Fact]
    public void SameState_SameHash_Deterministic()
    {
        // Two identical tokens (incl. same Dictionary) must hash equal across repeated computes —
        // otherwise Dictionary enumeration order makes the hash unstable (false cache misses / chaos).
        var a = RedbHash.ComputeForProps(Token("valid"));
        var b = RedbHash.ComputeForProps(Token("valid"));

        (a == b).Should().BeTrue( "identical props (including Dictionary) must hash identically every time");
    }

    [Fact]
    public void DictionaryOrder_DoesNotAffectHash()
    {
        var t1 = Token("valid");
        t1.Properties = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
        var t2 = Token("valid");
        t2.Properties = new Dictionary<string, string> { ["b"] = "2", ["a"] = "1" };  // reversed insert order

        (RedbHash.ComputeForProps(t1) == RedbHash.ComputeForProps(t2)).Should().BeTrue(
            "Dictionary content, not insertion order, must define the hash");
    }
}
