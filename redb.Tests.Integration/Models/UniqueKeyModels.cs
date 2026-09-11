using System;
using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

// ───────────────────────────────────────────────
// Models for the [RedbUnique] key suite (V4, UNIQUE stage 2).
// Synced by the tests that use them, not by the fixtures.
// ───────────────────────────────────────────────

/// <summary>
/// One scheme, several independent keys — one per canonical-form rule of
/// <c>UniqueKeyEncoder</c>. Each key is nullable: NULL never takes part in uniqueness.
/// </summary>
[RedbScheme(Name = "UniqueOrder")]
public class UniqueOrderProps
{
    [RedbUnique]
    public string? Code { get; set; }

    [RedbUnique]
    public long? Number { get; set; }

    [RedbUnique]
    public Guid? Token { get; set; }

    [RedbUnique]
    public decimal? Amount { get; set; }

    [RedbUnique]
    public DateTimeOffset? IssuedAt { get; set; }

    // Б1 (owner decision 2026-08-31): byte[] is a root scalar in _values._ByteArray, so it can be
    // a key - dedup by content is the ordinary byte[] scenario (plan decision 4).
    [RedbUnique]
    public byte[]? Fingerprint { get; set; }

    /// <summary>Not a key — the control field: duplicates here must stay legal.</summary>
    public string? Note { get; set; }
}

/// <summary>
/// A second scheme with the same key property name: uniqueness is per scheme, so the same
/// <c>Code</c> in <c>UniqueOrder</c> and <c>UniqueInvoice</c> must coexist.
/// </summary>
[RedbScheme(Name = "UniqueInvoice")]
public class UniqueInvoiceProps
{
    [RedbUnique]
    public string? Code { get; set; }

    public string? Note { get; set; }
}

/// <summary>A scheme with no keys at all — must stay completely untouched by the machinery.</summary>
[RedbScheme(Name = "UniqueFree")]
public class UniqueFreeProps
{
    public string? Code { get; set; }
    public string? Note { get; set; }
}

/// <summary>
/// S2 (subtree-unique plan): a key on a scalar INSIDE a nested class, reached without crossing
/// a collection. The nested field's rows carry their own structure id, so the same index
/// enforces them exactly like root keys.
/// </summary>
[RedbScheme(Name = "UniqueNested")]
public class UniqueNestedProps
{
    public string? Title { get; set; }
    public NestedIdentity? Identity { get; set; }
}

public class NestedIdentity
{
    [RedbUnique]
    public string? Passport { get; set; }

    public string? Issuer { get; set; }
}

/// <summary>
/// S1 (subtree-unique plan): [RedbUnique] on a root nested class, array and dictionary - the
/// key is the CONTENT of the whole subtree (the canonical hash the storage already maintains
/// on the base row), unique per scheme.
/// </summary>
[RedbScheme(Name = "UniqueSubtree")]
public class UniqueSubtreeProps
{
    [RedbUnique]
    public SubtreeConfig? Config { get; set; }

    [RedbUnique]
    public List<long>? Slots { get; set; }

    [RedbUnique]
    public Dictionary<string, long>? Limits { get; set; }

    public string? Note { get; set; }
}

public class SubtreeConfig
{
    public string? Region { get; set; }
    public long? Tier { get; set; }
}

/// <summary>
/// S3 (subtree-unique plan): ELEMENT keys on collections via [RedbUnique(Scope = ...)].
/// Collection scope - no duplicates inside one object's collection; Scheme scope - element
/// values unique across every object of the scheme.
/// </summary>
[RedbScheme(Name = "UniqueElems")]
public class UniqueElementsProps
{
    [RedbUnique(Scope = UniqueScope.Collection)]
    public List<string>? Codes { get; set; }

    [RedbUnique(Scope = UniqueScope.Scheme)]
    public List<string>? Emails { get; set; }

    [RedbUnique(Scope = UniqueScope.Collection)]
    public List<RedbObject<UniqueFreeProps>>? Links { get; set; }

    public string? Note { get; set; }
}

/// <summary>
/// The forbidden shape for S3: Scope on a SCALAR is meaningless - the bare attribute already
/// carries the strongest reading. Not [RedbScheme]-marked: its sync must throw, and the
/// fixtures auto-sync every marked type.
/// </summary>
public class ScopedScalarProps
{
    [RedbUnique(Scope = UniqueScope.Collection)]
    public string? Code { get; set; }
}

/// <summary>Two levels of nesting: the path machinery must be depth-agnostic.</summary>
[RedbScheme(Name = "UniqueDeep")]
public class UniqueDeepProps
{
    public DeepOuter? Outer { get; set; }
}

public class DeepOuter
{
    public DeepInner? Inner { get; set; }
}

public class DeepInner
{
    [RedbUnique]
    public string? Code { get; set; }
}

/// <summary>
/// The forbidden shape: a key inside an ELEMENT of a collection of classes. Every element of
/// every object shares one structure, so "unique" would degenerate to one value across all
/// elements of all objects — the validator must reject it at synchronisation.
/// Deliberately NOT [RedbScheme]-marked: the fixtures auto-sync every marked type of the
/// assembly at InitializeAsync, and a model whose sync must THROW would kill the fixture.
/// The test syncs it explicitly; its scheme name is the CLR FullName.
/// </summary>
public class UniqueInElementProps
{
    public List<KeyedElement>? Items { get; set; }
}

public class KeyedElement
{
    [RedbUnique]
    public string? Code { get; set; }
}
