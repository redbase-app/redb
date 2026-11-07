using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

// ───────────────────────────────────────────────
// Models for the byte[] storage conversion (V4, work Б1): a byte[] at the root and one nested in
// a class, both of which the pre-V4 sync classified as Array-of-Byte. Synced by the tests.
// ───────────────────────────────────────────────

[RedbScheme(Name = "ByteHolder")]
public class ByteHolderProps
{
    public byte[]? Root { get; set; }

    public ByteAttachment? File { get; set; }
}

public class ByteAttachment
{
    public string? Name { get; set; }

    public byte[]? Data { get; set; }
}
