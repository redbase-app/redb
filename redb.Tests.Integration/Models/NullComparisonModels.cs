using System;
using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Pairs of nullable fields of every kind a filter compares: a number, a text, a date, a flag and a list item, each
/// left empty on objects of its own period, so a filter meets objects without a value on one side, the other or both.
/// </summary>
[RedbScheme("TestNullComparisons")]
public class NullComparisonProps
{
    public string Tag { get; set; } = string.Empty;

    public int? N { get; set; }
    public int? N2 { get; set; }
    public string? S { get; set; }
    public string? S2 { get; set; }
    public DateTime? D { get; set; }
    public DateTime? D2 { get; set; }
    public bool? B { get; set; }
    public bool? B2 { get; set; }
    public RedbListItem? L { get; set; }
    public RedbListItem? L2 { get; set; }
}

public enum ContainsColor { Red, Green, Blue }

/// <summary>A nullable field of every value type a Props field can have, for Contains over a list of that type.</summary>
[RedbScheme("TestContainsTypes")]
public class ContainsTypesProps
{
    public string Tag { get; set; } = string.Empty;

    public long? I64 { get; set; }
    public int? I32 { get; set; }
    public short? I16 { get; set; }
    public byte? U8 { get; set; }
    public double? F64 { get; set; }
    public float? F32 { get; set; }
    public decimal? Dec { get; set; }
    public bool? Flag { get; set; }
    public char? Ch { get; set; }
    public string? Text { get; set; }
    public Guid? Id { get; set; }
    public DateTime? Dt { get; set; }
    public DateTimeOffset? Dto { get; set; }
    public DateOnly? Day { get; set; }
    public TimeOnly? Time { get; set; }
    public TimeSpan? Span { get; set; }
    public ContainsColor? Color { get; set; }
}
