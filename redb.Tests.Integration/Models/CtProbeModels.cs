using System;
using System.Collections.Generic;
using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Probe scheme for the ChangeTracking save-invariants suite (ревью 2026-09-03, волна 1.2 + Ш1/Ш2).
/// Synced by the tests that use it, not by the fixtures.
/// </summary>
[RedbScheme(Name = "CtProbe")]
public class CtProbeProps
{
    public string? Label { get; set; }
    public long[]? Longs { get; set; }
    public decimal[]? Decimals { get; set; }
    public string?[]? Tags { get; set; }
    public Dictionary<string, string>? Dict { get; set; }
    public Dictionary<(int Year, string Quarter), string>? TupleDict { get; set; }
    public List<CtProbeItem>? Items { get; set; }
    public List<RedbObject<CtProbeChildProps>>? Refs { get; set; }
    public Dictionary<string, RedbObject<CtProbeChildProps>>? RefDict { get; set; }
    public RedbListItem? Status { get; set; }
    public List<RedbListItem>? Roles { get; set; }
}

public class CtProbeItem
{
    public string? Name { get; set; }
    public decimal Price { get; set; }
    /// <summary>Ш4: собственный массив внутри класса-элемента массива (коллекция в коллекции).</summary>
    public long[]? Codes { get; set; }
    /// <summary>Ш4: собственный словарь внутри класса-элемента массива.</summary>
    public Dictionary<string, string>? Meta { get; set; }
}

/// <summary>Отдельная схема-ребёнок для пинов референс-коллекций (Ш2).</summary>
[RedbScheme(Name = "CtProbeChild")]
public class CtProbeChildProps
{
    public string? Tag { get; set; }
}
