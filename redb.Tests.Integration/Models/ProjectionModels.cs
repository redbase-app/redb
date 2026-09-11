using System;
using System.Collections.Generic;
using redb.Core.Attributes;
using redb.Core.Models.Entities;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Probe scheme for the Select-projection suite (review 2026-09-03, wave 1.1).
/// Synced by the tests that use it, not by the fixtures.
/// </summary>
[RedbScheme(Name = "ProjectionProbe")]
public class ProjectionProbeProps
{
    public string? S { get; set; }
    public int N { get; set; }
    public decimal? D { get; set; }
    public bool Flag { get; set; }
    public ProjectionProbeNested? Nested { get; set; }
    public List<ProjectionProbeItem>? Items { get; set; }
    public string[]? Tags { get; set; }
    public RedbObject<ProjectionProbeChildProps>? Child { get; set; }
}

public class ProjectionProbeNested
{
    public string? City { get; set; }
    public int Zip { get; set; }
}

public class ProjectionProbeItem
{
    public string? Name { get; set; }
    public decimal Price { get; set; }
}

[RedbScheme(Name = "ProjectionProbeChild")]
public class ProjectionProbeChildProps
{
    public string? Label { get; set; }
}

/// <summary>DTO target for MemberInit projections (S-5/G-1 shapes).</summary>
public class ProjectionDto
{
    public string? A { get; set; }
    public int N { get; set; }
    public ProjectionSubDto Sub { get; set; } = new();
}

public class ProjectionSubDto
{
    public int X { get; set; }
}
