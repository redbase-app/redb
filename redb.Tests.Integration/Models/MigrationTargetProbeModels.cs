using redb.Core.Attributes;
using redb.Core.Pro.Migration;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Probes how the migration DSL ADDRESSES a field that is not a root scheme field: a member of a
/// nested class and a member of an array element.
/// <para>
/// Written for review finding PR-32, which states that the compiled migration SQL correlates the
/// source subquery by object id and structure id only, carrying neither <c>_array_index</c> nor
/// <c>_parent_value_id</c>. That finding only bites if such a target is reachable through the DSL
/// at all, so this suite establishes reachability first and records the observed behaviour.
/// </para>
/// <para>
/// Field names are deliberately distinct between levels (<c>GrandTotal</c> at the root,
/// <c>LineTotal</c> inside) so a DSL that flattens a dotted path to its leaf name cannot silently
/// resolve to the root field of the same shape.
/// </para>
/// </summary>
[RedbScheme("Миграции: вложенная цель", Name = "migration_target_probe")]
public class MigrationTargetProbeProps
{
    public double Base { get; set; }
    public double Multiplier { get; set; }
    public double GrandTotal { get; set; }
    public LineTargetProps? Details { get; set; }
    public List<LineTargetProps> Items { get; set; } = new();
}

/// <summary>Nested (non-array) member and the array element member.</summary>
public class LineTargetProps
{
    public double Qty { get; set; }
    public double Price { get; set; }
    public double LineTotal { get; set; }
}

/// <summary>Control: both target and sources are ROOT fields — the shape every existing migration test uses.</summary>
public class MigrationTargetRootMigration : IRedbMigration<MigrationTargetProbeProps>
{
    public void Configure(IMigrationBuilder<MigrationTargetProbeProps> builder)
    {
        builder.Property(p => p.GrandTotal)
               .ComputedFrom(p => p.Base * p.Multiplier);
    }
}

/// <summary>Nested-class target and nested sources.</summary>
public class MigrationTargetNestedMigration : IRedbMigration<MigrationTargetProbeProps>
{
    public void Configure(IMigrationBuilder<MigrationTargetProbeProps> builder)
    {
        builder.Property(p => p.Details!.LineTotal)
               .ComputedFrom(p => p.Details!.Qty * p.Details!.Price);
    }
}

/// <summary>Array-element target and array-element sources.</summary>
public class MigrationTargetArrayMigration : IRedbMigration<MigrationTargetProbeProps>
{
    public void Configure(IMigrationBuilder<MigrationTargetProbeProps> builder)
    {
        builder.Property(p => p.Items[0].LineTotal)
               .ComputedFrom(p => p.Items[0].Qty * p.Items[0].Price);
    }
}
