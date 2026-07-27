using redb.Core.Attributes;
using redb.Core.Pro.Migration;

namespace redb.Tests.Integration.Models;

/// <summary>
/// Props for the Pro data-migration suite: TotalPrice is recomputed from Quantity * Price.
/// </summary>
[RedbScheme("Миграции: пробный объект", Name = "migration_probe")]
public class MigrationProbeProps
{
    public long Quantity { get; set; }
    public double Price { get; set; }
    public double TotalPrice { get; set; }
}

/// <summary>
/// The migration under test — the canonical ComputedFrom case from the Pro docs.
/// </summary>
public class MigrationProbeMigration : IRedbMigration<MigrationProbeProps>
{
    public void Configure(IMigrationBuilder<MigrationProbeProps> builder)
    {
        builder.Property(p => p.TotalPrice)
               .ComputedFrom(p => p.Quantity * p.Price);
    }
}
