using redb.Core.Attributes;

namespace redb.Tests.Integration.Models;

// Props for the scheme-sync type-migration suite.
//
// Note what these deliberately do NOT do: two classes sharing one [RedbScheme(Name = "...")] would be
// the obvious way to stage "the property changed type", but the library refuses it — a duplicate
// explicit name is a hard error (ClrSchemeTypeIndex.ThrowIfNameConflict and RedbSchemeNameConflictException), and rightly so.
// The suite therefore stages the change the way it actually happens in the field: it writes values
// through the current class, then rewrites _structures._id_type and the storage column with raw SQL to
// the shape an EARLIER version of the class would have produced, and re-syncs the same class. What the
// sync path then sees is indistinguishable from a real deployment where the property's type changed.

/// <summary>
/// Target of the supported case: values are staged as Boolean and the property asks for <c>int</c>,
/// which every provider can convert.
/// </summary>
[RedbScheme("Миграция типа: поддержанная", Name = "type_migration_supported")]
public class TypeMigrationIntProps
{
    /// <summary>The property whose type changes. Staged as Boolean, declared here as int.</summary>
    public int Flag { get; set; }

    /// <summary>Untouched neighbour: proves the migration does not disturb the rest of the structure.</summary>
    public string Tag { get; set; } = "";
}

/// <summary>
/// Target of the unsupported case: Boolean to Guid is in no provider's conversion matrix, so the
/// migration must refuse and synchronisation must stop.
/// </summary>
[RedbScheme("Миграция типа: неподдержанная", Name = "type_migration_unsupported")]
public class TypeMigrationGuidProps
{
    public Guid Flag { get; set; }

    public string Tag { get; set; } = "";
}

/// <summary>
/// Same storage column on both sides (Int and Long both live in <c>_Long</c>): the type changes, the
/// values do not move, and nothing may be lost.
/// </summary>
[RedbScheme("Миграция типа: та же колонка", Name = "type_migration_samecolumn")]
public class TypeMigrationLongProps
{
    public long Flag { get; set; }

    public string Tag { get; set; } = "";
}

/// <summary>
/// Text to Boolean, the conversion that used to destroy what it could not read: the CASE fell through
/// to NULL for an unrecognised token while the same statement cleared the source column, and the row
/// was counted as a success.
/// </summary>
[RedbScheme("Миграция типа: булева из текста", Name = "type_migration_bool_from_text")]
public class TypeMigrationBoolProps
{
    public bool Flag { get; set; }

    public string Tag { get; set; } = "";
}

/// <summary>
/// Text source with rows that cannot be read as a number: the convertible ones move, the rest stay,
/// and the partial result is a failure — synchronisation stops rather than stranding either group.
/// </summary>
[RedbScheme("Миграция типа: частичная", Name = "type_migration_partial")]
public class TypeMigrationPartialProps
{
    public long Flag { get; set; }

    public string Tag { get; set; } = "";
}
