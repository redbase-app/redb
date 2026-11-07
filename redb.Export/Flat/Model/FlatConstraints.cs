namespace redb.Export.Flat.Model;

/// <summary>A single-column foreign key of a flat table.</summary>
public sealed class FlatForeignKey
{
    /// <summary>Constraint name (already within the identifier limit).</summary>
    public string Name { get; init; } = "";

    /// <summary>Referencing column.</summary>
    public string Column { get; init; } = "";

    /// <summary>Referenced table. It may be created later than the referencing one, or be absent when its Props type is not part of the model.</summary>
    public string TargetTable { get; init; } = "";

    /// <summary>Referenced column.</summary>
    public string TargetColumn { get; init; } = "";

    /// <summary>For a <c>RedbObject&lt;T&gt;</c> reference: the Props type <c>T</c> the target table belongs to.</summary>
    public Type? TargetPropsType { get; init; }

    /// <summary>What happens to the referencing row when the referenced one is deleted.</summary>
    public FlatDeleteRule OnDelete { get; init; }
}

/// <summary>
/// A plain index of a flat table. The export creates one for every foreign key column that no key already
/// leads with, as EF Core does by convention: lookups by owner, parent or reference, and the checks a
/// cascading delete makes, then find the rows without reading the whole table.
/// </summary>
public sealed class FlatIndex
{
    /// <summary>Index name (already within the identifier limit).</summary>
    public string Name { get; init; } = "";

    /// <summary>Columns of the index, in order.</summary>
    public IReadOnlyList<string> Columns { get; init; } = [];
}

/// <summary>A unique constraint of a flat table.</summary>
public sealed class FlatUniqueConstraint
{
    /// <summary>Constraint name (already within the identifier limit).</summary>
    public string Name { get; init; } = "";

    /// <summary>Columns of the constraint, in order.</summary>
    public IReadOnlyList<string> Columns { get; init; } = [];
}
