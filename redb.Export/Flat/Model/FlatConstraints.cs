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
}

/// <summary>A unique constraint of a flat table.</summary>
public sealed class FlatUniqueConstraint
{
    /// <summary>Constraint name (already within the identifier limit).</summary>
    public string Name { get; init; } = "";

    /// <summary>Columns of the constraint, in order.</summary>
    public IReadOnlyList<string> Columns { get; init; } = [];
}
