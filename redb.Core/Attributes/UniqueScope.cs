namespace redb.Core.Attributes;

/// <summary>
/// Scope of a <see cref="RedbUniqueAttribute"/> key on a COLLECTION property (S3 of the
/// subtree-unique plan). On scalars and nested classes the scope is not configurable - the
/// bare attribute already means the strongest sensible reading (value unique per scheme,
/// subtree content unique per scheme).
/// </summary>
public enum UniqueScope
{
    /// <summary>
    /// The default reading of the bare attribute: for a scalar - the value canon; for a class,
    /// array or dictionary - the SUBTREE content key (no two objects hold this exact content).
    /// </summary>
    Default = 0,

    /// <summary>
    /// Element scope, scheme-wide: every ELEMENT value of the collection is unique across all
    /// objects of the scheme ("this email appears in one object only, whichever array it sits
    /// in"). References canonicalise by target id: "this object is attached at most once in
    /// the whole scheme".
    /// </summary>
    Scheme = 1,

    /// <summary>
    /// Element scope, per collection: no duplicate element values INSIDE one object's
    /// collection; different objects may repeat each other freely. The classic "unique set" -
    /// for references: "no duplicate links in this collection".
    /// </summary>
    Collection = 2,
}
