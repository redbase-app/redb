using System.Text.Json.Serialization;

namespace redb.Export.Models;

/// <summary>
/// Abstract base class for all data records in a <c>.redb</c> JSONL stream.
/// Each concrete subclass maps to one REDB database table.
/// JSON polymorphism is handled via the <c>"type"</c> discriminator property.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(TypeRecord), "type")]
[JsonDerivedType(typeof(RoleRecord), "role")]
[JsonDerivedType(typeof(UserRecord), "user")]
[JsonDerivedType(typeof(UserRoleRecord), "user_role")]
[JsonDerivedType(typeof(ListRecord), "list")]
[JsonDerivedType(typeof(ListItemRecord), "list_item")]
[JsonDerivedType(typeof(SchemeRecord), "scheme")]
[JsonDerivedType(typeof(StructureRecord), "structure")]
[JsonDerivedType(typeof(ObjectRecord), "object")]
[JsonDerivedType(typeof(PermissionRecord), "permission")]
[JsonDerivedType(typeof(ValueRecord), "value")]
public abstract class ExportRecord
{
}

/// <summary>
/// Represents a row from the <c>_types</c> table (REDB type definition).
/// </summary>
public sealed class TypeRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Type name.</summary>
    public string Name { get; init; } = "";

    /// <summary>Database-specific type name (e.g. <c>"bigint"</c>).</summary>
    public string? DbType { get; init; }

    /// <summary>.NET CLR type name (e.g. <c>"System.Int64"</c>).</summary>
    public string? DotnetType { get; init; }
}

/// <summary>
/// Represents a row from the <c>_roles</c> table.
/// </summary>
public sealed class RoleRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Role name.</summary>
    public string Name { get; init; } = "";

    /// <summary>Optional configuration object reference.</summary>
    public long? IdConfiguration { get; init; }
}

/// <summary>
/// Represents a row from the <c>_users</c> table.
/// </summary>
public sealed class UserRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Login (username).</summary>
    public string Login { get; init; } = "";

    /// <summary>Password hash.</summary>
    public string Password { get; init; } = "";

    /// <summary>Display name.</summary>
    public string Name { get; init; } = "";

    /// <summary>Phone number.</summary>
    public string? Phone { get; init; }

    /// <summary>E-mail address.</summary>
    public string? Email { get; init; }

    /// <summary>Registration timestamp.</summary>
    public DateTimeOffset DateRegister { get; init; }

    /// <summary>Dismissal timestamp (null if user is active).</summary>
    public DateTimeOffset? DateDismiss { get; init; }

    /// <summary>Whether the user account is enabled.</summary>
    public bool Enabled { get; init; }

    /// <summary>Optional numeric key.</summary>
    public long? Key { get; init; }

    /// <summary>Optional integer code.</summary>
    public long? CodeInt { get; init; }

    /// <summary>Optional string code.</summary>
    public string? CodeString { get; init; }

    /// <summary>Optional GUID code.</summary>
    public Guid? CodeGuid { get; init; }

    /// <summary>Free-text note.</summary>
    public string? Note { get; init; }

    /// <summary>Row hash for change tracking.</summary>
    public Guid? Hash { get; init; }

    /// <summary>Optional configuration object reference.</summary>
    public long? IdConfiguration { get; init; }
}

/// <summary>
/// Represents a row from the <c>_users_roles</c> junction table.
/// </summary>
public sealed class UserRoleRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Foreign key to <c>_roles</c>.</summary>
    public long IdRole { get; init; }

    /// <summary>Foreign key to <c>_users</c>.</summary>
    public long IdUser { get; init; }
}

/// <summary>
/// Represents a row from the <c>_lists</c> table (enumeration/lookup list).
/// </summary>
public sealed class ListRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>List display name.</summary>
    public string Name { get; init; } = "";

    /// <summary>Machine-friendly alias.</summary>
    public string? Alias { get; init; }
}

/// <summary>
/// Represents a row from the <c>_list_items</c> table.
/// </summary>
public sealed class ListItemRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Foreign key to the parent <c>_lists</c> row.</summary>
    public long IdList { get; init; }

    /// <summary>Display value of the list item.</summary>
    public string? Value { get; init; }

    /// <summary>Machine-friendly alias.</summary>
    public string? Alias { get; init; }

    /// <summary>Optional object reference.</summary>
    public long? IdObject { get; init; }
}

/// <summary>
/// Represents a row from the <c>_schemes</c> table (schema/class definition).
/// </summary>
public sealed class SchemeRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Parent scheme identifier (for inheritance).</summary>
    public long? IdParent { get; init; }

    /// <summary>Scheme name.</summary>
    public string Name { get; init; } = "";

    /// <summary>Machine-friendly alias.</summary>
    public string? Alias { get; init; }

    /// <summary>Namespace for code generation.</summary>
    public string? NameSpace { get; init; }

    /// <summary>Hash of the scheme structure for change detection.</summary>
    public Guid? StructureHash { get; init; }

    /// <summary>Scheme type discriminator (0 = normal, 1 = primitive, etc.).</summary>
    public long SchemeType { get; init; }
}

/// <summary>
/// Represents a row from the <c>_structures</c> table (field/property definition).
/// </summary>
public sealed class StructureRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Parent structure identifier (for nested fields).</summary>
    public long? IdParent { get; init; }

    /// <summary>Foreign key to the owning scheme.</summary>
    public long IdScheme { get; init; }

    /// <summary>Override reference for inherited fields.</summary>
    public long? IdOverride { get; init; }

    /// <summary>Foreign key to <c>_types</c>.</summary>
    public long IdType { get; init; }

    /// <summary>Foreign key to <c>_lists</c> (for enumeration-typed fields).</summary>
    public long? IdList { get; init; }

    /// <summary>Field name.</summary>
    public string Name { get; init; } = "";

    /// <summary>Machine-friendly alias.</summary>
    public string? Alias { get; init; }

    /// <summary>Display order within the scheme.</summary>
    public long? Order { get; init; }

    /// <summary>Whether the field is read-only.</summary>
    public bool? Readonly { get; init; }

    /// <summary>Whether the field requires a non-null value.</summary>
    public bool? AllowNotNull { get; init; }

    /// <summary>Collection type (0 = none, 1 = list, 2 = dictionary, etc.).</summary>
    public long? CollectionType { get; init; }

    /// <summary>Key type for dictionary-typed fields.</summary>
    public long? KeyType { get; init; }

    /// <summary>Whether the value is stored compressed.</summary>
    public bool? IsCompress { get; init; }

    /// <summary>Whether explicit NULLs are persisted.</summary>
    public bool? StoreNull { get; init; }

    /// <summary>Serialized default value.</summary>
    public byte[]? DefaultValue { get; init; }

    /// <summary>Editor hint for UI generation.</summary>
    public string? DefaultEditor { get; init; }
}

/// <summary>
/// Represents a row from the <c>_objects</c> table (entity instance).
/// </summary>
public sealed class ObjectRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Parent object identifier (for tree structures).</summary>
    public long? IdParent { get; init; }

    /// <summary>Foreign key to the defining scheme.</summary>
    public long IdScheme { get; init; }

    /// <summary>Owner user identifier.</summary>
    public long IdOwner { get; init; }

    /// <summary>User who last modified the object.</summary>
    public long IdWhoChange { get; init; }

    /// <summary>Creation timestamp.</summary>
    public DateTimeOffset DateCreate { get; init; }

    /// <summary>Last modification timestamp.</summary>
    public DateTimeOffset DateModify { get; init; }

    /// <summary>Optional workflow start date.</summary>
    public DateTimeOffset? DateBegin { get; init; }

    /// <summary>Optional workflow completion date.</summary>
    public DateTimeOffset? DateComplete { get; init; }

    /// <summary>Optional numeric key.</summary>
    public long? Key { get; init; }

    /// <summary>Display name.</summary>
    public string? Name { get; init; }

    /// <summary>Free-text note.</summary>
    public string? Note { get; init; }

    /// <summary>Row hash for change tracking.</summary>
    public Guid? Hash { get; init; }

    // --- Primitive value columns for RedbPrimitive<T> ---

    /// <summary>Inline <see cref="long"/> value (for primitive schemes).</summary>
    public long? ValueLong { get; init; }

    /// <summary>Inline <see cref="string"/> value (for primitive schemes).</summary>
    public string? ValueString { get; init; }

    /// <summary>Inline <see cref="Guid"/> value (for primitive schemes).</summary>
    public Guid? ValueGuid { get; init; }

    /// <summary>Inline <see cref="bool"/> value (for primitive schemes).</summary>
    public bool? ValueBool { get; init; }

    /// <summary>Inline <see cref="double"/> value (for primitive schemes).</summary>
    public double? ValueDouble { get; init; }

    /// <summary>Inline <see cref="decimal"/> value (for primitive schemes).</summary>
    public decimal? ValueNumeric { get; init; }

    /// <summary>Inline <see cref="DateTimeOffset"/> value (for primitive schemes).</summary>
    public DateTimeOffset? ValueDatetime { get; init; }

    /// <summary>Inline binary value (for primitive schemes).</summary>
    public byte[]? ValueBytes { get; init; }
}

/// <summary>
/// Represents a row from the <c>_permissions</c> table.
/// </summary>
public sealed class PermissionRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Role to which the permission applies (mutually exclusive with <see cref="IdUser"/>).</summary>
    public long? IdRole { get; init; }

    /// <summary>User to which the permission applies (mutually exclusive with <see cref="IdRole"/>).</summary>
    public long? IdUser { get; init; }

    /// <summary>Reference identifier (scheme or object) the permission targets.</summary>
    public long IdRef { get; init; }

    /// <summary>SELECT permission flag.</summary>
    public bool? Select { get; init; }

    /// <summary>INSERT permission flag.</summary>
    public bool? Insert { get; init; }

    /// <summary>UPDATE permission flag.</summary>
    public bool? Update { get; init; }

    /// <summary>DELETE permission flag.</summary>
    public bool? Delete { get; init; }
}

/// <summary>
/// Represents a row from the <c>_values</c> table (EAV property value).
/// Exactly one of the typed value columns is populated per row.
/// </summary>
public sealed class ValueRecord : ExportRecord
{
    /// <summary>Primary key.</summary>
    public long Id { get; init; }

    /// <summary>Foreign key to <c>_structures</c>.</summary>
    public long IdStructure { get; init; }

    /// <summary>Foreign key to <c>_objects</c>.</summary>
    public long IdObject { get; init; }

    /// <summary>String value.</summary>
    public string? String { get; init; }

    /// <summary>Integer value.</summary>
    public long? Long { get; init; }

    /// <summary>GUID value.</summary>
    public Guid? Guid { get; init; }

    /// <summary>Double-precision floating-point value.</summary>
    public double? Double { get; init; }

    /// <summary>Date/time value with timezone offset.</summary>
    public DateTimeOffset? DateTimeOffset { get; init; }

    /// <summary>Boolean value.</summary>
    public bool? Boolean { get; init; }

    /// <summary>Binary (byte array) value.</summary>
    public byte[]? ByteArray { get; init; }

    /// <summary>Decimal (numeric) value.</summary>
    public decimal? Numeric { get; init; }

    /// <summary>Foreign key to <c>_list_items</c>.</summary>
    public long? ListItem { get; init; }

    /// <summary>Foreign key to another <c>_objects</c> row (object reference).</summary>
    public long? Object { get; init; }

    /// <summary>Parent row identifier for array/collection elements.</summary>
    public long? ArrayParentId { get; init; }

    /// <summary>Index or key within an array/dictionary.</summary>
    public string? ArrayIndex { get; init; }
}
