namespace redb.Core.Query.Mapping;

/// <summary>
/// Maps C# property names to SQL column names for base RedbObject fields.
/// DB-agnostic: same column names across all databases (_objects table).
/// </summary>
public static class BaseFieldMapper
{
    private static readonly HashSet<string> _baseFieldNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "Id", "Name", "ParentId", "SchemeId", "DateCreate", "DateModify", "DateDelete",
        "Hash", "ValueLong", "ValueString", "ValueGuid", "ValueBool", "OwnerId", "WhoChangeId",
        "ValueDouble", "ValueNumeric", "ValueDatetime", "ValueBytes",
        "Note", "Key", "DateBegin", "DateComplete",
        "_id", "_name", "_id_parent", "_id_scheme", "_date_create", "_date_modify", "_date_delete",
        "_hash", "_value_long", "_value_string", "_value_guid", "_value_bool", "_id_owner", "_id_who_change",
        "_value_double", "_value_numeric", "_value_datetime", "_value_bytes",
        "_note", "_key", "_date_begin", "_date_complete"
    };

    /// <summary>
    /// Checks if the field name is a base RedbObject field (not a Props field).
    /// </summary>
    public static bool IsBaseField(string name) => _baseFieldNames.Contains(name);

    /// <summary>
    /// Maps C# property name to SQL column name.
    /// Returns "_id" as default for unknown fields.
    /// </summary>
    public static string MapToColumn(string fieldPath) => fieldPath.ToLowerInvariant() switch
    {
        "id" or "_id" => "_id",
        "name" or "_name" => "_name",
        "parentid" or "_id_parent" or "parent_id" => "_id_parent",
        "schemeid" or "_id_scheme" or "scheme_id" => "_id_scheme",
        "datecreate" or "_date_create" or "date_create" => "_date_create",
        "datemodify" or "_date_modify" or "date_modify" => "_date_modify",
        "datedelete" or "_date_delete" or "date_delete" => "_date_delete",
        "hash" or "_hash" => "_hash",
        "valuelong" or "_value_long" or "value_long" => "_value_long",
        "valuestring" or "_value_string" or "value_string" => "_value_string",
        "valueguid" or "_value_guid" or "value_guid" => "_value_guid",
        "valuebool" or "_value_bool" or "value_bool" => "_value_bool",
        "valuedouble" or "_value_double" or "value_double" => "_value_double",
        "valuenumeric" or "_value_numeric" or "value_numeric" => "_value_numeric",
        "valuedatetime" or "_value_datetime" or "value_datetime" => "_value_datetime",
        "valuebytes" or "_value_bytes" or "value_bytes" => "_value_bytes",
        "ownerid" or "_id_owner" or "owner_id" => "_id_owner",
        "whochangeid" or "_id_who_change" or "who_change_id" => "_id_who_change",
        "note" or "_note" => "_note",
        "key" or "_key" => "_key",
        "datebegin" or "_date_begin" or "date_begin" => "_date_begin",
        "datecomplete" or "_date_complete" or "date_complete" => "_date_complete",
        _ => "_id"
    };
}

