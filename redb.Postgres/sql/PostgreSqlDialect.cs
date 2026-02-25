using redb.Core.Query;

namespace redb.Postgres.Sql;

/// <summary>
/// PostgreSQL implementation of ISqlDialect.
/// Uses CASE WHEN for PVT (tested - 15 ms on 15K objects!).
/// </summary>
public class PostgreSqlDialect : ISqlDialect
{
    public string ProviderName => "PostgreSQL";
    
    public string FormatPagination(int? limit, int? offset)
    {
        var parts = new List<string>();
        if (limit.HasValue) parts.Add($"LIMIT {limit.Value}");
        if (offset.HasValue && offset.Value > 0) parts.Add($"OFFSET {offset.Value}");
        return string.Join(" ", parts);
    }
    
    public string WrapSubquery(string subquery, string alias)
        => $"({subquery}) AS {alias}";
    
    /// <summary>
    /// PVT via array_agg with GROUP BY.
    /// ⚡ Test showed: 0.293 ms vs 0.339 ms (subquery) - 15% faster!
    /// Works for ALL types including UUID.
    /// </summary>
    public string FormatPvtColumn(long structureId, string dbColumn, string alias)
        => $"(array_agg({dbColumn}) FILTER (WHERE _id_structure = {structureId}))[1] AS \"{alias}\"";
    
    public string FormatArrayContains(string column, string paramName)
        => $"{column} = ANY({paramName})";
    
    public string GetDbTypeName(string redbType) => redbType switch
    {
        "Long" => "bigint",
        "String" => "text",
        "Boolean" => "boolean",
        "DateTime" => "timestamptz",
        "DateTimeOffset" => "timestamptz",
        "Double" => "double precision",
        "Numeric" => "numeric",
        "Guid" => "uuid",
        "ByteArray" => "bytea",
        _ => "text"
    };
    
    public string FormatParameter(int index)
        => $"${index}";
    
    public string QuoteIdentifier(string name)
        => $"\"{name}\"";
    
    // ============================================================
    // === ROLES SQL ===
    // ============================================================
    
    public string Roles_SelectById() =>
        "SELECT _id AS Id, _name AS Name, _id_configuration AS IdConfiguration FROM _roles WHERE _id = $1";
    
    public string Roles_SelectByName() =>
        "SELECT _id AS Id, _name AS Name, _id_configuration AS IdConfiguration FROM _roles WHERE _name = $1";
    
    public string Roles_SelectAll() =>
        "SELECT _id AS Id, _name AS Name, _id_configuration AS IdConfiguration FROM _roles ORDER BY _name";
    
    public string Roles_Insert() =>
        "INSERT INTO _roles (_id, _name) VALUES ($1, $2)";
    
    public string Roles_UpdateName() =>
        "UPDATE _roles SET _name = $1 WHERE _id = $2";
    
    public string Roles_Delete() =>
        "DELETE FROM _roles WHERE _id = $1";
    
    public string Roles_ExistsByName() =>
        "SELECT _id FROM _roles WHERE _name = $1";
    
    public string Roles_ExistsByNameExcluding() =>
        "SELECT _id FROM _roles WHERE _name = $1 AND _id != $2";
    
    public string Roles_Count() =>
        "SELECT COUNT(*) FROM _roles";
    
    public string Roles_UpdateConfiguration() =>
        "UPDATE _roles SET _id_configuration = $1 WHERE _id = $2";
    
    public string Roles_SelectConfigurationId() =>
        "SELECT _id_configuration FROM _roles WHERE _id = $1";
    
    // ============================================================
    // === USERS_ROLES SQL ===
    // ============================================================
    
    public string UsersRoles_Insert() =>
        "INSERT INTO _users_roles (_id, _id_user, _id_role) VALUES ($1, $2, $3)";
    
    public string UsersRoles_Delete() =>
        "DELETE FROM _users_roles WHERE _id_user = $1 AND _id_role = $2";
    
    public string UsersRoles_DeleteByUser() =>
        "DELETE FROM _users_roles WHERE _id_user = $1";
    
    public string UsersRoles_DeleteByRole() =>
        "DELETE FROM _users_roles WHERE _id_role = $1";
    
    public string UsersRoles_Exists() =>
        "SELECT _id FROM _users_roles WHERE _id_user = $1 AND _id_role = $2";
    
    public string UsersRoles_SelectRolesByUser() =>
        """
        SELECT r._id AS Id, r._name AS Name, r._id_configuration AS IdConfiguration
        FROM _roles r
        INNER JOIN _users_roles ur ON ur._id_role = r._id
        WHERE ur._id_user = $1
        ORDER BY r._name
        """;
    
    public string UsersRoles_SelectUsersByRole() =>
        """
        SELECT u._id, u._login, u._name, u._password, u._phone, u._email,
               u._enabled, u._date_register, u._date_dismiss,
               u._key, u._code_int, u._code_string, u._code_guid, u._note, u._hash
        FROM _users u
        INNER JOIN _users_roles ur ON ur._id_user = u._id
        WHERE ur._id_role = $1
        ORDER BY u._name
        """;
    
    public string UsersRoles_CountByRole() =>
        "SELECT COUNT(*) FROM _users_roles WHERE _id_role = $1";
    
    // ============================================================
    // === USERS SQL ===
    // ============================================================
    
    public string Users_ExistsById() =>
        "SELECT _id FROM _users WHERE _id = $1";
    
    public string Users_SelectIdByLogin() =>
        "SELECT _id FROM _users WHERE _login = $1";
    
    // ============================================================
    // === PERMISSIONS SQL ===
    // ============================================================
    
    public string Permissions_DeleteByRole() =>
        "DELETE FROM _permissions WHERE _id_role = $1";
    
    /// <summary>
    /// CRITICAL: Uses PostgreSQL function get_user_permissions_for_object()!
    /// This function implements complex recursive permission logic.
    /// For MSSQL this must be replaced with CTE/JOIN query.
    /// </summary>
    public string Permissions_GetEffectiveForObject() =>
        "SELECT * FROM get_user_permissions_for_object($1, $2)";
    
    public string Permissions_SelectReadableObjectIds() =>
        """
        SELECT DISTINCT object_id FROM v_user_permissions 
        WHERE user_id = $1 AND can_select = true
        """;
    
    public string Permissions_Insert() =>
        """
        INSERT INTO _permissions (_id, _id_user, _id_role, _id_ref, _select, _insert, _update, _delete)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """;
    
    public string Permissions_Update() =>
        "UPDATE _permissions SET _select = $1, _insert = $2, _update = $3, _delete = $4 WHERE _id = $5";
    
    public string Permissions_Delete() =>
        "DELETE FROM _permissions WHERE _id = $1";
    
    public string Permissions_SelectById() =>
        """
        SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef, 
               _select AS "Select", _insert AS "Insert", _update AS "Update", _delete AS "Delete"
        FROM _permissions WHERE _id = $1
        """;
    
    public string Permissions_SelectByUser() =>
        """
        SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
               _select AS "Select", _insert AS "Insert", _update AS "Update", _delete AS "Delete"
        FROM _permissions WHERE _id_user = $1
        """;
    
    public string Permissions_SelectByRole() =>
        """
        SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
               _select AS "Select", _insert AS "Insert", _update AS "Update", _delete AS "Delete"
        FROM _permissions WHERE _id_role = $1
        """;
    
    public string Permissions_SelectByObject() =>
        """
        SELECT _id AS Id, _id_user AS IdUser, _id_role AS IdRole, _id_ref AS IdRef,
               _select AS "Select", _insert AS "Insert", _update AS "Update", _delete AS "Delete"
        FROM _permissions WHERE _id_ref = $1 OR _id_ref = 0
        """;
    
    public string Permissions_SelectByUserRoleObject() =>
        """
        SELECT _id AS Id, _select AS "Select", _insert AS "Insert", _update AS "Update", _delete AS "Delete"
        FROM _permissions 
        WHERE _id_user IS NOT DISTINCT FROM $1 AND _id_role IS NOT DISTINCT FROM $2 AND _id_ref = $3
        """;
    
    public string Permissions_DeleteByUserRoleObject() =>
        """
        DELETE FROM _permissions 
        WHERE _id_user IS NOT DISTINCT FROM $1 AND _id_role IS NOT DISTINCT FROM $2 AND _id_ref = $3
        """;
    
    public string Permissions_DeleteByUser() =>
        "DELETE FROM _permissions WHERE _id_user = $1";
    
    public string Permissions_Count() =>
        "SELECT COUNT(*) FROM _permissions";
    
    public string Permissions_CountByUser() =>
        "SELECT COUNT(*) FROM _permissions WHERE _id_user = $1";
    
    public string Permissions_CountByRole() =>
        "SELECT COUNT(*) FROM _permissions WHERE _id_role = $1";
    
    public string Permissions_SelectUserRoleIds() =>
        "SELECT _id_role AS IdRole FROM _users_roles WHERE _id_user = $1";
    
    // ============================================================
    // === USERS SQL (full) ===
    // ============================================================
    
    public string Users_SelectById() =>
        """
        SELECT _id AS Id, _login AS Login, _name AS Name, _password AS Password, 
               _phone AS Phone, _email AS Email, _enabled AS Enabled,
               _date_register AS DateRegister, _date_dismiss AS DateDismiss,
               _key AS Key, _code_int AS CodeInt, _code_string AS CodeString,
               _code_guid AS CodeGuid, _note AS Note, _hash AS Hash
        FROM _users WHERE _id = $1
        """;
    
    public string Users_SelectByLogin() =>
        """
        SELECT _id AS Id, _login AS Login, _name AS Name, _password AS Password, 
               _phone AS Phone, _email AS Email, _enabled AS Enabled,
               _date_register AS DateRegister, _date_dismiss AS DateDismiss,
               _key AS Key, _code_int AS CodeInt, _code_string AS CodeString,
               _code_guid AS CodeGuid, _note AS Note, _hash AS Hash
        FROM _users WHERE _login = $1
        """;
    
    public string Users_Insert() =>
        """
        INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _enabled,
                           _date_register, _date_dismiss, _key, _code_int, _code_string,
                           _code_guid, _note, _hash)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        """;
    
    public string Users_Update() =>
        """
        UPDATE _users SET _login = $1, _name = $2, _phone = $3, _email = $4, _enabled = $5,
                         _date_dismiss = $6, _key = $7, _code_int = $8, _code_string = $9,
                         _code_guid = $10, _note = $11, _hash = $12
        WHERE _id = $13
        """;
    
    public string Users_SoftDelete() =>
        "UPDATE _users SET _login = $1, _name = $2, _enabled = $3, _date_dismiss = $4 WHERE _id = $5";
    
    public string Users_UpdatePassword() =>
        "UPDATE _users SET _password = $1 WHERE _id = $2";
    
    public string Users_UpdateStatus() =>
        "UPDATE _users SET _enabled = $1, _date_dismiss = $2 WHERE _id = $3";
    
    public string Users_ExistsByLogin() =>
        "SELECT _id FROM _users WHERE _login = $1";
    
    public string Users_ExistsByLoginExcluding() =>
        "SELECT _id FROM _users WHERE _login = $1 AND _id != $2";
    
    public string Users_ExistsByEmail() =>
        "SELECT _id FROM _users WHERE _email = $1 LIMIT 1";
    
    public string Users_Count() =>
        "SELECT COUNT(*) FROM _users";
    
    public string Users_CountEnabled() =>
        "SELECT COUNT(*) FROM _users WHERE _enabled = true";
    
    public string Users_SelectConfigurationId() =>
        "SELECT _id_configuration FROM _users WHERE _id = $1";
    
    public string Users_UpdateConfiguration() =>
        "UPDATE _users SET _id_configuration = $1 WHERE _id = $2";
    
    public string Roles_SelectIdByName() =>
        "SELECT _id AS Id FROM _roles WHERE _name = $1";
    
    public string Roles_ExistsById() =>
        "SELECT _id FROM _roles WHERE _id = $1";
    
    // ============================================================
    // === SCHEMES SQL ===
    // ============================================================
    
    public string Schemes_SelectByName() =>
        "SELECT _id, _name, _alias, _name_space, _structure_hash, _type FROM _schemes WHERE _name = $1";
    
    public string Schemes_SelectById() =>
        "SELECT _id, _name, _alias, _name_space, _structure_hash, _type FROM _schemes WHERE _id = $1";
    
    public string Schemes_SelectAll() =>
        "SELECT _id, _name, _alias, _name_space, _structure_hash, _type FROM _schemes";
    
    public string Schemes_Insert() =>
        "INSERT INTO _schemes (_id, _name, _alias, _type) VALUES ($1, $2, $3, $4)";
    
    public string Schemes_UpdateHash() =>
        "UPDATE _schemes SET _structure_hash = $1 WHERE _id = $2";
    
    public string Schemes_UpdateName() =>
        "UPDATE _schemes SET _name = $1 WHERE _id = $2";
    
    public string Schemes_SelectHashById() =>
        "SELECT _structure_hash FROM _schemes WHERE _id = $1";
    
    public string Schemes_ExistsByName() =>
        "SELECT _id FROM _schemes WHERE _name = $1 LIMIT 1";
    
    public string Schemes_SelectObjectByName() =>
        "SELECT _id, _name, _alias, _name_space, _structure_hash, _type FROM _schemes WHERE _name = $1 AND _type = $2";
    
    public string Schemes_InsertObject() =>
        "INSERT INTO _schemes (_id, _name, _type) VALUES ($1, $2, $3)";
    
    // ============================================================
    // === STRUCTURES SQL ===
    // ============================================================
    
    public string Structures_SelectByScheme() =>
        """
        SELECT _id, _id_parent, _id_scheme, _id_override, _id_type, _id_list,
               _name, _alias, _order, _readonly, _allow_not_null, 
               _collection_type, _key_type, _is_compress, _store_null,
               _default_value, _default_editor
        FROM _structures WHERE _id_scheme = $1
        """;
    
    public string Structures_SelectBySchemeShort() =>
        "SELECT _id, _id_parent, _id_scheme, _id_type, _name, _order FROM _structures WHERE _id_scheme = $1";
    
    public string Structures_SelectBySchemeCacheable() =>
        """
        SELECT _id, _id_parent, _id_scheme, _id_type, _name, _alias, _order,
               _readonly, _allow_not_null, _collection_type, _key_type
        FROM _structures WHERE _id_scheme = $1
        """;
    
    public string Structures_Insert() =>
        """
        INSERT INTO _structures (_id, _id_scheme, _id_parent, _name, _alias, _id_type, _allow_not_null, _collection_type, _key_type, _order)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """;
    
    public string Structures_UpdateType() =>
        "UPDATE _structures SET _id_type = $1 WHERE _id = $2";
    
    public string Structures_UpdateCollectionType() =>
        "UPDATE _structures SET _collection_type = $1 WHERE _id = $2";
    
    public string Structures_UpdateKeyType() =>
        "UPDATE _structures SET _key_type = $1 WHERE _id = $2";
    
    public string Structures_UpdateAlias() =>
        "UPDATE _structures SET _alias = $1 WHERE _id = $2";
    
    public string Structures_UpdateAllowNotNull() =>
        "UPDATE _structures SET _allow_not_null = $1 WHERE _id = $2";
    
    public string Structures_DeleteByIds(IEnumerable<long> ids) =>
        $"DELETE FROM _structures WHERE _id IN ({string.Join(",", ids)})";
    
    // ============================================================
    // === TYPES SQL ===
    // ============================================================
    
    public string Types_SelectByName() =>
        "SELECT _id, _name FROM _types WHERE _name = $1";
    
    public string Types_SelectAll() =>
        "SELECT _id AS Id, _name AS Name, _db_type AS DbType, _type AS Type1 FROM _types";
    
    // ============================================================
    // === SCHEME FUNCTIONS (PostgreSQL-specific) ===
    // ============================================================
    
    public string Schemes_SyncMetadataCache() =>
        "SELECT sync_metadata_cache_for_scheme($1)";
    
    public string Schemes_MigrateStructureType() =>
        "SELECT * FROM migrate_structure_type($1, $2, $3, $4)";
    
    public string Schemes_GetStructureTree() =>
        "SELECT get_scheme_structure_tree($1)";
    
    // ============================================================
    // === TREE SQL ===
    // ============================================================
    
    public string Tree_GetObjectJson() =>
        "SELECT get_object_json($1, $2)";
    
    public string Tree_SelectChildrenJson() =>
        """
        SELECT json_data 
        FROM (
            SELECT get_object_json(o._id, 1) as json_data
            FROM _objects o
            WHERE o._id_parent = $1
              AND o._id_scheme = $2
            ORDER BY o._name, o._id
        ) subquery
        WHERE json_data IS NOT NULL
        """;
    
    public string Tree_SelectPolymorphicChildren() =>
        """
        SELECT o._id as ObjectId, o._id_scheme as SchemeId, get_object_json(o._id, 1)::text as JsonData 
        FROM _objects o
        WHERE o._id_parent = $1
        ORDER BY o._name, o._id
        """;
    
    public string Tree_SelectSchemeAndJson() =>
        "SELECT _id_scheme as SchemeId, get_object_json(_id, 1)::text as JsonData FROM _objects WHERE _id = $1";
    
    public string Tree_SelectChildrenBySchemeBase() =>
        """
        SELECT _id as Id, _id_parent as IdParent, _id_scheme as IdScheme, _name as Name, 
               _id_owner as IdOwner, _id_who_change as IdWhoChange,
               _date_create as DateCreate, _date_modify as DateModify, 
               _date_begin as DateBegin, _date_complete as DateComplete,
               _key as Key, _value_long as ValueLong, _value_string as ValueString, 
               _value_guid as ValueGuid, _value_bool as ValueBool, _value_double as ValueDouble, 
               _value_numeric as ValueNumeric, _value_datetime as ValueDatetime, 
               _value_bytes as ValueBytes, _note as Note, _hash as Hash
        FROM _objects 
        WHERE _id_parent = $1 AND _id_scheme = $2
        ORDER BY _name, _id
        """;
    
    public string Tree_SelectChildrenBase() =>
        """
        SELECT _id as Id, _id_parent as IdParent, _id_scheme as IdScheme, _name as Name, 
               _id_owner as IdOwner, _id_who_change as IdWhoChange,
               _date_create as DateCreate, _date_modify as DateModify, 
               _date_begin as DateBegin, _date_complete as DateComplete,
               _key as Key, _value_long as ValueLong, _value_string as ValueString, 
               _value_guid as ValueGuid, _value_bool as ValueBool, _value_double as ValueDouble, 
               _value_numeric as ValueNumeric, _value_datetime as ValueDatetime, 
               _value_bytes as ValueBytes, _note as Note, _hash as Hash
        FROM _objects 
        WHERE _id_parent = $1
        ORDER BY _name, _id
        """;
    
    public string Tree_ObjectExists() =>
        "SELECT _id FROM _objects WHERE _id = $1 LIMIT 1";
    
    public string Tree_SelectParentId() =>
        "SELECT _id_parent FROM _objects WHERE _id = $1";
    
    public string Tree_UpdateParent() =>
        "UPDATE _objects SET _id_parent = $1, _date_modify = $2, _id_who_change = $3 WHERE _id = $4";
    
    public string Tree_DeleteValuesByObjectIds() =>
        "DELETE FROM _values WHERE _id_object = ANY($1)";
    
    public string Tree_DeleteObjectsByIds() =>
        "DELETE FROM _objects WHERE _id = ANY($1)";
    
    // ============================================================
    // === OBJECT STORAGE SQL ===
    // ============================================================
    
    public string ObjectStorage_SelectObjectById() =>
        """
        SELECT _id as Id, _id_scheme as IdScheme, _hash as Hash, _name as Name, 
               _id_parent as IdParent, _id_owner as IdOwner, _id_who_change as IdWhoChange,
               _date_create as DateCreate, _date_modify as DateModify, 
               _date_begin as DateBegin, _date_complete as DateComplete,
               _key as Key, _value_long as ValueLong, _value_string as ValueString, 
               _value_guid as ValueGuid, _value_bool as ValueBool, _value_double as ValueDouble, 
               _value_numeric as ValueNumeric, _value_datetime as ValueDatetime, 
               _value_bytes as ValueBytes, _note as Note
        FROM _objects WHERE _id = $1
        """;
    
    public string ObjectStorage_SelectIdHash() =>
        "SELECT _id as Id, _hash as Hash FROM _objects WHERE _id = $1";
    
    public string ObjectStorage_SelectIdHashScheme() =>
        "SELECT _id as Id, _hash as Hash, _id_scheme as IdScheme FROM _objects WHERE _id = $1";
    
    public string ObjectStorage_SelectObjectsByIds() =>
        """
        SELECT _id as Id, _id_scheme as IdScheme, _id_parent as IdParent, _id_owner as IdOwner,
               _id_who_change as IdWhoChange, _name as Name, _hash as Hash,
               _date_create as DateCreate, _date_modify as DateModify, _date_begin as DateBegin,
               _date_complete as DateComplete, _key as Key, _note as Note,
               _value_long as ValueLong, _value_string as ValueString, _value_guid as ValueGuid,
               _value_bool as ValueBool, _value_double as ValueDouble, _value_numeric as ValueNumeric,
               _value_datetime as ValueDatetime, _value_bytes as ValueBytes
        FROM _objects WHERE _id = ANY($1)
        """;
    
    public string ObjectStorage_DeleteById() =>
        "DELETE FROM _objects WHERE _id = $1";
    
    public string ObjectStorage_DeleteByIds() =>
        "DELETE FROM _objects WHERE _id = ANY($1)";
    
    public string ObjectStorage_InsertObject() => """
        INSERT INTO _objects (
            _id, _id_scheme, _name, _note, _date_create, _date_modify,
            _id_owner, _id_who_change, _id_parent, _hash,
            _value_string, _value_long, _value_guid, _value_bool,
            _value_double, _value_numeric, _value_datetime, _value_bytes,
            _key, _date_begin, _date_complete
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
        """;
    
    public string ObjectStorage_UpdateObject() => """
        UPDATE _objects SET 
            _name = $1, _note = $2, _date_modify = $3, _id_who_change = $4, _hash = $5,
            _value_string = $6, _value_long = $7, _value_guid = $8, _value_bool = $9,
            _value_double = $10, _value_numeric = $11, _value_datetime = $12, _value_bytes = $13,
            _key = $14, _date_begin = $15, _date_complete = $16
        WHERE _id = $17
        """;
    
    public string ObjectStorage_DeleteValuesByObjectId() =>
        "DELETE FROM _values WHERE _id_object = $1";
    
    public string ObjectStorage_GetObjectJson() =>
        "SELECT get_object_json($1, $2)::text";
    
    public string ObjectStorage_GetObjectsJsonBulk() =>
        """
        SELECT get_object_json(id, $2)::text 
        FROM unnest($1::bigint[]) AS id
        """;
    
    public string ObjectStorage_SelectStructuresWithMetadata() =>
        """
        SELECT s._id as Id, s._id_parent as IdParent, s._name as Name, 
               COALESCE(t._db_type, 'String') as DbType,
               s._collection_type as CollectionType, s._key_type as KeyType, 
               COALESCE(s._store_null, false) as StoreNull,
               COALESCE(t._type, 'string') as TypeSemantic
        FROM _structures s
        LEFT JOIN _types t ON s._id_type = t._id
        WHERE s._id_scheme = $1
        """;
    
    public string ObjectStorage_SelectValuesWithTypes() =>
        """
        SELECT v._id as Id, v._id_structure as IdStructure, v._id_object as IdObject,
               v._string as String, v._long as Long, v._guid as Guid, v._double as Double,
               v._datetimeoffset as DateTimeOffset, v._boolean as Boolean, v._bytearray as ByteArray,
               v._numeric as Numeric, v._listitem as ListItem, v._object as Object,
               v._array_parent_id as ArrayParentId, v._array_index as ArrayIndex,
               COALESCE(t._db_type, 'String') as DbType
        FROM _values v
        JOIN _structures s ON v._id_structure = s._id
        JOIN _types t ON s._id_type = t._id
        WHERE v._id_object = $1 AND v._id_structure = ANY($2)
        """;
    
    public string ObjectStorage_SelectStructureTypes() =>
        """
        SELECT s._id as StructureId, COALESCE(t._db_type, 'String') as DbType
        FROM _structures s
        JOIN _types t ON s._id_type = t._id
        WHERE s._id = ANY($1)
        """;
    
    public string ObjectStorage_SelectTypeById() =>
        "SELECT _id as Id, _name as Name, _db_type as DbType, _type as Type1 FROM _types WHERE _id = $1";
    
    public string ObjectStorage_CheckObjectExists() =>
        "SELECT _id FROM _objects WHERE _id = $1 LIMIT 1";
    
    public string ObjectStorage_SelectSchemeById() =>
        "SELECT _id as Id, _name as Name FROM _schemes WHERE _id = $1";
    
    public string ObjectStorage_SelectValuesForObjects() =>
        """
        SELECT _id as Id, _id_structure as IdStructure, _id_object as IdObject, 
               _string as String, _long as Long, _guid as Guid, _double as Double,
               _datetimeoffset as DateTimeOffset, _boolean as Boolean, _bytearray as ByteArray,
               _numeric as Numeric, _listitem as ListItem, _object as Object,
               _array_parent_id as ArrayParentId, _array_index as ArrayIndex
        FROM _values 
        WHERE _id_object = ANY($1)
        """;
    
    public string ObjectStorage_SelectSchemeIdsForObjects() =>
        "SELECT _id as ObjectId, _id_scheme as SchemeId FROM _objects WHERE _id = ANY($1)";
    
    public string ObjectStorage_SelectValueById() =>
        """
        SELECT _id as Id, _id_structure as IdStructure, _id_object as IdObject,
               _string as String, _long as Long, _guid as Guid, _double as Double,
               _datetimeoffset as DateTimeOffset, _boolean as Boolean, _bytearray as ByteArray,
               _numeric as Numeric, _listitem as ListItem, _object as Object,
               _array_parent_id as ArrayParentId, _array_index as ArrayIndex
        FROM _values WHERE _id = $1
        """;
    
    public string ObjectStorage_SelectAllTypes() =>
        "SELECT _id as Id, _name as Name, _db_type as DbType, _type as Type1 FROM _types";
    
    public string ObjectStorage_SelectExistingIds() =>
        "SELECT _id as Id FROM _objects WHERE _id = ANY($1)";
    
    public string ObjectStorage_SelectSchemesByIds() =>
        """
        SELECT _id as Id, _name as Name, _alias as Alias, _name_space as NameSpace, 
               _structure_hash as StructureHash, _type as Type 
        FROM _schemes WHERE _id = ANY($1)
        """;
    
    public string ObjectStorage_LockObjectsForUpdate() =>
        "SELECT 1 FROM _objects WHERE _id = ANY($1) FOR UPDATE";
    
    public string ObjectStorage_SelectSchemeIdByObjectId() =>
        "SELECT _id_scheme FROM _objects WHERE _id = $1";
    
    // ============================================================
    // === LIST SQL ===
    // ============================================================
    
    public string Lists_SelectById() =>
        "SELECT _id, _name FROM _lists WHERE _id = $1";
    
    public string Lists_SelectByName() =>
        "SELECT _id, _name FROM _lists WHERE _name = $1";
    
    public string Lists_SelectAll() =>
        "SELECT _id, _name FROM _lists ORDER BY _name";
    
    public string Lists_Insert() =>
        "INSERT INTO _lists (_id, _name, _alias) VALUES ($1, $2, $3)";
    
    public string Lists_Update() =>
        "UPDATE _lists SET _name = $1, _alias = $2 WHERE _id = $3";
    
    public string Lists_Delete() =>
        "DELETE FROM _lists WHERE _id = $1";
    
    public string Lists_IsUsedInStructures() =>
        "SELECT _id FROM _structures WHERE _id_list = $1 LIMIT 1";
    
    // ============================================================
    // === LIST ITEMS SQL ===
    // ============================================================
    
    public string ListItems_SelectById() =>
        "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items WHERE _id = $1";
    
    public string ListItems_SelectByListId() =>
        "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items WHERE _id_list = $1 ORDER BY _value";
    
    public string ListItems_SelectByListIdAndValue() =>
        "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items WHERE _id_list = $1 AND _value = $2";
    
    public string ListItems_Insert() =>
        "INSERT INTO _list_items (_id, _id_list, _value, _alias, _id_object) VALUES ($1, $2, $3, $4, $5)";
    
    public string ListItems_UpdateAliasAndObject() =>
        "UPDATE _list_items SET _alias = $1, _id_object = $2 WHERE _id = $3";
    
    public string ListItems_Update() =>
        "UPDATE _list_items SET _value = $1, _alias = $2, _id_object = $3 WHERE _id = $4";
    
    public string ListItems_Delete() =>
        "DELETE FROM _list_items WHERE _id = $1";
    
    public string ListItems_SelectByObjectId() =>
        "SELECT _id, _id_list, _value, _alias, _id_object FROM _list_items WHERE _id_object = $1";
    
    // ============================================================
    // === VALIDATION SQL ===
    // ============================================================
    
    public string Validation_SelectAllTypes() =>
        "SELECT _id, _name, _db_type, _type FROM _types";
    
    public string Validation_SelectSchemeByName() =>
        "SELECT _id, _id_parent, _name, _alias, _name_space, _structure_hash, _type FROM _schemes WHERE _name = $1";
    
    public string Validation_SelectStructuresBySchemeId() =>
        @"SELECT s._id, s._id_parent, s._id_scheme, s._id_override, s._id_type, s._id_list, 
                 s._name, s._alias, s._order, s._readonly, s._allow_not_null, 
                 s._collection_type, s._key_type, s._is_compress, s._store_null, 
                 s._default_value, s._default_editor
          FROM _structures s WHERE s._id_scheme = $1";
    
    // ============================================================
    // === LAZY LOADER SQL ===
    // ============================================================
    
    public string LazyLoader_SelectObjectBase() =>
        @"SELECT _id, _id_scheme, _hash, _name, _id_parent, _id_owner, _id_who_change,
                 _date_create, _date_modify, _date_begin, _date_complete,
                 _key, _value_long, _value_string, _value_guid, _value_bool,
                 _value_double, _value_numeric, _value_datetime, _value_bytes, _note
          FROM _objects WHERE _id = $1";
    
    public string LazyLoader_GetObjectJson() =>
        "SELECT get_object_json($1, $2)::text";
    
    public string LazyLoader_GetObjectJsonBatch() =>
        @"SELECT id as ""Id"", get_object_json(id, 10)::text as ""JsonData""
          FROM unnest($1::bigint[]) as id";
    
    public string LazyLoader_SelectObjectHash() =>
        "SELECT _hash FROM _objects WHERE _id = $1";
    
    // ============================================================
    // === QUERY PROVIDER SQL ===
    // ============================================================
    
    public string Query_SearchObjectsFunction() => "search_objects_with_facets";
    
    public string Query_SearchObjectsBaseFunction() => "search_objects_with_facets_base";
    
    public string Query_SearchObjectsProjectionByPathsFunction() => "search_objects_with_projection_by_paths";
    
    public string Query_SearchObjectsProjectionByIdsFunction() => "search_objects_with_projection_by_ids";
    
    public string Query_SearchTreeObjectsFunction() => "search_tree_objects_with_facets";
    
    public string Query_SearchTreeObjectsBaseFunction() => "search_tree_objects_with_facets_base";
    
    public string Query_CountTemplate() =>
        "SELECT ({0}($1, $2::jsonb, NULL, NULL, NULL, $3))->>'total_count'";
    
    public string Query_SearchTemplate() =>
        "SELECT {0}($1, $2::jsonb, $3, $4, $5::jsonb, $6) as result";
    
    public string Query_SearchWithDistinctTemplate() =>
        "SELECT {0}($1, $2::jsonb, $3, $4, $5::jsonb, $6, $7) as result";
    
    public string Query_SearchFullTemplate() =>
        "SELECT {0}($1, $2::jsonb, $3, $4, $5::jsonb, $6, $7, $8) as result";
    
    public string Query_JsonCast() => "::jsonb";
    
    public string Query_TextArrayCast() => "::text[]";
    
    public string Query_BigintArrayCast() => "::bigint[]";
    
    public string Query_ProjectionByPathsTemplate() =>
        "SELECT search_objects_with_projection_by_paths($1, $2::jsonb, $3::text[], $4, $5, $6::jsonb, $7) as result";
    
    public string Query_ProjectionByIdsTemplate(string structureIdsArray) =>
        $"SELECT search_objects_with_projection_by_ids($1, $2::jsonb, ARRAY[{structureIdsArray}]::bigint[], $3, $4, $5::jsonb, $6) as result";
    
    public string Query_CheckPermissionSql() =>
        "SELECT EXISTS(SELECT 1 FROM get_user_permissions_for_object($1, $2) WHERE can_select = true) as has_permission";
    
    public string Query_AggregateBatchPreviewSql() =>
        "SELECT aggregate_batch_preview($1, $2::jsonb, $3::jsonb) as sql_preview";
    
    public string Query_AggregateFieldSql() =>
        "SELECT aggregate_field($1, $2, $3, $4::jsonb) as result";
    
    public string Query_SqlPreviewTemplate() =>
        "SELECT {0}($1, $2::jsonb, $3, $4, $5::jsonb, $6, $7) as sql_preview";
    
    public string Query_AggregateBatchSql() =>
        "SELECT aggregate_batch($1, $2::jsonb, $3::jsonb)";
    
    /// <summary>
    /// Simple search for Delete operations - uses search_objects_with_facets with minimal params.
    /// Returns JSON with {objects: [{id:...},...], total_count:...}
    /// </summary>
    public string Query_SearchObjectsSimpleSql() =>
        "SELECT search_objects_with_facets($1, $2::jsonb, NULL, 0, NULL, 10, false, false)";
    
    public string Query_AggregateGroupedSql() =>
        "SELECT aggregate_grouped($1, $2::jsonb, $3::jsonb, $4::jsonb) as result";
    
    public string Query_AggregateArrayGroupedSql() =>
        "SELECT aggregate_array_grouped($1, $2, $3::jsonb, $4::jsonb, $5::jsonb) as result";
    
    public string Query_WindowSql() =>
        "SELECT query_with_window($1, $2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7, $8::jsonb) as result";
    
    public string Query_TreeCountNormalSql(string functionName) =>
        $"SELECT (result->>'total_count')::int FROM {functionName}($1, $2::jsonb, 1, 0, NULL::jsonb, $3) as result";
    
    public string Query_TreeSearchNormalSql(string functionName) =>
        $"SELECT result->>'objects' FROM {functionName}($1, $2::jsonb, $3, $4, $5::jsonb, $6) as result";
    
    /// <summary>
    /// Tree search with parent_ids array (8 params).
    /// </summary>
    public string Query_TreeSearchWithParentIdsSql(string functionName) =>
        $"SELECT result->>'objects' FROM {functionName}($1, $2, $3::jsonb, $4, $5, $6::jsonb, $7, $8) as result";
    
    /// <summary>
    /// HasAncestor with tree function (8 params).
    /// </summary>
    public string Query_HasAncestorTreeSql(string functionName) =>
        $@"WITH search_result AS (
            SELECT result->>'objects' as objects_json
            FROM {functionName}($1, $2, $3::jsonb, NULL, 0, NULL::jsonb, $4, 10) as result
        )
        SELECT jsonb_array_elements(objects_json::jsonb)->>'id' as ""Value""
        FROM search_result
        WHERE objects_json IS NOT NULL AND objects_json != 'null'";
    
    /// <summary>
    /// HasAncestor with normal function (6 params).
    /// </summary>
    public string Query_HasAncestorNormalSql(string functionName) =>
        $@"WITH search_result AS (
            SELECT result->>'objects' as objects_json
            FROM {functionName}($1, $2::jsonb, NULL, 0, NULL::jsonb, 10) as result
        )
        SELECT jsonb_array_elements(objects_json::jsonb)->>'id' as ""Value""
        FROM search_result
        WHERE objects_json IS NOT NULL AND objects_json != 'null'";
    
    /// <summary>
    /// HasDescendant with normal function (6 params).
    /// </summary>
    public string Query_HasDescendantSql(string functionName) =>
        $@"WITH search_result AS (
            SELECT result->>'objects' as objects_json
            FROM {functionName}($1, $2::jsonb, NULL, 0, NULL::jsonb, 10) as result
        )
        SELECT jsonb_array_elements(objects_json::jsonb)->>'id' as ""Value""
        FROM search_result
        WHERE objects_json IS NOT NULL AND objects_json != 'null'";
    
    /// <summary>
    /// PostgreSQL: WITH RECURSIVE for traversing ancestors.
    /// </summary>
    public string Query_GetParentIdsFromDescendantsSql(string idsString, int depthLimit) =>
        $@"WITH RECURSIVE ancestors AS (
            SELECT _id, _id_parent, 0 as level
            FROM _objects
            WHERE _id IN ({idsString})
            UNION ALL
            SELECT o._id, o._id_parent, ancestors.level + 1
            FROM _objects o
            INNER JOIN ancestors ON ancestors._id_parent = o._id
            WHERE ancestors.level < {depthLimit}
        )
        SELECT DISTINCT _id FROM ancestors WHERE _id_parent IS NULL OR level > 0";
    
    /// <summary>
    /// PostgreSQL: extract total_count from tree function result.
    /// </summary>
    public string Query_TreeCountWithParentIdsSql(string functionName) =>
        $"SELECT (result->>'total_count')::int as \"Value\" FROM {functionName}($1, $2, $3::jsonb, 1, 0, NULL::jsonb, $4, $5) as result";
    
    /// <summary>
    /// PostgreSQL: Get all IDs with their ancestors using recursive CTE.
    /// </summary>
    public string Query_GetIdsWithAncestorsSql(string idsString) =>
        $@"WITH RECURSIVE parent_chain AS (
            SELECT _id, _id_parent 
            FROM _objects 
            WHERE _id IN ({idsString})
            UNION
            SELECT o._id, o._id_parent 
            FROM _objects o
            INNER JOIN parent_chain pc ON pc._id_parent = o._id
        )
        SELECT DISTINCT _id FROM parent_chain";
    
    /// <summary>
    /// PostgreSQL: Load objects by IDs as JSON using get_object_json function.
    /// </summary>
    public string Query_LoadObjectsByIdsSql(string idsString, int maxDepth) =>
        $@"SELECT get_object_json(id, {maxDepth})::text as ""Value""
           FROM unnest(ARRAY[{idsString}]) as id
           ORDER BY id";
    
    public string Query_TreeSqlPreviewTemplate(string functionName) =>
        $"SELECT {functionName}($1, $2, $3::jsonb, $4, $5, $6::jsonb, $7, $8) as sql_preview";
    
    // ============================================================
    // === SQL PREVIEW FUNCTION NAMES ===
    // ============================================================
    
    public string Query_SqlPreviewFunction() => "get_search_sql_preview";
    
    public string Query_SqlPreviewBaseFunction() => "get_search_sql_preview_base";
    
    public string Query_TreeSqlPreviewFunction() => "get_search_tree_sql_preview";
    
    public string Query_TreeSqlPreviewBaseFunction() => "get_search_tree_sql_preview_base";
    
    // ============================================================
    // === SOFT DELETE ===
    // ============================================================
    
    /// <summary>
    /// Calls mark_for_deletion function to soft-delete objects.
    /// Params: $1=objectIds (bigint[]), $2=userId (bigint), $3=trashParentId (bigint, nullable)
    /// Returns: trash_id, marked_count
    /// </summary>
    public string SoftDelete_MarkForDeletion() => 
        "SELECT * FROM mark_for_deletion($1, $2, $3)";
    
    /// <summary>
    /// Calls purge_trash function to physically delete objects from trash.
    /// Params: $1=trashId (bigint), $2=batchSize (integer)
    /// Returns: deleted_count, remaining_count
    /// </summary>
    public string SoftDelete_PurgeTrash() => 
        "SELECT * FROM purge_trash($1, $2)";
    
    /// <summary>
    /// Gets deletion progress for a specific trash container.
    /// Params: $1=trashId (bigint)
    /// Returns: trash_id, total, deleted, status, started_at, owner_id
    /// </summary>
    public string SoftDelete_GetDeletionProgress() => """
        SELECT 
            _id AS trash_id,
            COALESCE(_value_long, 0) AS total,
            COALESCE(_key, 0) AS deleted,
            COALESCE(_value_string, 'pending') AS status,
            _date_create AS started_at,
            _id_owner AS owner_id
        FROM _objects 
        WHERE _id = $1 AND _id_scheme = -10
        """;
    
    /// <summary>
    /// Gets all active deletions for a user.
    /// Params: $1=userId (bigint)
    /// Returns: trash_id, total, deleted, status, started_at, owner_id
    /// </summary>
    public string SoftDelete_GetUserActiveDeletions() => """
        SELECT 
            _id AS trash_id,
            COALESCE(_value_long, 0) AS total,
            COALESCE(_key, 0) AS deleted,
            COALESCE(_value_string, 'pending') AS status,
            _date_create AS started_at,
            _id_owner AS owner_id
        FROM _objects 
        WHERE _id_owner = $1 
          AND _id_scheme = -10 
          AND _id_parent IS NULL
          AND _value_string IN ('pending', 'running')
        ORDER BY _date_create DESC
        """;
    
    /// <summary>
    /// Gets orphaned deletion tasks for recovery at startup.
    /// CLUSTER-SAFE: Only returns pending OR running with stale _date_modify.
    /// Params: $1=timeoutMinutes (int)
    /// Returns: trash_id, total, deleted, status, owner_id
    /// </summary>
    public string SoftDelete_GetOrphanedTasks() => """
        SELECT 
            _id AS trash_id,
            COALESCE(_value_long, 0) AS total,
            COALESCE(_key, 0) AS deleted,
            COALESCE(_value_string, 'pending') AS status,
            _id_owner AS owner_id
        FROM _objects 
        WHERE _id_scheme = -10 
          AND _id_parent IS NULL
          AND (
            _value_string = 'pending'
            OR (
              _value_string = 'running' 
              AND _date_modify < NOW() - INTERVAL '1 minute' * $1
            )
          )
        ORDER BY _date_create
        """;
    
    /// <summary>
    /// Atomically claim an orphaned task for processing.
    /// CLUSTER-SAFE: UPDATE with condition prevents race.
    /// Params: $1=trashId, $2=timeoutMinutes
    /// Returns: claimed count (1=success, 0=already taken)
    /// </summary>
    public string SoftDelete_ClaimOrphanedTask() => """
        UPDATE _objects 
        SET _value_string = 'running',
            _date_modify = NOW()
        WHERE _id = $1 
          AND _id_scheme = -10
          AND (
            _value_string = 'pending'
            OR (
              _value_string = 'running' 
              AND _date_modify < NOW() - INTERVAL '1 minute' * $2
            )
          )
        """;
    
    // ============================================================
    // === METADATA CACHE WARMUP ===
    // ============================================================
    
    public string Warmup_AllMetadataCaches() =>
        "SELECT scheme_id, structures_count, scheme_name FROM warmup_all_metadata_caches()";
}

