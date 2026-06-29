-- ============================================================================
-- REDB SQLite schema (DDL). Ported from redb.Postgres/sql/redbPostgre.sql.
-- Target: SQLite 3.44.0+ (Microsoft.Data.Sqlite).
-- Contains schema ONLY (tables + indexes + identity). The PL/pgSQL functions
-- and views from the PG file are NOT here — for SQLite they live in the native
-- C extension (Free) or in C# (Pro). See redb.SQLite/doc/C_EXTENSION.md.
-- ----------------------------------------------------------------------------
-- Type mapping: bigint->INTEGER, text/varchar->TEXT, boolean->INTEGER(0/1),
--   uuid->TEXT, bytea->BLOB, float->REAL, timestamptz->TEXT(ISO-8601),
--   NUMERIC(38,18)->TEXT (exact; configurable to REAL for speed).
-- NOTE: PRAGMA foreign_keys=ON must be set per-connection (in C#); it is not
--   persistent. WAL is persistent and set once below.
-- ============================================================================

PRAGMA journal_mode = WAL;

-- ============================================================================
-- ID generation. SQLite has no sequences; we use a native AUTOINCREMENT table.
-- The high-water mark lives in sqlite_sequence._global_identity and is advanced
-- monotonically by every id consumer, so ids are globally unique and never reused:
--   * C# keygen (hi-lo block):  UPDATE sqlite_sequence SET seq = seq + :n
--                               WHERE name='_global_identity' RETURNING seq;  -- ids=[seq-n+1 .. seq]
--   * C ext nextId (single):    same with +1.
--   * soft-delete (single, in a multi-statement dialect script): INSERT INTO
--     _global_identity DEFAULT VALUES + last_insert_rowid() — last_insert_rowid()
--     is per-connection, so it is stable across statements regardless of other
--     writers (no counter re-read race).
-- (RETURNING requires SQLite 3.35+, AUTOINCREMENT is native; we target 3.44+.)
-- ============================================================================
CREATE TABLE IF NOT EXISTS _global_identity(
    _id INTEGER PRIMARY KEY AUTOINCREMENT
);
-- Reserve ids 1..1000 for system/seed rows; runtime ids start at 1001.
-- Forcing one explicit insert materializes the sqlite_sequence row at seq=1000,
-- then we clear the table (the sequence persists and only ever increases).
INSERT INTO _global_identity(_id) VALUES (1000000);
DELETE FROM _global_identity WHERE _id = 1000000;

-- ============================================================================
-- Core tables. `_id INTEGER PRIMARY KEY` aliases the SQLite rowid; the app
-- supplies explicit ids from SqliteKeyGenerator (no AUTOINCREMENT).
-- ============================================================================

CREATE TABLE _types(
    _id      INTEGER NOT NULL PRIMARY KEY,
    _name    TEXT    NOT NULL UNIQUE,
    _db_type TEXT    NULL,
    _type    TEXT    NULL
);

CREATE TABLE _links(
    _id   INTEGER NOT NULL PRIMARY KEY,
    _id_1 INTEGER NOT NULL,
    _id_2 INTEGER NOT NULL,
    CONSTRAINT IX__links UNIQUE (_id_1, _id_2),
    CONSTRAINT CK__links CHECK (_id_1 <> _id_2)
);

CREATE TABLE _lists(
    _id    INTEGER NOT NULL PRIMARY KEY,
    _name  TEXT    NOT NULL,
    _alias TEXT    NULL,
    CONSTRAINT IX__lists_name UNIQUE (_name)
);

CREATE TABLE _roles(
    _id               INTEGER NOT NULL PRIMARY KEY,
    _name             TEXT    NOT NULL,
    -- forward FK to _objects (created later); SQLite resolves FKs at DML time
    _id_configuration INTEGER NULL REFERENCES _objects(_id) ON DELETE SET NULL,
    CONSTRAINT IX__roles UNIQUE (_name)
);

CREATE TABLE _users(
    _id               INTEGER NOT NULL PRIMARY KEY,
    _login            TEXT    NOT NULL UNIQUE,
    _password         TEXT    NOT NULL,
    _name             TEXT    NOT NULL,
    _phone            TEXT    NULL,
    _email            TEXT    NULL,
    _date_register    REAL    NOT NULL DEFAULT (julianday('now')),  -- UTC Julian day (REAL); native datetime()/strftime()/julianday() consume directly
    _date_dismiss     REAL    NULL,
    _enabled          INTEGER NOT NULL DEFAULT 1,
    _key              INTEGER NULL,
    _code_int         INTEGER NULL,
    _code_string      TEXT    NULL,
    _code_guid        TEXT    NULL,
    _note             TEXT    NULL,
    _hash             TEXT    NULL,
    _id_configuration INTEGER NULL REFERENCES _objects(_id) ON DELETE SET NULL
);

CREATE TABLE _users_roles(
    _id      INTEGER NOT NULL PRIMARY KEY,
    _id_role INTEGER NOT NULL,
    _id_user INTEGER NOT NULL,
    CONSTRAINT IX__users_roles UNIQUE (_id_role, _id_user),
    CONSTRAINT FK__users_roles__roles FOREIGN KEY (_id_role) REFERENCES _roles (_id) ON DELETE CASCADE,
    CONSTRAINT FK__users_roles__users FOREIGN KEY (_id_user) REFERENCES _users (_id) ON DELETE CASCADE
);

CREATE TABLE _schemes(
    _id             INTEGER NOT NULL PRIMARY KEY,
    _id_parent      INTEGER NULL,
    _name           TEXT    NOT NULL,
    _alias          TEXT    NULL,
    _name_space     TEXT    NULL,
    _structure_hash TEXT    NULL,
    _type           INTEGER NOT NULL DEFAULT -9223372036854775675, -- Class (default), Array, Dictionary, JsonDocument, XDocument
    CONSTRAINT IX__schemes UNIQUE (_name),
    CONSTRAINT FK__schemes__schemes FOREIGN KEY (_id_parent) REFERENCES _schemes (_id),
    CONSTRAINT FK__schemes__types   FOREIGN KEY (_type)      REFERENCES _types (_id)
);

CREATE TABLE _structures(
    _id              INTEGER NOT NULL PRIMARY KEY,
    _id_parent       INTEGER NULL,
    _id_scheme       INTEGER NOT NULL,
    _id_override     INTEGER NULL,
    _id_type         INTEGER NOT NULL,
    _id_list         INTEGER NULL,
    _name            TEXT    NOT NULL,
    _alias           TEXT    NULL,
    _order           INTEGER NULL,
    _readonly        INTEGER NULL,
    _allow_not_null  INTEGER NULL,
    _collection_type INTEGER NULL,  -- Array/Dictionary type ID or NULL for non-collections
    _key_type        INTEGER NULL,  -- Key type for Dictionary fields
    _is_compress     INTEGER NULL,
    _store_null      INTEGER NULL,
    _default_value   BLOB    NULL,
    _default_editor  TEXT    NULL,
    CONSTRAINT IX__structures UNIQUE (_id_scheme, _name, _id_parent),
    CONSTRAINT FK__structures__structures      FOREIGN KEY (_id_parent)       REFERENCES _structures (_id) ON DELETE CASCADE,
    CONSTRAINT FK__structures__schemes         FOREIGN KEY (_id_scheme)       REFERENCES _schemes (_id),
    CONSTRAINT FK__structures__types           FOREIGN KEY (_id_type)         REFERENCES _types (_id),
    CONSTRAINT FK__structures__lists           FOREIGN KEY (_id_list)         REFERENCES _lists (_id),
    CONSTRAINT FK__structures__collection_type FOREIGN KEY (_collection_type) REFERENCES _types (_id),
    CONSTRAINT FK__structures__key_type        FOREIGN KEY (_key_type)        REFERENCES _types (_id)
);

CREATE TABLE _dependencies(
    _id          INTEGER NOT NULL PRIMARY KEY,
    _id_scheme_1 INTEGER NULL,
    _id_scheme_2 INTEGER NOT NULL,
    CONSTRAINT IX__dependencies UNIQUE (_id_scheme_1, _id_scheme_2),
    CONSTRAINT FK__dependencies__schemes_1 FOREIGN KEY (_id_scheme_1) REFERENCES _schemes (_id),
    CONSTRAINT FK__dependencies__schemes_2 FOREIGN KEY (_id_scheme_2) REFERENCES _schemes (_id) ON DELETE CASCADE
);

CREATE TABLE _objects(
    _id             INTEGER NOT NULL PRIMARY KEY,
    _id_parent      INTEGER NULL,
    _id_scheme      INTEGER NOT NULL,
    _id_owner       INTEGER NOT NULL,
    _id_who_change  INTEGER NOT NULL,
    _date_create    REAL    NOT NULL DEFAULT (julianday('now')),  -- UTC Julian day (REAL)
    _date_modify    REAL    NOT NULL DEFAULT (julianday('now')),  -- UTC Julian day (REAL)
    _date_begin     REAL    NULL,
    _date_complete  REAL    NULL,
    _key            INTEGER NULL,
    _name           TEXT    NULL,
    _note           TEXT    NULL,
    _hash           TEXT    NULL,
    -- Value columns for RedbPrimitive<T> (Props = primitive value stored directly)
    _value_long     INTEGER NULL,
    _value_string   TEXT    NULL,
    _value_guid     TEXT    NULL,
    _value_bool     INTEGER NULL,
    _value_double   REAL    NULL,
    _value_numeric  REAL    NULL,  -- NUMERIC(38,18): REAL default (numeric ops/JSON); TEXT = exact (config)
    _value_datetime REAL    NULL,  -- UTC Julian day (REAL)
    _value_bytes    BLOB    NULL,
    CONSTRAINT FK__objects__objects FOREIGN KEY (_id_parent)     REFERENCES _objects (_id) ON DELETE CASCADE,
    CONSTRAINT FK__objects__schemes FOREIGN KEY (_id_scheme)     REFERENCES _schemes (_id) ON DELETE CASCADE,
    CONSTRAINT FK__objects__users1  FOREIGN KEY (_id_owner)      REFERENCES _users (_id),
    CONSTRAINT FK__objects__users2  FOREIGN KEY (_id_who_change) REFERENCES _users (_id)
);

CREATE TABLE _list_items(
    _id        INTEGER NOT NULL PRIMARY KEY,
    _id_list   INTEGER NOT NULL,
    _value     TEXT    NULL,
    _alias     TEXT    NULL,
    _id_object INTEGER NULL,
    CONSTRAINT FK__list_items__id_list FOREIGN KEY (_id_list)   REFERENCES _lists (_id)   ON DELETE CASCADE,
    CONSTRAINT FK__list_items__objects FOREIGN KEY (_id_object) REFERENCES _objects (_id) ON DELETE SET NULL
);

-- PG used `NULLS NOT DISTINCT` (one NULL per list). SQLite treats NULLs as
-- distinct in UNIQUE, so we emulate via an expression index with a sentinel.
-- Sentinel char(1) (SOH) is not expected as a real dictionary value.
CREATE UNIQUE INDEX IX__list_items_unique ON _list_items (_id_list, IFNULL(_value, char(1)));

CREATE TABLE _values(
    _id              INTEGER NOT NULL PRIMARY KEY,
    _id_structure    INTEGER NOT NULL,
    _id_object       INTEGER NOT NULL,
    _String          TEXT    NULL,
    _Long            INTEGER NULL,
    _Guid            TEXT    NULL,
    _Double          REAL    NULL,
    _DateTimeOffset  REAL    NULL,  -- DateTime/DateTimeOffset/DateOnly as UTC Julian day (REAL)
    _Boolean         INTEGER NULL,
    _ByteArray       BLOB    NULL,
    _Numeric         REAL    NULL,  -- NUMERIC(38,18): REAL default (numeric ops/JSON); TEXT = exact (config)
    _ListItem        INTEGER NULL,
    _Object          INTEGER NULL,
    -- Relational storage of collections (arrays, dictionaries)
    _array_parent_id INTEGER NULL, -- parent element (nested structures)
    _array_index     TEXT    NULL, -- '0','1',... for arrays; string key for dictionaries
    CONSTRAINT FK__values__objects      FOREIGN KEY (_id_object)       REFERENCES _objects (_id)    ON DELETE CASCADE,
    CONSTRAINT FK__values__structures   FOREIGN KEY (_id_structure)    REFERENCES _structures (_id) ON DELETE CASCADE,
    CONSTRAINT FK__values__array_parent FOREIGN KEY (_array_parent_id) REFERENCES _values (_id)      ON DELETE CASCADE,
    CONSTRAINT FK__values__list_items   FOREIGN KEY (_ListItem)        REFERENCES _list_items (_id),
    CONSTRAINT FK__values__objects_ref  FOREIGN KEY (_Object)          REFERENCES _objects (_id)
);

CREATE TABLE _permissions(
    _id      INTEGER NOT NULL PRIMARY KEY,
    _id_role INTEGER NULL,
    _id_user INTEGER NULL,
    _id_ref  INTEGER NOT NULL,
    _select  INTEGER NULL,
    _insert  INTEGER NULL,
    _update  INTEGER NULL,
    _delete  INTEGER NULL,
    CONSTRAINT CK__permissions_users_roles CHECK (_id_role IS NOT NULL AND _id_user IS NULL OR _id_role IS NULL AND _id_user IS NOT NULL),
    CONSTRAINT IX__permissions UNIQUE (_id_role, _id_user, _id_ref, _select, _insert, _update, _delete),
    CONSTRAINT FK__permissions__roles FOREIGN KEY (_id_role) REFERENCES _roles (_id) ON DELETE CASCADE,
    CONSTRAINT FK__permissions__users FOREIGN KEY (_id_user) REFERENCES _users (_id) ON DELETE CASCADE
);

CREATE TABLE _functions(
    _id        INTEGER NOT NULL PRIMARY KEY,
    _id_scheme INTEGER NOT NULL,
    _language  TEXT    NOT NULL,
    _name      TEXT    NOT NULL,
    _body      TEXT    NOT NULL,
    CONSTRAINT IX__functions_scheme_name UNIQUE (_id_scheme, _name),
    CONSTRAINT FK__functions__schemes FOREIGN KEY (_id_scheme) REFERENCES _schemes (_id)
);

-- ============================================================================
-- Metadata cache (ported 1:1 from redb_metadata_cache.sql). Denormalized
-- _structures ⋈ _types (+ scheme/collection/key type names) per scheme, to avoid
-- repeated JOINs. Maintained by SqliteDialect.Schemes_SyncMetadataCache (called
-- by SchemeSyncProviderBase after each scheme sync) and Warmup_AllMetadataCaches
-- (InitializeAsync). bytea->BLOB, boolean->INTEGER.
-- ============================================================================
CREATE TABLE _scheme_metadata_cache (
    _scheme_id           INTEGER NOT NULL,
    _structure_id        INTEGER NOT NULL,
    _parent_structure_id INTEGER,
    _id_override         INTEGER,
    _name                TEXT    NOT NULL,
    _alias               TEXT,
    _type_id             INTEGER NOT NULL,
    _list_id             INTEGER,
    type_name            TEXT    NOT NULL,
    db_type              TEXT    NOT NULL,
    type_semantic        TEXT    NOT NULL,
    _scheme_type         INTEGER,
    scheme_type_name     TEXT,
    _order               INTEGER,
    _collection_type     INTEGER,
    collection_type_name TEXT,
    _key_type            INTEGER,
    key_type_name        TEXT,
    _readonly            INTEGER,
    _allow_not_null      INTEGER,
    _is_compress         INTEGER,
    _store_null          INTEGER,
    _default_value       BLOB,
    _default_editor      TEXT
);

CREATE INDEX idx_metadata_cache_lookup      ON _scheme_metadata_cache (_scheme_id, _parent_structure_id, _order);
CREATE INDEX idx_metadata_cache_structure   ON _scheme_metadata_cache (_structure_id);
CREATE INDEX idx_metadata_cache_scheme      ON _scheme_metadata_cache (_scheme_id);
CREATE INDEX idx_metadata_cache_name        ON _scheme_metadata_cache (_scheme_id, _name);
CREATE INDEX idx_metadata_cache_collection  ON _scheme_metadata_cache (_scheme_id, _collection_type) WHERE _collection_type IS NOT NULL;
CREATE INDEX idx_metadata_cache_scheme_type ON _scheme_metadata_cache (_scheme_id, _scheme_type);
CREATE INDEX idx_metadata_cache_key_type    ON _scheme_metadata_cache (_scheme_id, _key_type) WHERE _key_type IS NOT NULL;

-- ============================================================================
-- Indexes. PG `WITH (deduplicate_items=True)` dropped. PG covering `INCLUDE(...)`
-- columns are appended to the index key (SQLite has no INCLUDE; trailing key
-- columns give the same covering-scan benefit). Partial `WHERE` and `DESC`
-- columns are supported as-is. The pg_trgm GIN index is dropped (no SQLite
-- equivalent; text pattern search to be revisited via FTS5).
-- ============================================================================

CREATE INDEX IF NOT EXISTS "IX__functions__schemes"  ON _functions  (_id_scheme);
CREATE INDEX IF NOT EXISTS "IX__permissions__roles"  ON _permissions (_id_role);
CREATE INDEX IF NOT EXISTS "IX__permissions__users"  ON _permissions (_id_user);
CREATE INDEX IF NOT EXISTS "IX__permissions__ref"    ON _permissions (_id_ref);
CREATE INDEX IF NOT EXISTS "IX__values__objects"     ON _values (_id_object);
CREATE INDEX IF NOT EXISTS "IX__values__structures"  ON _values (_id_structure);

CREATE INDEX IF NOT EXISTS "IX__values__array_parent_id"    ON _values (_array_parent_id);
CREATE INDEX IF NOT EXISTS "IX__values__array_parent_index" ON _values (_array_parent_id, _array_index);
CREATE INDEX IF NOT EXISTS "IX__values__array_key"          ON _values (_id_structure, _array_index) WHERE _array_index IS NOT NULL;

CREATE INDEX IF NOT EXISTS "IX__list_items__id_list" ON _list_items (_id_list);
CREATE INDEX IF NOT EXISTS "IX__list_items__objects" ON _list_items (_id_object);

CREATE INDEX IF NOT EXISTS "IX__objects__objects"     ON _objects (_id_parent);
CREATE INDEX IF NOT EXISTS "IX__objects__schemes"     ON _objects (_id_scheme);
CREATE INDEX IF NOT EXISTS "IX__objects__users1"      ON _objects (_id_owner);
CREATE INDEX IF NOT EXISTS "IX__objects__users2"      ON _objects (_id_who_change);
CREATE INDEX IF NOT EXISTS "IX__objects__date_create" ON _objects (_date_create);
CREATE INDEX IF NOT EXISTS "IX__objects__date_modify" ON _objects (_date_modify);
CREATE INDEX IF NOT EXISTS "IX__objects__name"        ON _objects (_name);
CREATE INDEX IF NOT EXISTS "IX__users__name"          ON _users (_name);
CREATE INDEX IF NOT EXISTS "IX__objects__hash"        ON _objects (_hash);

CREATE INDEX IF NOT EXISTS "IX__objects__value_long"     ON _objects (_value_long)     WHERE _value_long     IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_string"   ON _objects (_value_string)   WHERE _value_string   IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_guid"     ON _objects (_value_guid)     WHERE _value_guid     IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_datetime" ON _objects (_value_datetime) WHERE _value_datetime IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_numeric"  ON _objects (_value_numeric)  WHERE _value_numeric  IS NOT NULL;

CREATE INDEX IF NOT EXISTS "IX__dependencies__schemes_1" ON _dependencies (_id_scheme_1);
CREATE INDEX IF NOT EXISTS "IX__dependencies__schemes_2" ON _dependencies (_id_scheme_2);
CREATE INDEX IF NOT EXISTS "IX__structures__structures"  ON _structures (_id_parent);
CREATE INDEX IF NOT EXISTS "IX__structures__schemes"     ON _structures (_id_scheme);
CREATE INDEX IF NOT EXISTS "IX__structures__types"       ON _structures (_id_type);
CREATE INDEX IF NOT EXISTS "IX__structures__lists"       ON _structures (_id_list);
CREATE INDEX IF NOT EXISTS "IX__schemes__schemes"        ON _schemes (_id_parent);
CREATE INDEX IF NOT EXISTS "IX__schemes__structure_hash" ON _schemes (_structure_hash);
CREATE INDEX IF NOT EXISTS "IX__schemes__type"           ON _schemes (_type);
CREATE INDEX IF NOT EXISTS "IX__users_roles__roles"      ON _users_roles (_id_role);
CREATE INDEX IF NOT EXISTS "IX__users_roles__users"      ON _users_roles (_id_user);

CREATE INDEX IF NOT EXISTS "IX__users__id_configuration" ON _users (_id_configuration) WHERE _id_configuration IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__roles__id_configuration" ON _roles (_id_configuration) WHERE _id_configuration IS NOT NULL;

-- Partial index (only rows where _array_index IS NULL)
CREATE INDEX IF NOT EXISTS "ix__values__array_null_structure"
    ON _values (_id_structure, _id_object, _Long)
    WHERE _array_index IS NULL;

-- Nested fields by structure_id (INCLUDE cols folded into key)
CREATE INDEX IF NOT EXISTS "IX__values__structure_parent_batch"
    ON _values (_id_structure, _array_parent_id, _Long, _Double, _Boolean, _Guid)
    WHERE _array_parent_id IS NOT NULL;

-- Structure lookups (INCLUDE folded into key)
CREATE INDEX IF NOT EXISTS "IX__structures__name"
    ON _structures (_name, _id, _id_type, _collection_type, _id_scheme);
CREATE INDEX IF NOT EXISTS "IX__structures__id_lookup"
    ON _structures (_id, _id_type, _name, _collection_type, _id_scheme);
CREATE INDEX IF NOT EXISTS "IX__structures__not_collection"
    ON _structures (_id, _name, _id_scheme) WHERE _collection_type IS NULL;
CREATE INDEX IF NOT EXISTS "IX__structures__not_collection_enhanced"
    ON _structures (_id, _name, _id_scheme, _id_type) WHERE _collection_type IS NULL;
CREATE INDEX IF NOT EXISTS "IX__structures__collection"
    ON _structures (_id, _id_scheme, _collection_type) WHERE _collection_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__structures__key_type"
    ON _structures (_id, _id_scheme, _key_type) WHERE _key_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__structures__collection_type"
    ON _structures (_collection_type) WHERE _collection_type IS NOT NULL;

-- Covering value lookups (INCLUDE folded into key)
CREATE INDEX IF NOT EXISTS "IX__values__object_structure_lookup"
    ON _values (_id_object, _id_structure, _array_index, _Long, _Double, _DateTimeOffset, _Boolean, _Guid, _Numeric, _ListItem, _Object);
CREATE INDEX IF NOT EXISTS "IX__values__object_array_null"
    ON _values (_id_object, _id_structure, _Long, _Double, _DateTimeOffset, _Boolean, _Guid, _Numeric, _ListItem, _Object)
    WHERE _array_index IS NULL;

-- Faceted search core
CREATE INDEX IF NOT EXISTS "IX__values__structure_object_lookup"
    ON _values (_id_structure, _id_object, _Long, _DateTimeOffset, _Boolean, _Double, _Guid, _Numeric, _ListItem, _Object);
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent"
    ON _objects (_id_scheme, _id_parent, _id);
CREATE INDEX IF NOT EXISTS "IX__values__object_structure_array_index"
    ON _values (_id_object, _id_structure, _array_index) WHERE _array_index IS NOT NULL;

-- Partial NOT NULL value indexes
CREATE INDEX IF NOT EXISTS "IX__values__String_not_null"
    ON _values (_id_structure, _id_object, _String) WHERE _String IS NOT NULL AND length(_String) < 2000;
CREATE INDEX IF NOT EXISTS "IX__values__Long_not_null"
    ON _values (_id_structure, _id_object, _Long) WHERE _Long IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__values__DateTimeOffset_not_null"
    ON _values (_id_structure, _id_object, _DateTimeOffset) WHERE _DateTimeOffset IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__values__Numeric_not_null"
    ON _values (_id_structure, _id_object, _Numeric) WHERE _Numeric IS NOT NULL;

-- Hierarchy / tree
CREATE INDEX IF NOT EXISTS "IX__objects__parent_scheme_id"
    ON _objects (_id_parent, _id_scheme, _id) WHERE _id_parent IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__root_objects"
    ON _objects (_id_scheme, _id) WHERE _id_parent IS NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_date_create"
    ON _objects (_id_scheme, _date_create DESC, _id);
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_date_modify"
    ON _objects (_id_scheme, _date_modify DESC, _id);
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_name"
    ON _objects (_id_scheme, _name, _id);

-- Ancestor/descendant (INCLUDE folded into key)
CREATE INDEX IF NOT EXISTS "IX__objects__parent_id_descendant_lookup"
    ON _objects (_id_parent, _id_scheme, _id, _id_owner, _date_create, _date_modify) WHERE _id_parent IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent_date_create"
    ON _objects (_id_scheme, _id_parent, _date_create DESC, _id, _id_owner, _date_modify);
CREATE INDEX IF NOT EXISTS "IX__objects__id_parent_scheme"
    ON _objects (_id, _id_parent, _id_scheme) WHERE _id_parent IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent_owner"
    ON _objects (_id_scheme, _id_parent, _id, _id_owner, _date_create, _date_modify, _name) WHERE _id_parent IS NOT NULL;

CREATE INDEX IF NOT EXISTS "IX__values__parent_structure"
    ON _values (_array_parent_id, _id_structure) WHERE _array_parent_id IS NOT NULL;

-- ============================================================================
-- SEED DATA (ported from redbPostgre.sql, lines 710-801, 1346-1364).
-- Required by the C# layer: _types are referenced by _schemes._type /
-- _structures._id_type; system _users (-1/0/1) are referenced by _objects'
-- owner/who_change FKs; the soft-delete scheme (-10); and default permissions.
-- Booleans -> 0/1, timestamptz -> ISO-8601 TEXT. (PG validation triggers on
-- _structures/_schemes are intentionally NOT ported — they are PL/pgSQL.)
-- ============================================================================

INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775709, 'Boolean', 'Boolean', 'boolean');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775708, 'DateTime', 'DateTimeOffset', 'DateTime');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775707, 'Double', 'Double', 'double');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775706, 'ListItem', 'ListItem', '_RListItem');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775705, 'Guid', 'Guid', 'Guid');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775673, 'DateTimeOffset', 'DateTimeOffset', 'DateTimeOffset');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775674, 'Numeric', 'Numeric', 'decimal');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775704, 'Long', 'Long', 'long');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775703, 'Object', 'Object', '_RObject');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775701, 'ByteArray', 'ByteArray', 'byte[]');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775700, 'String', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775699, 'Int', 'Long', 'int');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775698, 'Short', 'Long', 'short');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775697, 'Byte', 'Long', 'byte');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775696, 'Float', 'Double', 'float');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775695, 'Decimal', 'Numeric', 'decimal');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775694, 'Char', 'String', 'char');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775693, 'Url', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775692, 'Email', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775691, 'Phone', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775690, 'Json', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775689, 'Xml', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775688, 'Base64', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775687, 'Color', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775686, 'DateOnly', 'DateTime', 'DateOnly');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775685, 'TimeOnly', 'String', 'TimeOnly');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775684, 'TimeSpan', 'String', 'TimeSpan');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775683, 'Enum', 'String', 'Enum');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775682, 'EnumInt', 'Long', 'Enum');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775681, 'Latitude', 'Double', 'double');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775680, 'Longitude', 'Double', 'double');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775679, 'GeoPoint', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775678, 'FilePath', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775677, 'FileName', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775676, 'MimeType', 'String', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775675, 'Class', 'Guid', 'Object');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775668, 'Array', 'Guid', 'Array');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775667, 'Dictionary', 'Guid', 'Dictionary');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775666, 'JsonDocument', 'Guid', 'JsonDocument');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775665, 'XDocument', 'Guid', 'XDocument');

INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (-1, 'default', '', 'default', NULL, NULL, julianday('2023-12-26T01:14:34.410'), NULL, 1);
INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (0, 'sys', '', 'sys', NULL, NULL, julianday('2023-12-26T01:14:34.410'), NULL, 1);
INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (1, 'admin', '', 'admin', NULL, NULL, julianday('2023-12-26T01:14:34.410'), NULL, 1);

-- Soft-delete reserved scheme (objects marked for deletion get _id_scheme = -10)
INSERT INTO _schemes (_id, _name, _alias, _type)
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
ON CONFLICT (_id) DO NOTHING;

-- Default permissions for sys (0) and admin (1): all rights on all objects (_id_ref=0).
-- System rows use explicit ids in the reserved 1..1000 range, so they never collide
-- with runtime ids handed out from sqlite_sequence (which start at 1001).
INSERT INTO _permissions (_id, _id_user, _id_ref, _select, _insert, _update, _delete)
VALUES (1, 0, 0, 1, 1, 1, 1);
INSERT INTO _permissions (_id, _id_user, _id_ref, _select, _insert, _update, _delete)
VALUES (2, 1, 0, 1, 1, 1, 1);

-- ============================================================================
-- v_user_permissions — effective permissions per (object, user), with tree
-- inheritance + global (_id_ref=0) fallback. Port of the PG view (redbPostgre.sql).
-- PG `DISTINCT ON (object_id) ... ORDER BY object_id, level` -> SQLite
-- ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY level) = 1. Booleans are 0/1.
-- Columns match UserPermissionResult (permission_source_id, _id_user, ...).
-- NOTE: per-object permission checks scan the full view (O(N)); the optimized
-- per-object get_user_permissions_for_object() stays a Free C-function task (Python surface).
-- System user (id=0) is short-circuited to full rights in C# before any query here.
-- ============================================================================
CREATE VIEW IF NOT EXISTS v_user_permissions AS
WITH RECURSIVE permission_search AS (
    -- Step 1: each object searches for its own permission
    SELECT
        o._id AS object_id,
        o._id AS current_search_id,
        o._id_parent,
        0 AS level,
        EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) AS has_permission
    FROM _objects o
    UNION ALL
    -- Step 2: if NO permission - walk up to the parent
    SELECT
        ps.object_id,
        o._id AS current_search_id,
        o._id_parent,
        ps.level + 1,
        EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) AS has_permission
    FROM _objects o
    JOIN permission_search ps ON o._id = ps._id_parent
    WHERE ps.level < 50
      AND ps.has_permission = 0
),
-- closest permission per (object, grantee). NOTE: deviation from the literal PG view,
-- which partitions by object_id ONLY (DISTINCT ON object_id) and is therefore lossy when
-- one node carries permissions for several users/roles (e.g. the global _id_ref=0 row for
-- both sys and admin) — it would keep just one, shadowing the others. We partition by
-- (object_id, _id_user, _id_role) so every grantee keeps its own closest-level permission.
object_permissions AS (
    SELECT object_id, permission_id, _id_user, _id_role, _select, _insert, _update, _delete, level
    FROM (
        SELECT
            ps.object_id,
            p._id AS permission_id,
            p._id_user, p._id_role, p._select, p._insert, p._update, p._delete, ps.level,
            ROW_NUMBER() OVER (PARTITION BY ps.object_id, p._id_user, p._id_role ORDER BY ps.level) AS rn
        FROM permission_search ps
        JOIN _permissions p ON p._id_ref = ps.current_search_id
        WHERE ps.has_permission = 1
    ) WHERE rn = 1
),
-- global rights as virtual records with object_id = 0
global_permissions AS (
    SELECT
        0 AS object_id, p._id AS permission_id, p._id_user, p._id_role,
        p._select, p._insert, p._update, p._delete, 999 AS level
    FROM _permissions p WHERE p._id_ref = 0
),
all_permissions AS (
    SELECT * FROM object_permissions
    UNION ALL
    SELECT * FROM global_permissions
),
-- specific (object) beats global (object_id=0) per grantee: lower level wins.
final_permissions AS (
    SELECT object_id, permission_id, _id_user, _id_role, _select, _insert, _update, _delete, level
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY object_id, _id_user, _id_role ORDER BY level) AS rn
        FROM all_permissions
    ) WHERE rn = 1
)
SELECT
    fp.object_id,
    CASE WHEN fp._id_user IS NOT NULL THEN fp._id_user ELSE ur._id_user END AS user_id,
    fp.permission_id AS permission_source_id,
    CASE WHEN fp._id_user IS NOT NULL THEN 'user' ELSE 'role' END AS permission_type,
    fp._id_role,
    fp._id_user,
    fp._select AS can_select,
    fp._insert AS can_insert,
    fp._update AS can_update,
    fp._delete AS can_delete
FROM final_permissions fp
LEFT JOIN _users_roles ur ON ur._id_role = fp._id_role
WHERE fp._id_user IS NOT NULL OR ur._id_user IS NOT NULL;
