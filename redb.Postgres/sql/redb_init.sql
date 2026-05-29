-- ==========================================================
-- REDB: Combined schema initialization script (auto-generated)
-- DO NOT EDIT — this file is overwritten on every build.
-- ==========================================================

-- ===== redbPostgre.sql =====
-- DROP SEQUENCE IF EXISTS global_identity;
-- DROP VIEW IF EXISTS v_user_permissions;
-- DROP VIEW IF EXISTS v_objects_json;
-- DROP VIEW IF EXISTS v_schemes_definition;
-- DROP FUNCTION IF EXISTS get_scheme_definition;
-- DROP FUNCTION IF EXISTS get_object_json;
-- DROP FUNCTION IF EXISTS search_objects_with_facets;
-- DROP FUNCTION IF EXISTS get_facets;
-- DROP FUNCTION IF EXISTS build_base_facet_conditions;
-- DROP TABLE IF EXISTS _scheme_metadata_cache;
-- DROP TABLE IF EXISTS _dependencies;
-- DROP TABLE IF EXISTS _values;
-- DROP TABLE IF EXISTS _permissions;
-- DROP TABLE IF EXISTS _structures;
-- DROP TABLE IF EXISTS _users_roles;
-- DROP TABLE IF EXISTS _roles;
-- DROP TABLE IF EXISTS _list_items;
-- DROP TABLE IF EXISTS _lists;
-- DROP TABLE IF EXISTS _links;
-- DROP TABLE IF EXISTS _objects CASCADE;
-- DROP TABLE IF EXISTS _users;
-- DROP TABLE IF EXISTS _functions;
-- DROP TABLE IF EXISTS _schemes;
-- DROP TABLE IF EXISTS _types;
-- DROP FUNCTION IF EXISTS validate_structure_name;
-- DROP FUNCTION IF EXISTS validate_scheme_name;
-- DROP FUNCTION IF EXISTS get_user_permissions_for_object;
-- DROP FUNCTION IF EXISTS auto_create_node_permissions;


CREATE TABLE _types(
	_id bigint NOT NULL,
	_name text NOT NULL UNIQUE,
	_db_type text NULL,
	_type text NULL,
    CONSTRAINT PK__types PRIMARY KEY (_id)
);

CREATE TABLE _links(
	_id bigint NOT NULL,
	_id_1 bigint NOT NULL,
	_id_2 bigint NOT NULL,
    CONSTRAINT PK__links PRIMARY KEY (_id),
    CONSTRAINT IX__links UNIQUE (_id_1, _id_2),
    CONSTRAINT CK__links CHECK  (_id_1<>_id_2)
);

CREATE TABLE _lists(
	_id bigint NOT NULL,
	_name text NOT NULL,
	_alias text NULL,
    CONSTRAINT PK__lists PRIMARY KEY (_id),
    CONSTRAINT IX__lists_name UNIQUE (_name)
);


CREATE TABLE _roles(
	_id bigint NOT NULL,
	_name text NOT NULL,
	_id_configuration bigint NULL,
    CONSTRAINT PK__roles PRIMARY KEY (_id),
    CONSTRAINT IX__roles UNIQUE (_name)
);


CREATE TABLE _users(
	_id bigint NOT NULL,
	_login text NOT NULL UNIQUE,
	_password TEXT NOT NULL,
	_name text NOT NULL,
	_phone text NULL,
	_email text NULL,
	_date_register timestamptz DEFAULT now() NOT NULL,
	_date_dismiss timestamptz NULL,
	_enabled boolean DEFAULT true NOT NULL,
	_key bigint NULL,
	_code_int bigint NULL,
	_code_string text NULL,
	_code_guid uuid NULL,
	_note text NULL,
	_hash uuid NULL,
	_id_configuration bigint NULL,
    CONSTRAINT PK__users PRIMARY KEY (_id)
);


CREATE TABLE _users_roles(
	_id bigint NOT NULL,
	_id_role bigint NOT NULL,
    _id_user bigint NOT NULL,
    CONSTRAINT PK__users_roles PRIMARY KEY (_id),
    CONSTRAINT IX__users_roles UNIQUE (_id_role, _id_user),
    CONSTRAINT FK__users_roles__roles FOREIGN KEY (_id_role) REFERENCES _roles (_id) ON DELETE CASCADE,
    CONSTRAINT FK__users_roles__users FOREIGN KEY (_id_user) REFERENCES _users (_id) ON DELETE CASCADE
);


CREATE TABLE _schemes(
	_id bigint NOT NULL,
	_id_parent bigint NULL,
	_name text NOT NULL,
	_alias text NULL,
	_name_space text NULL,
	_structure_hash uuid NULL,
	_type bigint NOT NULL DEFAULT -9223372036854775675, -- Scheme type: Class (default), Array, Dictionary, JsonDocument, XDocument
    CONSTRAINT PK__schemes PRIMARY KEY (_id),
	CONSTRAINT IX__schemes UNIQUE (_name),
    CONSTRAINT FK__schemes__schemes FOREIGN KEY (_id_parent) REFERENCES _schemes (_id),
    CONSTRAINT FK__schemes__types FOREIGN KEY (_type) REFERENCES _types (_id)
);


CREATE TABLE _structures(
	_id bigint NOT NULL,
	_id_parent bigint NULL,
	_id_scheme bigint NOT NULL,
	_id_override bigint NULL,
	_id_type bigint NOT NULL,
	_id_list bigint NULL,
	_name text NOT NULL,
	_alias text NULL,
	_order bigint NULL,
	_readonly boolean NULL,
	_allow_not_null boolean NULL,
	_collection_type bigint NULL,  -- Array/Dictionary type ID or NULL for non-collections
	_key_type bigint NULL,         -- Key type for Dictionary fields
	_is_compress boolean NULL,
	_store_null boolean NULL,
	_default_value bytea NULL,
	_default_editor text NULL,
    CONSTRAINT PK__structure PRIMARY KEY (_id),
	CONSTRAINT IX__structures UNIQUE (_id_scheme,_name,_id_parent),
    CONSTRAINT FK__structures__structures FOREIGN KEY (_id_parent) REFERENCES _structures (_id) ON DELETE CASCADE,
    CONSTRAINT FK__structures__schemes FOREIGN KEY (_id_scheme) REFERENCES _schemes (_id),
    CONSTRAINT FK__structures__types FOREIGN KEY (_id_type) REFERENCES _types (_id),
    CONSTRAINT FK__structures__lists FOREIGN KEY (_id_list) REFERENCES _lists (_id),
    CONSTRAINT FK__structures__collection_type FOREIGN KEY (_collection_type) REFERENCES _types (_id),
    CONSTRAINT FK__structures__key_type FOREIGN KEY (_key_type) REFERENCES _types (_id)
);

CREATE TABLE _dependencies(
	_id bigint NOT NULL,
	_id_scheme_1 bigint,
	_id_scheme_2 bigint NOT NULL,
    CONSTRAINT PK__dependencies PRIMARY KEY (_id),
	CONSTRAINT IX__dependencies UNIQUE (_id_scheme_1,_id_scheme_2),
    CONSTRAINT FK__dependencies__schemes_1 FOREIGN KEY (_id_scheme_1) REFERENCES _schemes (_id), 
    CONSTRAINT FK__dependencies__schemes_2 FOREIGN KEY (_id_scheme_2) REFERENCES _schemes (_id)  ON DELETE CASCADE
);

CREATE TABLE _objects(
	_id bigint NOT NULL,
	_id_parent bigint NULL,
	_id_scheme bigint NOT NULL,
	_id_owner bigint NOT NULL,
	_id_who_change bigint NOT NULL,
	_date_create timestamptz DEFAULT now() NOT NULL,
	_date_modify timestamptz DEFAULT now() NOT NULL,
	_date_begin timestamptz NULL,
	_date_complete timestamptz NULL,
	_key bigint NULL,
	_name text NULL,
	_note text NULL,
	_hash uuid NULL,
	-- Value columns for RedbPrimitive<T> (Props = primitive value stored directly)
	-- Replaces old _code_int, _code_string, _code_guid, _bool columns
	_value_long bigint NULL,        -- was _code_int
	_value_string text NULL,        -- was _code_string (expanded to text!)
	_value_guid uuid NULL,          -- was _code_guid
	_value_bool boolean NULL,       -- was _bool
	_value_double float NULL,       -- NEW
	_value_numeric NUMERIC(38, 18) NULL,  -- NEW
	_value_datetime timestamptz NULL,     -- NEW
	_value_bytes bytea NULL,        -- NEW
    CONSTRAINT PK__objects PRIMARY KEY (_id),
    CONSTRAINT FK__objects__objects FOREIGN KEY (_id_parent) REFERENCES _objects (_id) ON DELETE CASCADE,
    CONSTRAINT FK__objects__schemes FOREIGN KEY (_id_scheme) REFERENCES _schemes (_id) ON DELETE CASCADE, 
    CONSTRAINT FK__objects__users1 FOREIGN KEY (_id_owner) REFERENCES _users (_id),
	CONSTRAINT FK__objects__users2 FOREIGN KEY (_id_who_change) REFERENCES _users (_id)    
);

CREATE TABLE _list_items(
	_id bigint NOT NULL,
	_id_list bigint NOT NULL,
	_value text NULL,
	_alias text NULL,
	_id_object bigint NULL,
    CONSTRAINT PK__list_items PRIMARY KEY (_id),
    CONSTRAINT FK__list_items__id_list FOREIGN KEY (_id_list) REFERENCES _lists (_id) ON DELETE CASCADE,
    CONSTRAINT FK__list_items__objects FOREIGN KEY (_id_object) REFERENCES _objects (_id) ON DELETE SET NULL
);

-- PostgreSQL 15+: UNIQUE с учетом NULL (NULL = NULL)
CREATE UNIQUE INDEX IX__list_items_unique ON _list_items (_id_list, _value) NULLS NOT DISTINCT;

CREATE TABLE _values(
	_id bigint NOT NULL,
	_id_structure bigint NOT NULL,
	_id_object bigint NOT NULL,
	_String text NULL,
	_Long bigint NULL,
	_Guid uuid NULL,
	_Double float NULL,
	_DateTimeOffset timestamptz NULL,
	_Boolean boolean NULL,
	_ByteArray bytea NULL,
	_Numeric NUMERIC(38, 18) NULL,
	_ListItem bigint NULL,
	_Object bigint NULL,
    -- Fields for relational storage of collections (arrays, dictionaries, JSON/XML documents)
    _array_parent_id bigint NULL, -- Reference to parent element (for nested structures)
    _array_index text NULL, -- Key/index of element: '0','1','2' for arrays, string key for dictionaries
    CONSTRAINT PK__values PRIMARY KEY (_id),
    CONSTRAINT FK__values__objects FOREIGN KEY (_id_object) REFERENCES _objects (_id) ON DELETE CASCADE,
    -- DESIGN: ON DELETE CASCADE here intentionally mirrors the MSSQL
    -- TR__structures__cascade_values INSTEAD OF DELETE trigger
    -- (redb.MSSql/sql/redbMSSQL.sql ~L717). Both backends silently remove
    -- every _values row when a _structures row is deleted. Destructive
    -- structure deletion at runtime is gated by
    -- RedbServiceConfiguration.DefaultStrictDeleteExtra (default true,
    -- see README "Schema lifecycle and multi-version deployments").
    CONSTRAINT FK__values__structures FOREIGN KEY (_id_structure) REFERENCES _structures (_id) ON DELETE CASCADE,
    CONSTRAINT FK__values__array_parent FOREIGN KEY (_array_parent_id) REFERENCES _values (_id) ON DELETE CASCADE,
    CONSTRAINT FK__values__list_items FOREIGN KEY (_ListItem) REFERENCES _list_items (_id),
    CONSTRAINT FK__values__objects_ref FOREIGN KEY (_Object) REFERENCES _objects (_id)
); 

-- Comments for extended _values table for relational array support
COMMENT ON TABLE _values IS 'Table for storing field values of objects. Supports relational arrays of all types (simple and Class fields) via _array_parent_id and _array_index';
COMMENT ON COLUMN _values._array_parent_id IS 'ID of parent element for array elements. NULL for regular (non-array) fields and root array elements';
COMMENT ON COLUMN _values._array_index IS 'Key/index of collection element (text). For arrays: ''0'',''1'',''2''. For dictionaries: string key. NULL for regular fields. For numeric sorting of arrays use ORDER BY _array_index::int';
COMMENT ON COLUMN _values._Numeric IS 'Exact decimal numbers for financial calculations. NUMERIC(38,18) provides lossless precision, unlike Double. Used for money, taxes, percentages where arithmetic accuracy is critical.';
COMMENT ON COLUMN _values._DateTimeOffset IS 'Date and time with timezone (timestamptz). PostgreSQL stores in UTC, converts on retrieval. Used for precise time moments: logs, transactions, events. Allows accurate reconstruction of event moment regardless of local timezone.';
COMMENT ON COLUMN _values._ListItem IS 'Reference to dictionary item (_list_items). FK without cascade delete - attempting to delete the linked list_item will cause integrity violation error. Used for ListItem type instead of storing ID in _Long.';
COMMENT ON COLUMN _values._Object IS 'Reference to object (_objects). FK without cascade delete - attempting to delete the linked object will cause integrity violation error. Used for Object type (redbObject) instead of storing ID in _Long.';

-- Comments for _schemes table
COMMENT ON COLUMN _schemes._type IS 'Scheme type ID (FK to _types): Class (default), Array, Dictionary, JsonDocument, XDocument. Determines how Props are stored and serialized.';

-- Comments for _objects._value_* columns (RedbPrimitive<T> support)
-- These columns replace old _code_int, _code_string, _code_guid, _bool
COMMENT ON COLUMN _objects._value_long IS 'Direct value for RedbPrimitive<long/int/short/byte>. Replaces _code_int.';
COMMENT ON COLUMN _objects._value_string IS 'Direct value for RedbPrimitive<string>. Replaces _code_string, expanded to text.';
COMMENT ON COLUMN _objects._value_guid IS 'Direct value for RedbPrimitive<Guid>. Replaces _code_guid.';
COMMENT ON COLUMN _objects._value_bool IS 'Direct value for RedbPrimitive<bool>. Replaces _bool.';
COMMENT ON COLUMN _objects._value_double IS 'Direct value for RedbPrimitive<double/float>. NEW column.';
COMMENT ON COLUMN _objects._value_numeric IS 'Direct value for RedbPrimitive<decimal>. NEW column.';
COMMENT ON COLUMN _objects._value_datetime IS 'Direct value for RedbPrimitive<DateTime/DateTimeOffset>. NEW column.';
COMMENT ON COLUMN _objects._value_bytes IS 'Direct value for RedbPrimitive<byte[]>. NEW column.';

-- Comments for _structures table
COMMENT ON COLUMN _structures._collection_type IS 'Collection type ID (FK to _types): Array or Dictionary. NULL for non-collection fields. Replaces old _is_array boolean.';
COMMENT ON COLUMN _structures._key_type IS 'Key type ID for Dictionary fields (FK to _types). NULL for non-dictionary fields.';

COMMENT ON INDEX IX__list_items_unique IS 
'Unique index on combination of dictionary + value with NULLS NOT DISTINCT. Prevents duplicate values in the same dictionary, including NULL (only one NULL per dictionary). Ensures strict integrity of dictionary data.';


CREATE TABLE _permissions(
	_id bigint NOT NULL,
	_id_role bigint NULL,
	_id_user bigint NULL,
	_id_ref bigint NOT NULL,
	_select boolean NULL,
	_insert boolean NULL,
	_update boolean NULL,
	_delete boolean NULL,
    CONSTRAINT PK__object_permissions PRIMARY KEY (_id),
    CONSTRAINT CK__permissions_users_roles CHECK  (_id_role IS NOT NULL AND _id_user IS NULL OR _id_role IS NULL AND _id_user IS NOT NULL),
    CONSTRAINT IX__permissions UNIQUE (_id_role, _id_user, _id_ref, _select, _insert, _update, _delete),
    CONSTRAINT FK__permissions__roles FOREIGN KEY (_id_role) REFERENCES _roles (_id) ON DELETE CASCADE,
    CONSTRAINT FK__permissions__users FOREIGN KEY (_id_user) REFERENCES _users (_id) ON DELETE CASCADE
);

CREATE TABLE _functions
(
    _id bigint NOT NULL,
    _id_scheme bigint NOT NULL,
	_language varchar(50) NOT NULL,
    _name text NOT NULL,
    _body text NOT NULL,
    CONSTRAINT PK__functions PRIMARY KEY (_id),
    CONSTRAINT IX__functions_scheme_name UNIQUE (_id_scheme, _name),
    CONSTRAINT FK__functions__schemes FOREIGN KEY (_id_scheme) REFERENCES _schemes (_id)
);


CREATE INDEX IF NOT EXISTS "IX__functions__schemes" ON _functions (_id_scheme) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__permissions__roles" ON _permissions (_id_role) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__permissions__users" ON _permissions (_id_user) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__permissions__ref" ON _permissions (_id_ref);
CREATE INDEX IF NOT EXISTS "IX__values__objects" ON _values (_id_object) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__values__structures" ON _values (_id_structure) WITH (deduplicate_items=True);

-- ============================================
-- REMOVED REDUNDANT INDEXES (migration_drop_redundant_indexes.sql)
-- Reason: Covered by composite index IX__values__structure_object_lookup
-- Facet search ALWAYS filters by (_id_structure, _id_object) BEFORE value
-- ============================================
-- CREATE INDEX IF NOT EXISTS "IX__values__String" ON _values (_String) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Long" ON _values (_Long) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Guid" ON _values (_Guid) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Double" ON _values (_Double) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__DateTimeOffset" ON _values (_DateTimeOffset) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Boolean" ON _values (_Boolean) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Numeric" ON _values (_Numeric) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__ListItem" ON _values (_ListItem) WITH (deduplicate_items=True);
-- CREATE INDEX IF NOT EXISTS "IX__values__Object" ON _values (_Object) WITH (deduplicate_items=True);

-- Indexes for relational arrays of all types
CREATE INDEX IF NOT EXISTS "IX__values__array_parent_id" ON _values (_array_parent_id) WITH (deduplicate_items=True);
-- REMOVED: _array_index is ALWAYS used together with _id_object + _id_structure
-- CREATE INDEX IF NOT EXISTS "IX__values__array_index" ON _values (_array_index) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__values__array_parent_index" ON _values (_array_parent_id, _array_index) WITH (deduplicate_items=True);
-- Index for Dictionary key lookups (text _array_index)
CREATE INDEX IF NOT EXISTS "IX__values__array_key" ON _values (_id_structure, _array_index) WHERE _array_index IS NOT NULL;

-- Unique indexes to ensure data integrity
-- For ROOT fields (not arrays, not nested): structure + object must be unique
CREATE UNIQUE INDEX IF NOT EXISTS "UIX__values__structure_object" 
ON _values (_id_structure, _id_object) 
WHERE _array_index IS NULL AND _array_parent_id IS NULL;

-- For BASE RECORDS OF NESTED ARRAYS: structure + object + parent must be unique
CREATE UNIQUE INDEX IF NOT EXISTS "UIX__values__structure_object_parent" 
ON _values (_id_structure, _id_object, _array_parent_id) 
WHERE _array_index IS NULL AND _array_parent_id IS NOT NULL;

-- For array elements: structure + object + parent + index must be unique
CREATE UNIQUE INDEX IF NOT EXISTS "UIX__values__structure_object_array_index" 
ON _values (_id_structure, _id_object, _array_parent_id, _array_index) 
WHERE _array_index IS NOT NULL;

-- Comments for created unique indexes
COMMENT ON INDEX "UIX__values__structure_object" IS 'Ensures uniqueness: one value per structure+object for ROOT fields (not nested arrays)';
COMMENT ON INDEX "UIX__values__structure_object_parent" IS 'Ensures uniqueness: one base record per structure+object+parent for nested arrays';
COMMENT ON INDEX "UIX__values__structure_object_array_index" IS 'Ensures uniqueness: one element per structure+object+parent+position for elements of all arrays (including nested)';
CREATE INDEX IF NOT EXISTS "IX__list_items__id_list" ON _list_items (_id_list) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__list_items__objects" ON _list_items (_id_object) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__objects" ON _objects (_id_parent) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__schemes" ON _objects (_id_scheme) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__users1" ON _objects (_id_owner) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__users2" ON _objects (_id_who_change) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__date_create" ON _objects (_date_create) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__date_modify" ON _objects (_date_modify) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__name" ON _objects (_name) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__users__name" ON _users (_name) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__objects__hash" ON _objects (_hash) WITH (deduplicate_items=True);
-- Indexes for RedbPrimitive<T> value columns (replaces old _code_* indexes)
CREATE INDEX IF NOT EXISTS "IX__objects__value_long" ON _objects (_value_long) WHERE _value_long IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_string" ON _objects (_value_string) WHERE _value_string IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_guid" ON _objects (_value_guid) WHERE _value_guid IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_datetime" ON _objects (_value_datetime) WHERE _value_datetime IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__objects__value_numeric" ON _objects (_value_numeric) WHERE _value_numeric IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__dependencies__schemes_1" ON _dependencies (_id_scheme_1) WITH (deduplicate_items=True); 
CREATE INDEX IF NOT EXISTS "IX__dependencies__schemes_2" ON _dependencies (_id_scheme_2) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__structures__structures" ON _structures (_id_parent) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__structures__schemes" ON _structures (_id_scheme) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__structures__types" ON _structures (_id_type) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__structures__lists" ON _structures (_id_list) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__schemes__schemes" ON _schemes (_id_parent) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__schemes__structure_hash" ON _schemes (_structure_hash) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__schemes__type" ON _schemes (_type) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__users_roles__roles" ON _users_roles (_id_role) WITH (deduplicate_items=True);
CREATE INDEX IF NOT EXISTS "IX__users_roles__users" ON _users_roles (_id_user) WITH (deduplicate_items=True);

-- Indexes for user and role configurations
CREATE INDEX IF NOT EXISTS "IX__users__id_configuration" ON _users (_id_configuration) WHERE _id_configuration IS NOT NULL;
CREATE INDEX IF NOT EXISTS "IX__roles__id_configuration" ON _roles (_id_configuration) WHERE _id_configuration IS NOT NULL;

-- Foreign keys for configurations (added after creating all tables)
ALTER TABLE _users ADD CONSTRAINT FK_users_configuration FOREIGN KEY (_id_configuration) REFERENCES _objects(_id) ON DELETE SET NULL;
ALTER TABLE _roles ADD CONSTRAINT FK_roles_configuration FOREIGN KEY (_id_configuration) REFERENCES _objects(_id) ON DELETE SET NULL;


-- PostgreSQL: partial index (only rows where _array_index IS NULL)
CREATE INDEX ix__values__array_null_structure 
ON _values (_id_structure, _id_object, _long) 
WHERE _array_index IS NULL;


-- Index for quickly getting all nested fields by structure_id
-- NOTE: _String removed from INCLUDE to prevent btree row overflow on large string values
CREATE INDEX IF NOT EXISTS "IX__values__structure_parent_batch" 
ON _values (_id_structure, _array_parent_id) 
INCLUDE (_Long, _Double, _Boolean, _Guid)
WHERE _array_parent_id IS NOT NULL;


-- ============================================
-- OPTIMIZED INDEXES FOR FACETED SEARCH
-- (Index Only Scan with INCLUDE - maximum performance)
-- Tested EXPLAIN ANALYZE on Stage 41 - all Seq Scan eliminated!
-- ============================================

-- 🔥 CRITICAL #1: Covering index for ORDER BY by structure name
-- Eliminates: Seq Scan on _structures s (Filter: (_name)::text = 'Auction.Baskets')
-- Cost reduction: 6.10 → 4.29 (-30%)
CREATE INDEX IF NOT EXISTS "IX__structures__name" 
ON _structures (_name) 
INCLUDE (_id, _id_type, _collection_type, _id_scheme)
WITH (deduplicate_items=True);

-- 🔥 CRITICAL #2: Covering index for searching structure by ID
-- Eliminates: Seq Scan on _structures (Filter: _id = X) in InitPlan
-- Eliminates: Seq Scan on _structures fs_1 (Filter: (_collection_type IS NULL) AND (_id = X))
-- Cost reduction: 6.10 → 4.29 (-30%)
CREATE INDEX IF NOT EXISTS "IX__structures__id_lookup" 
ON _structures (_id) 
INCLUDE (_id_type, _name, _collection_type, _id_scheme)
WITH (deduplicate_items=True);

-- Partial index for non-array structures (base)
CREATE INDEX IF NOT EXISTS "IX__structures__not_collection" 
ON _structures (_id, _name, _id_scheme) 
WHERE _collection_type IS NULL;

-- Extended partial index for non-collection structures
-- Used in Nested Loop for fast search by _id with _collection_type IS NULL
CREATE INDEX IF NOT EXISTS "IX__structures__not_collection_enhanced" 
ON _structures (_id, _name, _id_scheme, _id_type) 
WHERE _collection_type IS NULL;

-- Index for collection fields (Array/Dictionary)
CREATE INDEX IF NOT EXISTS "IX__structures__collection" 
ON _structures (_id, _id_scheme, _collection_type) 
WHERE _collection_type IS NOT NULL;

-- Index for dictionaries with key type
CREATE INDEX IF NOT EXISTS "IX__structures__key_type" 
ON _structures (_id, _id_scheme, _key_type) 
WHERE _key_type IS NOT NULL;

-- Index for collection type lookups (Array/Dictionary fields)
CREATE INDEX IF NOT EXISTS "IX__structures__collection_type" 
ON _structures (_collection_type) 
WHERE _collection_type IS NOT NULL;

-- 🚀 Covering index for _values (object -> structure)
-- Index Only Scan for EXISTS subqueries with value filtering
-- INCLUDE allows filtering (_long > X) without accessing the table
-- NOTE: _String excluded from INCLUDE — large text values (>2700 bytes) exceed btree row limit
CREATE INDEX IF NOT EXISTS "IX__values__object_structure_lookup" 
ON _values (_id_object, _id_structure, _array_index) 
INCLUDE (_Long, _Double, _DateTimeOffset, _Boolean, _Guid, _Numeric, _ListItem, _Object)
WITH (deduplicate_items=True);

-- 🚀 Covering index for nested fields (Class)
-- Index Only Scan for JOIN nested fields and ORDER BY subqueries
-- INCLUDE eliminates table access for reading values
-- NOTE: _String excluded from INCLUDE — large text values (>2700 bytes) exceed btree row limit
CREATE INDEX IF NOT EXISTS "IX__values__object_array_null" 
ON _values (_id_object, _id_structure) 
INCLUDE (_Long, _Double, _DateTimeOffset, _Boolean, _Guid, _Numeric, _ListItem, _Object)
WHERE _array_index IS NULL;

-- ============================================
-- COMMENTS FOR OPTIMIZED INDEXES
-- ============================================

COMMENT ON INDEX "IX__structures__name" IS 
'🔥 CRITICAL covering index for ORDER BY subqueries. Eliminates Seq Scan when searching structures by name (s._name = ''Field''). INCLUDE (_id, _id_type, _collection_type, _id_scheme) provides Index Only Scan without table access. Cost: 6.10 → 4.29 (-30%). Tested EXPLAIN ANALYZE Stage 41';

COMMENT ON INDEX "IX__structures__id_lookup" IS 
'🔥 CRITICAL covering index for InitPlan and EXISTS subqueries. Eliminates Seq Scan on Filter: (_id = X). INCLUDE contains all necessary fields for Index Only Scan. Covers InitPlan 2 and Nested Loop conditions. Cost: 6.10 → 4.29 (-30%). Tested EXPLAIN ANALYZE Stage 41';

COMMENT ON INDEX "IX__structures__not_collection" IS 
'Base partial index for NON collection fields. Optimizes filtering _collection_type IS NULL in faceted queries. Used together with IX__structures__name';

COMMENT ON INDEX "IX__structures__not_collection_enhanced" IS 
'Extended partial index for NON collection fields. Includes _id_type to eliminate additional JOINs in Nested Loop. Used when searching by _id with condition _collection_type IS NULL. Provides Index Only Scan';

COMMENT ON INDEX "IX__structures__collection" IS 
'Partial index for collection fields (Array/Dictionary). Optimizes searching arrays and dictionaries by _collection_type';

COMMENT ON INDEX "IX__structures__collection_type" IS 
'Index for fast search by collection type (Array or Dictionary). Used when analyzing schemes';

COMMENT ON INDEX "IX__values__array_key" IS 
'Index for fast search by Dictionary key. Supports string keys for Dictionary<string, T>';

COMMENT ON INDEX "IX__values__object_structure_lookup" IS 
'🚀 Covering index for EXISTS subqueries with value filtering. INCLUDE contains all value types for Index Only Scan. Updated: added _Numeric, _ListItem, _Object; _DateTime → _DateTimeOffset. Critical for faceted search with conditions on nested Class fields';

COMMENT ON INDEX "IX__values__object_array_null" IS 
'🚀 Covering index for non-array values. Optimizes JOIN nested Class fields and ORDER BY subqueries. INCLUDE contains all value types for reading without table access. Updated: added _Numeric, _ListItem, _Object; _DateTime → _DateTimeOffset. Partial index (_array_index IS NULL) reduces size by 30-40%';

-- ============================================
-- CRITICAL INDEXES FOR FACETED SEARCH
-- ============================================

-- 1. 🔥 MOST IMPORTANT: search values by structure + object
-- NOTE: _String excluded from KEY — large text values (>2700 bytes) exceed btree row limit
CREATE INDEX IF NOT EXISTS "IX__values__structure_object_lookup" 
ON _values (_id_structure, _id_object, _Long, _DateTimeOffset, _Boolean, _Double, _Guid, _Numeric, _ListItem, _Object);

-- 2. 🌳 Tree queries
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent" 
ON _objects (_id_scheme, _id_parent, _id);

-- 3. 🔢 Arrays
CREATE INDEX IF NOT EXISTS "IX__values__object_structure_array_index" 
ON _values (_id_object, _id_structure, _array_index)
WHERE _array_index IS NOT NULL;

-- 4. 📌 Partial indexes for NOT NULL values
-- NOTE: length guard prevents btree overflow on large string values (JWT payloads > 2704 bytes)
CREATE INDEX IF NOT EXISTS "IX__values__String_not_null" 
ON _values (_id_structure, _id_object, _String)
WHERE _String IS NOT NULL AND length(_String) < 2000;

CREATE INDEX IF NOT EXISTS "IX__values__Long_not_null" 
ON _values (_id_structure, _id_object, _Long)
WHERE _Long IS NOT NULL;

CREATE INDEX IF NOT EXISTS "IX__values__DateTimeOffset_not_null" 
ON _values (_id_structure, _id_object, _DateTimeOffset)
WHERE _DateTimeOffset IS NOT NULL;

CREATE INDEX IF NOT EXISTS "IX__values__Numeric_not_null" 
ON _values (_id_structure, _id_object, _Numeric)
WHERE _Numeric IS NOT NULL;

-- 5. 🔍 Text search (requires pg_trgm)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS "IX__values__String_pattern" 
ON _values USING gin (_String gin_trgm_ops)
WHERE _String IS NOT NULL;

-- 6. 🌳 Hierarchical queries (recursive CTE)
CREATE INDEX IF NOT EXISTS "IX__objects__parent_scheme_id" 
ON _objects (_id_parent, _id_scheme, _id)
WHERE _id_parent IS NOT NULL;

-- 7. 🌲 Root objects
CREATE INDEX IF NOT EXISTS "IX__objects__root_objects" 
ON _objects (_id_scheme, _id)
WHERE _id_parent IS NULL;

-- 8. 📅 Sorting by dates
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_date_create" 
ON _objects (_id_scheme, _date_create DESC, _id);

CREATE INDEX IF NOT EXISTS "IX__objects__scheme_date_modify" 
ON _objects (_id_scheme, _date_modify DESC, _id);

-- 9. 🔤 Sorting by name
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_name" 
ON _objects (_id_scheme, _name, _id);

-- Comments for indexes
COMMENT ON INDEX "IX__values__structure_object_lookup" IS 
'Composite index for fast value search by structure and object. CRITICAL for faceted queries with filtering by Props fields. Updated: added _Numeric, _ListItem, _Object; _DateTime → _DateTimeOffset';

COMMENT ON INDEX "IX__objects__scheme_parent" IS 
'Composite index for fast filtering of objects by scheme and parent. CRITICAL for tree queries and searching direct children';

COMMENT ON INDEX "IX__values__object_structure_array_index" IS 
'Composite index for fast work with arrays. Optimizes $arrayContains, $arrayFirst, $arrayLast, $arrayAt and aggregation operators';

COMMENT ON INDEX "IX__values__String_not_null" IS 
'Partial index for fast search of objects with NOT NULL string values. Optimizes queries with {"field": {"$ne": null}} and {"field": {"$exists": true}}';

COMMENT ON INDEX "IX__values__String_pattern" IS 
'GIN index for fast text search via LIKE/ILIKE. Requires pg_trgm extension. Optimizes $contains, $startsWith, $endsWith, $matches (regex)';

COMMENT ON INDEX "IX__objects__parent_scheme_id" IS 
'Partial index for fast recursive hierarchy traversal. Optimizes $hasAncestor, $hasDescendant, $level conditions';

COMMENT ON INDEX "IX__objects__root_objects" IS 
'Partial index for fast search of root objects (without parent). Optimizes {"$isRoot": true} condition';

COMMENT ON INDEX "IX__objects__scheme_date_create" IS 
'Composite index for fast sorting by creation date. Uses DESC to optimize ORDER BY _date_create DESC';

-- ====================================================================================================
-- CRITICAL INDEXES FOR OPTIMIZING WhereHasAncestor/WhereHasDescendant
-- ====================================================================================================

-- 1. CRITICAL: Covering index for fast descendant search by ancestor
CREATE INDEX IF NOT EXISTS "IX__objects__parent_id_descendant_lookup" 
ON _objects (_id_parent, _id_scheme) 
INCLUDE (_id, _id_owner, _date_create, _date_modify)
WHERE _id_parent IS NOT NULL;

COMMENT ON INDEX "IX__objects__parent_id_descendant_lookup" IS 
'Covering index for fast search of all descendants of an object. INCLUDE allows getting basic fields without table access. Critical for optimized WhereHasAncestor (inverted logic). Index Only Scan speeds up queries 10-50x';

-- 2. IMPORTANT: Composite index for tree queries with date sorting
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent_date_create" 
ON _objects (_id_scheme, _id_parent, _date_create DESC)
INCLUDE (_id, _id_owner, _date_modify);

COMMENT ON INDEX "IX__objects__scheme_parent_date_create" IS 
'Optimizes tree queries with sorting by creation date. Used in WhereHasAncestor + ORDER BY _date_create. INCLUDE for Index Only Scan';

-- 3. IMPORTANT: Composite index for reverse ancestor search
CREATE INDEX IF NOT EXISTS "IX__objects__id_parent_scheme" 
ON _objects (_id, _id_parent, _id_scheme)
WHERE _id_parent IS NOT NULL;

COMMENT ON INDEX "IX__objects__id_parent_scheme" IS 
'Accelerates reverse ancestor search (from descendant to parent). Used when building Parent/Children chains in ToTreeListAsync';

-- 4. MEDIUM PRIORITY: Covering index for search with filtering
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent_owner" 
ON _objects (_id_scheme, _id_parent)
INCLUDE (_id, _id_owner, _date_create, _date_modify, _name)
WHERE _id_parent IS NOT NULL;

COMMENT ON INDEX "IX__objects__scheme_parent_owner" IS 
'Covering index for tree queries with additional filtering by owner/name. Index Only Scan for complex WHERE conditions';

-- Index for nested Dictionary/Array field lookups via _array_parent_id
-- Used by PRO PVT CTE for AddressBook[home].City queries
CREATE INDEX IF NOT EXISTS "IX__values__parent_structure" 
ON _values (_array_parent_id, _id_structure) 
WHERE _array_parent_id IS NOT NULL;


CREATE OR REPLACE FUNCTION public.get_scheme_definition(
    scheme_id bigint
) RETURNS json
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json json;
    scheme_json json;
    structures_json json;
    scheme_exists boolean;
BEGIN
    -- Check if scheme exists
    SELECT EXISTS(SELECT 1 FROM _schemes WHERE _id = scheme_id) INTO scheme_exists;
    
    IF NOT scheme_exists THEN
        RETURN json_build_object('error', 'Scheme not found');
    END IF;
    
    -- Get scheme information
    SELECT json_build_object(
        '_id', _s._id,
        '_id_parent', _s._id_parent,
        '_name', _s._name,
        '_alias', _s._alias,
        '_name_space', _s._name_space
    ) INTO scheme_json
    FROM _schemes _s
    WHERE _s._id = scheme_id;
    
    -- Get scheme structures (fields)
    SELECT json_agg(
        json_build_object(
            '_id', _st._id,
            '_id_parent', _st._id_parent,
            '_name', _st._name,
            '_alias', _st._alias,
            '_order', _st._order,
            '_type_name', _t._name,
            '_type_db_type', _t._db_type,
            '_type_dotnet_type', _t._type,
            '_readonly', _st._readonly,
            '_allow_not_null', _st._allow_not_null,
            '_collection_type', _st._collection_type,  -- Array/Dictionary type ID or NULL
            '_key_type', _st._key_type,                -- Key type for Dictionary
            '_is_compress', _st._is_compress,
            '_store_null', _st._store_null,
            '_id_list', _st._id_list,
            '_default_editor', _st._default_editor
        ) ORDER BY _st._order, _st._id
    ) INTO structures_json
    FROM _structures _st
    JOIN _types _t ON _t._id = _st._id_type
    WHERE _st._id_scheme = scheme_id;
    
    -- Build final JSON
    result_json := json_build_object(
        'scheme', scheme_json,
        'structures', COALESCE(structures_json, '[]'::json)
    );
    
    RETURN result_json;
END;
$BODY$;

-- View for selecting definitions of all schemes
CREATE OR REPLACE VIEW v_schemes_definition AS
SELECT 
    _id as scheme_id,
    _name as scheme_name,
    _alias as scheme_alias,
    get_scheme_definition(_id) as scheme_definition
FROM _schemes
ORDER BY _id;



CREATE SEQUENCE global_identity
 AS bigint
 START WITH 1000000
 INCREMENT BY 1
 MINVALUE 1000000
 MAXVALUE 9223372036854775807;

INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775709, 'Boolean', 'Boolean', 'boolean');
-- DateTime (C# type) -> DateTimeOffset (DB timestamptz)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775708, 'DateTime', 'DateTimeOffset', 'DateTime');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775707, 'Double', 'Double', 'double');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775706, 'ListItem', 'ListItem', '_RListItem');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775705, 'Guid', 'Guid', 'Guid');
-- DateTimeOffset (C# type) -> DateTimeOffset (DB timestamptz)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775673, 'DateTimeOffset', 'DateTimeOffset', 'DateTimeOffset');
-- Numeric for exact decimal numbers (financial calculations)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775674, 'Numeric', 'Numeric', 'decimal');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775704, 'Long', 'Long', 'long');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775703, 'Object', 'Object', '_RObject');
--INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775702, 'Text', 'Text', 'string');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775701, 'ByteArray', 'ByteArray', 'byte[]');
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775700, 'String', 'String', 'string');
-- Additional simple types for REDB
-- These can be added to extend functionality

-- 1. Additional numeric types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775699, 'Int', 'Long', 'int');           -- int (mapped to Long)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775698, 'Short', 'Long', 'short');       -- short (mapped to Long)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775697, 'Byte', 'Long', 'byte');         -- byte (mapped to Long)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775696, 'Float', 'Double', 'float');     -- float (mapped to Double)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775695, 'Decimal', 'Numeric', 'decimal'); -- decimal (mapped to Numeric for precision)

-- 2. Additional string types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775694, 'Char', 'String', 'char');        -- char (mapped to String)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775693, 'Url', 'String', 'string');       -- URL (validation via attributes)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775692, 'Email', 'String', 'string');     -- Email (validation via attributes)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775691, 'Phone', 'String', 'string');     -- Phone (validation via attributes)

-- 3. Special types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775690, 'Json', 'String', 'string');      -- JSON as string
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775689, 'Xml', 'String', 'string');       -- XML as string
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775688, 'Base64', 'String', 'string');    -- Base64 strings
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775687, 'Color', 'String', 'string');     -- Colors (hex, rgb)

-- 4. Temporal types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775686, 'DateOnly', 'DateTime', 'DateOnly');     -- .NET 6+ DateOnly
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775685, 'TimeOnly', 'String', 'TimeOnly');      -- .NET 6+ TimeOnly (as string)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775684, 'TimeSpan', 'String', 'TimeSpan');      -- TimeSpan (format "HH:MM:SS" for JSON compatibility)

-- 5. Enum support
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775683, 'Enum', 'String', 'Enum');        -- Enum (stored as string)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775682, 'EnumInt', 'Long', 'Enum');       -- Enum (stored as number)

-- 6. Geographic types (for future expansion)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775681, 'Latitude', 'Double', 'double');   -- Latitude
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775680, 'Longitude', 'Double', 'double');  -- Longitude
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775679, 'GeoPoint', 'String', 'string');   -- Geographic point as JSON

-- 7. File types
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775678, 'FilePath', 'String', 'string');   -- File path
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775677, 'FileName', 'String', 'string');   -- File name
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775676, 'MimeType', 'String', 'string');   -- MIME type
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775675, 'Class', 'Guid', 'Object');   -- Nested class type

-- Collection and document types for RedbObject<T> and Props fields
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775668, 'Array', 'Guid', 'Array');         -- For RedbObject<T[]>/List<T> and array fields in Props
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775667, 'Dictionary', 'Guid', 'Dictionary'); -- For RedbObject<Dictionary<K,V>> and dict fields in Props
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775666, 'JsonDocument', 'Guid', 'JsonDocument'); -- For RedbObject<JsonDocument> (hierarchy via _array_parent_id)
INSERT INTO _types (_id, _name, _db_type, _type) VALUES (-9223372036854775665, 'XDocument', 'Guid', 'XDocument');     -- For RedbObject<XDocument> (hierarchy via _array_parent_id)

-- Comments for additional types
COMMENT ON TABLE _types IS 'REDB data types table. Supports basic C# types and additional specialized types with validation';

-- Usage examples:
-- 1. Int, Short, Byte - all mapped to Long for simplicity
-- 2. Float - mapped to Double
-- 3. Decimal - can be mapped to Double (precision loss) or Numeric (precision preserved)
-- 4. Char - mapped to String
-- 5. Url, Email, Phone - strings with additional application-level validation
-- 6. Json, Xml - strings for storing structured data
-- 7. DateOnly, TimeOnly, TimeSpan - special temporal types .NET 6+
-- 8. DateTime - for dates without timezone, DateTimeOffset - for dates with timezone
-- 9. Enum - enumeration support (as string or number)
-- 10. Geographic types - for location-based applications
-- 11. File types - for working with files and media

INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (-1, 'default', '', 'default', NULL, NULL, CAST('2023-12-26T01:14:34.410' AS timestamptz), NULL, true);
INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (0, 'sys', '', 'sys', NULL, NULL, CAST('2023-12-26T01:14:34.410' AS timestamptz), NULL, true);
INSERT INTO _users (_id, _login, _password, _name, _phone, _email, _date_register, _date_dismiss, _enabled) VALUES (1, 'admin', '', 'admin', NULL, NULL, CAST('2023-12-26T01:14:34.410' AS timestamptz), NULL, true);

-- =====================================================
-- SOFT DELETE SYSTEM: Reserved scheme for deleted objects
-- =====================================================
-- Scheme @@__deleted is used for soft-delete functionality
-- Objects marked for deletion get _id_scheme = -10
-- Type = Object (-9223372036854775703) means no Props/structures
INSERT INTO _schemes (_id, _name, _alias, _type) 
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
ON CONFLICT (_id) DO NOTHING;

-- for webApi dashboard

-- Trigger for validating field names in _structures
CREATE OR REPLACE FUNCTION validate_structure_name()
RETURNS TRIGGER AS $$
DECLARE
    reserved_fields text[] := ARRAY[
        '_id', '_id_parent', '_id_scheme', '_id_owner', '_id_who_change',
        '_date_create', '_date_modify', '_date_begin', '_date_complete',
        '_key', '_name', '_note', '_hash',
        '_value_long', '_value_string', '_value_guid', '_value_bool',
        '_value_double', '_value_numeric', '_value_datetime', '_value_bytes'
    ];
    field_name text;
BEGIN
    -- Check only on INSERT and UPDATE of _name field
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD._name IS DISTINCT FROM NEW._name) THEN
        field_name := LOWER(NEW._name);
        
        -- Check 1: Name must not match reserved fields of _objects
        IF field_name = ANY(reserved_fields) THEN
            RAISE EXCEPTION 'Field name "_name" cannot match system object fields: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Use a different name, avoid: ' || array_to_string(reserved_fields, ', ');
        END IF;
        
        -- Check 2: Name must not start with a digit
        IF NEW._name ~ '^[0-9]' THEN
            RAISE EXCEPTION 'Field name "_name" cannot start with a digit: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Field name must start with a letter or underscore';
        END IF;
        
        -- Check 3: Name must follow C# naming rules (letters, digits, underscores)
        IF NEW._name !~ '^[a-zA-Z_][a-zA-Z0-9_]*$' THEN
            RAISE EXCEPTION 'Field name "_name" contains invalid characters: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Name can only contain Latin letters, digits, underscores. Must start with letter or underscore';
        END IF;
        
        -- Check 4: Name must not be empty or contain only spaces
        IF LENGTH(TRIM(NEW._name)) = 0 THEN
            RAISE EXCEPTION 'Field name "_name" cannot be empty'
                USING ERRCODE = '23514';
        END IF;
        
        -- Check 5: Maximum name length (reasonable limit)
        IF LENGTH(NEW._name) > 64 THEN
            RAISE EXCEPTION 'Field name "_name" is too long (max 64 characters): %', NEW._name
                USING ERRCODE = '23514';
        END IF;
        
        -- Check 6: Additional C# reserved words
        IF LOWER(NEW._name) = ANY(ARRAY[
            'abstract', 'as', 'bool', 'break', 'byte', 'case', 'catch', 'char', 'checked',
            'class', 'const', 'continue', 'decimal', 'default', 'delegate', 'do', 'double', 'else',
            'enum', 'event', 'explicit', 'extern', 'false', 'finally', 'fixed', 'float', 'for',
            'foreach', 'goto', 'if', 'implicit', 'in', 'int', 'interface', 'internal', 'is', 'lock',
            'long', 'namespace', 'new', 'null', 'object', 'operator', 'out', 'override', 'params',
            'private', 'protected', 'public', 'readonly', 'ref', 'return', 'sbyte', 'sealed',
            'short', 'sizeof', 'stackalloc', 'static', 'string', 'struct', 'switch', 'this',
            'throw', 'true', 'try', 'typeof', 'uint', 'ulong', 'unchecked', 'unsafe', 'ushort',
            'using', 'virtual', 'void', 'volatile', 'while'
        ]) THEN
            RAISE EXCEPTION 'Field name "_name" is a C# reserved word: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Use a different name or add prefix/suffix';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER tr_validate_structure_name
    BEFORE INSERT OR UPDATE ON _structures
    FOR EACH ROW
    EXECUTE FUNCTION validate_structure_name();

-- Comments for trigger
COMMENT ON FUNCTION validate_structure_name() IS 'Function for validating field names in _structures according to C# naming rules';
COMMENT ON TRIGGER tr_validate_structure_name ON _structures IS 'Trigger checks correctness of field names: prohibits system names, names starting with digits, special characters, and C# reserved words';

-- Trigger for validating scheme names in _schemes
CREATE OR REPLACE FUNCTION validate_scheme_name()
RETURNS TRIGGER AS $$
DECLARE
    scheme_name text;
BEGIN
    -- Check only on INSERT and UPDATE of _name field
    IF TG_OP = 'INSERT' OR (TG_OP = 'UPDATE' AND OLD._name IS DISTINCT FROM NEW._name) THEN
        scheme_name := LOWER(NEW._name);
        
        -- Skip validation for system schemes (starting with @@)
        -- These are reserved for internal use (e.g. @@__deleted for soft-delete)
        IF NEW._name ~ '^@@' THEN
            RETURN NEW;
        END IF;
        
        -- Check 1: Name must not start with a digit
        IF NEW._name ~ '^[0-9]' THEN
            RAISE EXCEPTION 'Scheme name "_name" cannot start with a digit: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Scheme name must start with a letter or underscore';
        END IF;
        
        -- Check 2: Name must follow C# class naming rules (letters, digits, underscores, dots for namespace, + for nested classes)
        IF NEW._name !~ '^[a-zA-Z_][a-zA-Z0-9_.+]*$' THEN
            RAISE EXCEPTION 'Scheme name "_name" contains invalid characters: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Name can only contain Latin letters, digits, underscores, dots and +. Must start with letter or underscore';
        END IF;
        
        -- Check 3: Name must not end with a dot
        IF NEW._name ~ '\.$' THEN
            RAISE EXCEPTION 'Scheme name "_name" cannot end with a dot: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Remove dot at the end of the name';
        END IF;
        
        -- Check 4: Name must not contain two consecutive dots
        IF NEW._name ~ '\.\.' THEN
            RAISE EXCEPTION 'Scheme name "_name" cannot contain two consecutive dots: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Use one dot to separate namespace parts';
        END IF;
        
        -- Check 5: Name must not be empty or contain only spaces
        IF LENGTH(TRIM(NEW._name)) = 0 THEN
            RAISE EXCEPTION 'Scheme name "_name" cannot be empty'
                USING ERRCODE = '23514';
        END IF;
        
        -- Check 6: Maximum name length (reasonable limit)
        IF LENGTH(NEW._name) > 128 THEN
            RAISE EXCEPTION 'Scheme name "_name" is too long (max 128 characters): %', NEW._name
                USING ERRCODE = '23514';
        END IF;
        
        -- Check 7: C# reserved words (check each part after splitting by dots or +)
        IF EXISTS (
            SELECT 1 FROM regexp_split_to_table(LOWER(NEW._name), '[.+]') AS part
            WHERE part = ANY(ARRAY[
                'abstract', 'as', 'bool', 'break', 'byte', 'case', 'catch', 'char', 'checked',
                'class', 'const', 'continue', 'decimal', 'default', 'delegate', 'do', 'double', 'else',
                'enum', 'event', 'explicit', 'extern', 'false', 'finally', 'fixed', 'float', 'for',
                'foreach', 'goto', 'if', 'implicit', 'in', 'int', 'interface', 'internal', 'is', 'lock',
                'long', 'namespace', 'new', 'null', 'object', 'operator', 'out', 'override', 'params',
                'private', 'protected', 'public', 'readonly', 'ref', 'return', 'sbyte', 'sealed',
                'short', 'sizeof', 'stackalloc', 'static', 'string', 'struct', 'switch', 'this',
                'throw', 'true', 'try', 'typeof', 'uint', 'ulong', 'unchecked', 'unsafe', 'ushort',
                'using', 'virtual', 'void', 'volatile', 'while'
            ])
        ) THEN
            RAISE EXCEPTION 'Scheme name "_name" contains C# reserved word: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Use a different name or add prefix/suffix to the problematic part';
        END IF;
        
        -- Check 8: Each part (separated by dots or +) must be a valid identifier
        IF EXISTS (
            SELECT 1 FROM regexp_split_to_table(NEW._name, '[.+]') AS part
            WHERE LENGTH(TRIM(part)) = 0 OR part ~ '^[0-9]' OR part !~ '^[a-zA-Z_][a-zA-Z0-9_]*$'
        ) THEN
            RAISE EXCEPTION 'Scheme name "_name" contains invalid namespace part: %', NEW._name
                USING ERRCODE = '23514',
                      HINT = 'Each part between dots/+ must be a valid C# identifier';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for schemes
CREATE TRIGGER tr_validate_scheme_name
    BEFORE INSERT OR UPDATE ON _schemes
    FOR EACH ROW
    EXECUTE FUNCTION validate_scheme_name();

-- Comments for scheme trigger
COMMENT ON FUNCTION validate_scheme_name() IS 'Function for validating scheme names in _schemes according to C# class and namespace naming rules';
COMMENT ON TRIGGER tr_validate_scheme_name ON _schemes IS 'Trigger checks correctness of scheme names: C# class naming rules, namespace support via dots, reserved words';

-- Function for building facets (faceted search)


-- Function for faceted search of objects with filtering (REFACTORED MODULAR VERSION)

-- Function to get user permissions for a specific object
CREATE OR REPLACE FUNCTION get_user_permissions_for_object(
    p_object_id bigint,
    p_user_id bigint DEFAULT NULL  -- now optional for use in trigger
) RETURNS TABLE(
    object_id bigint,
    user_id bigint,
    permission_source_id bigint,
    permission_type varchar,
    _id_role bigint,
    _id_user bigint,
    can_select boolean,
    can_insert boolean,
    can_update boolean,
    can_delete boolean
) AS $$
BEGIN
    -- System user (id=0) has full rights to everything
    IF p_user_id = 0 THEN
        RETURN QUERY SELECT 
            p_object_id as object_id,
            0::bigint as user_id,
            0::bigint as permission_source_id,
            'system'::varchar as permission_type,
            NULL::bigint as _id_role,
            0::bigint as _id_user,
            true as can_select,
            true as can_insert,
            true as can_update,
            true as can_delete;
        RETURN;
    END IF;

    RETURN QUERY
    WITH RECURSIVE permission_search AS (
        -- Step 1: Start with target object
        SELECT 
            p_object_id as object_id,
            p_object_id as current_search_id,
            o._id_parent,
            0 as level,
            EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = p_object_id) as has_permission
        FROM _objects o
        WHERE o._id = p_object_id
        
        UNION ALL
        
        -- Step 2: If NO permission - go to parent
        SELECT 
            ps.object_id,
            o._id as current_search_id,
            o._id_parent,
            ps.level + 1,
            EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) as has_permission
        FROM _objects o
        JOIN permission_search ps ON o._id = ps._id_parent
        WHERE ps.level < 50
          AND ps.has_permission = false  -- continue only if NO permission
    ),
    -- Take first found permission for object
    object_permission AS (
        SELECT DISTINCT ON (ps.object_id)
            ps.object_id,
            p._id as permission_id,
            p._id_user,
            p._id_role,
            p._select,
            p._insert,
            p._update,
            p._delete,
            ps.level,
            ps.current_search_id as permission_source_id
        FROM permission_search ps
        JOIN _permissions p ON p._id_ref = ps.current_search_id
        WHERE ps.has_permission = true
        ORDER BY ps.object_id, ps.level  -- closest to object
    ),
    -- Add global rights as fallback (_id_ref = 0)
    global_permission AS (
        SELECT 
            p_object_id as object_id,
            p._id as permission_id,
            p._id_user,
            p._id_role,
            p._select,
            p._insert,
            p._update,
            p._delete,
            999 as level,  -- low priority
            0 as permission_source_id
        FROM _permissions p
        WHERE p._id_ref = 0
    ),
    -- Combine specific and global rights
    all_permissions AS (
        SELECT * FROM object_permission
        UNION ALL
        SELECT * FROM global_permission
    ),
    -- Take first by priority (specific > global)
    final_permission AS (
        SELECT DISTINCT ON (object_id)
            *
        FROM all_permissions
        ORDER BY object_id, level  -- specific rights have smaller level
    )
    -- Result: for user permissions - directly, for role permissions - via users_roles
    SELECT 
        fp.object_id,
        CASE 
            WHEN p_user_id IS NULL THEN NULL  -- if user_id not provided for trigger
            WHEN fp._id_user IS NOT NULL THEN fp._id_user  -- direct user permission
            ELSE ur._id_user  -- via role
        END as user_id,
        fp.permission_source_id,
        CASE 
            WHEN fp._id_user IS NOT NULL THEN 'user'::varchar
            ELSE 'role'::varchar
        END as permission_type,
        fp._id_role,
        fp._id_user,
        fp._select as can_select,
        fp._insert as can_insert,
        fp._update as can_update,
        fp._delete as can_delete
    FROM final_permission fp
    LEFT JOIN _users_roles ur ON ur._id_role = fp._id_role  -- only for role permissions
    WHERE p_user_id IS NULL OR (fp._id_user = p_user_id OR ur._id_user = p_user_id);  -- if user_id NULL - all permissions, otherwise filter
END;
$$ LANGUAGE plpgsql;

-- Comment for function to get permissions for object
COMMENT ON FUNCTION get_user_permissions_for_object(bigint, bigint) IS 'Function to get effective permissions for a specific object considering hierarchical inheritance and priorities (user > role). If user_id = NULL, returns first found permission without filtering by user (for use in triggers)';

-- Trigger function for automatically creating permissions when creating node objects
CREATE OR REPLACE FUNCTION auto_create_node_permissions()
RETURNS TRIGGER AS $$
DECLARE
    source_permission RECORD;
BEGIN
    -- Process only INSERT of new objects with parent
    IF TG_OP = 'INSERT' AND NEW._id_parent IS NOT NULL THEN
        
        -- Check if parent already has permission
        IF EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = NEW._id_parent) THEN
            RETURN NEW;  -- Parent already has permission, do nothing
        END IF;
        
        -- Use modified function WITHOUT user_id to search for source permission
        SELECT * INTO source_permission 
        FROM get_user_permissions_for_object(NEW._id_parent, NULL) 
        LIMIT 1;
        
        -- If found source permission - create permission for parent
        IF FOUND THEN
            INSERT INTO _permissions (
                _id, _id_role, _id_user, _id_ref,
                _select, _insert, _update, _delete
            ) VALUES (
                nextval('global_identity'),
                source_permission._id_role,
                source_permission._id_user,
                NEW._id_parent,  -- create permission for parent
                source_permission.can_select,
                source_permission.can_insert,
                source_permission.can_update,
                source_permission.can_delete
            );
        END IF;
        
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on _objects table
CREATE TRIGGER tr_auto_create_node_permissions
    AFTER INSERT ON _objects
    FOR EACH ROW
    EXECUTE FUNCTION auto_create_node_permissions();

-- Comments for trigger
COMMENT ON FUNCTION auto_create_node_permissions() IS 'Trigger function for automatically creating permissions for parent objects when child objects appear. Creates permission only if parent doesn''t have one yet, inheriting from nearest ancestor with permission';
COMMENT ON TRIGGER tr_auto_create_node_permissions ON _objects IS 'Trigger automatically creates permission for an object when a child object is added to it, if it doesn''t already have its own permission. Speeds up permission search by reducing recursion depth';

CREATE VIEW v_user_permissions AS
WITH RECURSIVE permission_search AS (
    -- Step 1: Each object searches for its permission
    SELECT 
        o._id as object_id,
        o._id as current_search_id,
        o._id_parent,
        0 as level,
        EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) as has_permission
    FROM _objects o
    
    UNION ALL
    
    -- Step 2: If NO permission - go to parent
    SELECT 
        ps.object_id,
        o._id as current_search_id,
        o._id_parent,
        ps.level + 1,
        EXISTS(SELECT 1 FROM _permissions WHERE _id_ref = o._id) as has_permission
    FROM _objects o
    JOIN permission_search ps ON o._id = ps._id_parent
    WHERE ps.level < 50
      AND ps.has_permission = false  -- continue only if NO permission
),
-- Take first found permission for each object
object_permissions AS (
    SELECT DISTINCT ON (ps.object_id)
        ps.object_id,
        p._id as permission_id,
        p._id_user,
        p._id_role,
        p._select,
        p._insert,
        p._update,
        p._delete,
        ps.level
    FROM permission_search ps
    JOIN _permissions p ON p._id_ref = ps.current_search_id
    WHERE ps.has_permission = true
    ORDER BY ps.object_id, ps.level  -- closest to object
),
-- 🚀 NEW: Add global rights as virtual records with object_id = 0
global_permissions AS (
    SELECT 
        0 as object_id,  -- virtual object for global rights
        p._id as permission_id,
        p._id_user,
        p._id_role,
        p._select,
        p._insert,
        p._update,
        p._delete,
        999 as level  -- low priority
    FROM _permissions p
    WHERE p._id_ref = 0  -- global rights
),
-- Combine specific and global rights
all_permissions AS (
    SELECT * FROM object_permissions
    UNION ALL
    SELECT * FROM global_permissions
),
-- Take first by priority (specific > global)
final_permissions AS (
    SELECT DISTINCT ON (object_id)
        *
    FROM all_permissions
    ORDER BY object_id, level  -- specific rights have smaller level
)
-- Result: for user permissions - directly, for role permissions - via users_roles
SELECT 
    fp.object_id,
    CASE 
        WHEN fp._id_user IS NOT NULL THEN fp._id_user  -- direct user permission
        ELSE ur._id_user  -- via role
    END as user_id,
    fp.permission_id,
    CASE 
        WHEN fp._id_user IS NOT NULL THEN 'user'::varchar
        ELSE 'role'::varchar
    END as permission_type,
    fp._id_role,
    fp._select as can_select,
    fp._insert as can_insert,
    fp._update as can_update,
    fp._delete as can_delete
FROM final_permissions fp
LEFT JOIN _users_roles ur ON ur._id_role = fp._id_role  -- only for role permissions
WHERE fp._id_user IS NOT NULL OR ur._id_user IS NOT NULL;  -- has user


-- 🔐 TRIGGER FOR PROTECTING SYSTEM USERS
-- Prohibits deletion of system users with ID 0, 1, -1
-- IDs 0 (sys), -1 (default): fully immutable (no changes allowed)
-- Login (_login) is immutable for ALL users

-- Trigger function
CREATE OR REPLACE FUNCTION protect_system_users()
RETURNS TRIGGER AS $$
BEGIN
    -- Check DELETE operation
    IF TG_OP = 'DELETE' THEN
        -- Prohibit deletion of system users
        IF OLD._id IN (0, 1, -1) THEN
            RAISE EXCEPTION 'Cannot delete system user with ID %', OLD._id;
        END IF;
        RETURN OLD;
    END IF;
    
    -- Check UPDATE operation
    IF TG_OP = 'UPDATE' THEN
        -- Prohibit changing ID (just in case)
        IF OLD._id != NEW._id THEN
            RAISE EXCEPTION 'Cannot change user ID';
        END IF;
        
        -- IDs 0 (sys) and -1 (default): fully immutable, no changes allowed
        IF OLD._id IN (0, -1) THEN
            IF OLD._login != NEW._login OR OLD._password != NEW._password OR OLD._name != NEW._name
               OR COALESCE(OLD._phone, '') != COALESCE(NEW._phone, '')
               OR COALESCE(OLD._email, '') != COALESCE(NEW._email, '')
               OR OLD._enabled != NEW._enabled
               OR COALESCE(OLD._note, '') != COALESCE(NEW._note, '')
            THEN
                RAISE EXCEPTION 'Cannot modify system user with ID %', OLD._id;
            END IF;
        END IF;
        
        -- Login is immutable for ALL users
        IF OLD._login != NEW._login THEN
            RAISE EXCEPTION 'Cannot change user login';
        END IF;
        
        RETURN NEW;
    END IF;
    
    -- For INSERT nothing to check
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on _users table
DROP TRIGGER IF EXISTS tr_protect_system_users ON _users;
CREATE TRIGGER tr_protect_system_users
    BEFORE UPDATE OR DELETE ON _users
    FOR EACH ROW
    EXECUTE FUNCTION protect_system_users();

-- Comments for trigger
COMMENT ON FUNCTION protect_system_users() IS 'Protects system users (ID -1, 0) from any modification, prohibits login change for all users';
COMMENT ON TRIGGER tr_protect_system_users ON _users IS 'Trigger protecting system users and login immutability';


-- Disable trigger (keeps trigger definition but prevents execution)
ALTER TABLE _objects DISABLE TRIGGER tr_auto_create_node_permissions;


-- View current value
--SHOW shared_buffers;
 
-- Change (requires PostgreSQL restart)
-- ALTER SYSTEM SET shared_buffers = '2GB';


-- Schemes for TrueSight.Models

INSERT INTO _permissions (_id, _id_user, _id_ref, _select, _insert, _update, _delete)
VALUES (
    nextval('global_identity'),  -- Permission record ID
    0,        -- ID of sys user
    0,                           -- _id_ref = 0 means "all objects"
    true,                        -- _select = allow reading
    true,                        -- _insert = allow creation
    true,                        -- _update = allow modification
    true                         -- _delete = allow deletion
);
INSERT INTO _permissions (_id, _id_user, _id_ref, _select, _insert, _update, _delete)
VALUES (
    nextval('global_identity'),  -- Permission record ID
    1,        -- ID of sys user
    0,                           -- _id_ref = 0 means "all objects"
    true,                        -- _select = allow reading
    true,                        -- _insert = allow creation
    true,                        -- _update = allow modification
    true                         -- _delete = allow deletion
);

-- ===== redb_metadata_cache.sql =====
-- ============================================================
-- METADATA CACHE: Solution for repeated JOIN problem
-- ============================================================
-- Goal: Avoid repeated JOIN _structures ← _types in each query
-- Approach: UNLOGGED TABLE + automatic synchronization via triggers on _structure_hash
-- Advantages:
--   ✅ No recursion problems (indexes created once)
--   ✅ Works with connection pooling (global table)
--   ✅ Automatic invalidation (triggers on _schemes._structure_hash)
--   ✅ No changes in C# code required
--   ✅ Minimal cache rebuilds (only on actual schema changes)
-- ============================================================

-- 1️⃣ Create metadata cache (ALL fields from _structures + fields from _types)
DROP TABLE IF EXISTS _scheme_metadata_cache CASCADE;

CREATE TABLE _scheme_metadata_cache ( --<-- UNLOGGED TABLE IF NEEDED
    -- Identifiers
    _scheme_id bigint NOT NULL,
    _structure_id bigint NOT NULL,
    _parent_structure_id bigint,
    _id_override bigint,
    
    -- Names and aliases
    _name text NOT NULL,
    _alias text,
    
    -- Structure type
    _type_id bigint NOT NULL,
    _list_id bigint,
    type_name text NOT NULL,
    db_type text NOT NULL,
    type_semantic text NOT NULL,
    
    -- Scheme type (Class/Array/Dictionary/JsonDocument/XDocument)
    _scheme_type bigint,
    scheme_type_name text,
    
    -- Structure attributes
    _order bigint,
    _collection_type bigint,      -- NULL = not a collection, otherwise collection type ID (Array/Dictionary)
    collection_type_name text,    -- Collection type name
    _key_type bigint,             -- Key type for Dictionary
    key_type_name text,           -- Key type name
    _readonly boolean,
    _allow_not_null boolean,
    _is_compress boolean,
    _store_null boolean,
    
    -- Default values
    _default_value bytea,
    _default_editor text
);

-- 2️⃣ Indexes for fast search
CREATE INDEX idx_metadata_cache_lookup 
    ON _scheme_metadata_cache(_scheme_id, _parent_structure_id, _order);

CREATE INDEX idx_metadata_cache_structure 
    ON _scheme_metadata_cache(_structure_id);

CREATE INDEX idx_metadata_cache_scheme
    ON _scheme_metadata_cache(_scheme_id);

CREATE INDEX idx_metadata_cache_name
    ON _scheme_metadata_cache(_scheme_id, _name);

CREATE INDEX idx_metadata_cache_collection
    ON _scheme_metadata_cache(_scheme_id, _collection_type)
    WHERE _collection_type IS NOT NULL;

CREATE INDEX idx_metadata_cache_scheme_type
    ON _scheme_metadata_cache(_scheme_id, _scheme_type);

CREATE INDEX idx_metadata_cache_key_type
    ON _scheme_metadata_cache(_scheme_id, _key_type)
    WHERE _key_type IS NOT NULL;

-- 3️⃣ Cache synchronization function for a scheme
CREATE OR REPLACE FUNCTION sync_metadata_cache_for_scheme(target_scheme_id bigint)
RETURNS void AS $$
BEGIN
    -- Remove old scheme data
    DELETE FROM _scheme_metadata_cache 
    WHERE _scheme_id = target_scheme_id;
    
    -- Insert current data (with support for collection types and scheme type)
    INSERT INTO _scheme_metadata_cache (
        _scheme_id, _structure_id, _parent_structure_id, _id_override,
        _name, _alias,
        _type_id, _list_id, type_name, db_type, type_semantic,
        _scheme_type, scheme_type_name,
        _order, _collection_type, collection_type_name, _key_type, key_type_name,
        _readonly, _allow_not_null, _is_compress, _store_null,
        _default_value, _default_editor
    )
    SELECT 
        s._id_scheme,
        s._id,
        s._id_parent,
        s._id_override,
        s._name,
        s._alias,
        t._id,
        s._id_list,
        t._name,
        t._db_type,
        t._type,
        sch._type,                    -- Scheme type
        scht._name,                   -- Scheme type name
        s._order,
        s._collection_type,           -- Collection type (Array/Dictionary/NULL)
        ct._name,                     -- Collection type name
        s._key_type,                  -- Key type for Dictionary
        kt._name,                     -- Key type name
        s._readonly,
        s._allow_not_null,
        s._is_compress,
        s._store_null,
        s._default_value,
        s._default_editor
    FROM _structures s
    JOIN _types t ON t._id = s._id_type
    JOIN _schemes sch ON sch._id = s._id_scheme
    LEFT JOIN _types scht ON scht._id = sch._type         -- Scheme type
    LEFT JOIN _types ct ON ct._id = s._collection_type    -- Collection type
    LEFT JOIN _types kt ON kt._id = s._key_type           -- Key type
    WHERE s._id_scheme = target_scheme_id;
    
    -- NOTICE removed to avoid spam during mass warmup
    -- Use warmup_all_metadata_caches() to get statistics
END;
$$ LANGUAGE plpgsql;

-- 4️⃣ Trigger on _structure_hash change in _schemes
-- 🔥 KEY IDEA: Track ONLY hash changes, not every INSERT/UPDATE in _structures!
CREATE OR REPLACE FUNCTION sync_metadata_cache_on_hash_change()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if _structure_hash changed
    IF (TG_OP = 'UPDATE' AND OLD._structure_hash IS DISTINCT FROM NEW._structure_hash) THEN
        -- Hash changed → rebuild cache for this scheme
        PERFORM sync_metadata_cache_for_scheme(NEW._id);
        RAISE NOTICE 'Metadata cache rebuilt for scheme_id=% due to structure_hash change (old=%, new=%)', 
            NEW._id, OLD._structure_hash, NEW._structure_hash;
        
    ELSIF (TG_OP = 'INSERT' AND NEW._structure_hash IS NOT NULL) THEN
        -- New scheme with hash → create cache
        PERFORM sync_metadata_cache_for_scheme(NEW._id);
        RAISE NOTICE 'Metadata cache created for new scheme_id=% (hash=%)', 
            NEW._id, NEW._structure_hash;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_metadata_cache_on_hash_change ON _schemes;
CREATE TRIGGER trg_sync_metadata_cache_on_hash_change
AFTER INSERT OR UPDATE ON _schemes
FOR EACH ROW EXECUTE FUNCTION sync_metadata_cache_on_hash_change();

-- 5️⃣ Trigger on DELETE of scheme
CREATE OR REPLACE FUNCTION cleanup_metadata_cache_on_scheme_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM _scheme_metadata_cache WHERE _scheme_id = OLD._id;
    RAISE NOTICE 'Metadata cache cleared for deleted scheme_id=%', OLD._id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_cleanup_metadata_cache_on_scheme_delete ON _schemes;
CREATE TRIGGER trg_cleanup_metadata_cache_on_scheme_delete
AFTER DELETE ON _schemes
FOR EACH ROW EXECUTE FUNCTION cleanup_metadata_cache_on_scheme_delete();

-- 6️⃣ Trigger on type changes (invalidates entire cache)
-- 🔥 When _types change → all schemes must rebuild
CREATE OR REPLACE FUNCTION invalidate_all_metadata_caches()
RETURNS TRIGGER AS $$
BEGIN
    TRUNCATE _scheme_metadata_cache;
    RAISE NOTICE 'All metadata caches invalidated due to _types change';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_invalidate_all_caches ON _types;
CREATE TRIGGER trg_invalidate_all_caches
AFTER UPDATE OR DELETE ON _types
FOR EACH STATEMENT EXECUTE FUNCTION invalidate_all_metadata_caches();

-- 7️⃣ Warmup function (for application startup or after crash)
CREATE OR REPLACE FUNCTION warmup_all_metadata_caches()
RETURNS TABLE(scheme_id bigint, structures_count bigint, scheme_name text, structure_hash uuid) AS $$
BEGIN
    TRUNCATE _scheme_metadata_cache;
    
    -- Rebuild cache for ALL schemes (removed filter for _structure_hash)
    PERFORM sync_metadata_cache_for_scheme(s._id)
    FROM _schemes s;
    
    -- Return statistics for ALL schemes
    RETURN QUERY
    SELECT 
        s._id as scheme_id,
        COUNT(c._structure_id) as structures_count,
        s._name::text as scheme_name,
        s._structure_hash as structure_hash
    FROM _schemes s
    LEFT JOIN _scheme_metadata_cache c ON c._scheme_id = s._id
    GROUP BY s._id, s._name, s._structure_hash
    ORDER BY s._id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION warmup_all_metadata_caches() IS 
'Warms up metadata cache for ALL schemes (including schemes without _structure_hash).
Recommended to call:
  1. On application startup
  2. After PostgreSQL crash (UNLOGGED TABLE is cleared)
  3. After schema migrations

Returns statistics: scheme_id → number of structures for all schemes.

Usage:
  SELECT * FROM warmup_all_metadata_caches();
  
UPDATED: Now warms up ALL schemes, not only those with _structure_hash IS NOT NULL.
This eliminates auto-filling of cache on every query to v_objects_json.
';

-- 8️⃣ Cache consistency check function
CREATE OR REPLACE FUNCTION check_metadata_cache_consistency()
RETURNS TABLE(
    scheme_id bigint, 
    scheme_name text,
    cached_count bigint, 
    actual_count bigint, 
    is_consistent boolean
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s._id as scheme_id,
        s._name::text as scheme_name,
        COALESCE(cache_stats.cached_count, 0) as cached_count,
        COALESCE(actual_stats.actual_count, 0) as actual_count,
        (COALESCE(cache_stats.cached_count, 0) = COALESCE(actual_stats.actual_count, 0)) as is_consistent
    FROM _schemes s
    LEFT JOIN (
        SELECT _scheme_id, COUNT(*) as cached_count
        FROM _scheme_metadata_cache
        GROUP BY _scheme_id
    ) cache_stats ON cache_stats._scheme_id = s._id
    LEFT JOIN (
        SELECT _id_scheme, COUNT(*) as actual_count
        FROM _structures
        GROUP BY _id_scheme
    ) actual_stats ON actual_stats._id_scheme = s._id
    ORDER BY s._id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION check_metadata_cache_consistency() IS 
'Checks metadata cache consistency.
Returns discrepancies between cache and actual data.

Usage:
  SELECT * FROM check_metadata_cache_consistency() WHERE NOT is_consistent;
';

COMMENT ON TABLE _scheme_metadata_cache IS 
'Metadata cache for schemes to optimize queries.
Automatically synchronized via triggers on _schemes._structure_hash.
Note: Use UNLOGGED TABLE for better performance if replication is not needed (cleared after crash).

Invalidation strategy:
  ✅ _schemes._structure_hash changed → rebuild cache for scheme (trigger)
  ✅ _schemes deleted → delete scheme cache (trigger)
  ✅ _types changed → clear entire cache (TRUNCATE trigger)
  
Cache warmup:
  ✅ warmup_all_metadata_caches() warms up ALL schemes (including those without _structure_hash)
  ✅ Auto-filling in functions (get_object_json, get_facets) as fallback
';

-- ============================================================
-- READY! Now use in queries:
-- 
-- INSTEAD OF:
--   FROM _structures s 
--   JOIN _types t ON t._id = s._id_type
--   WHERE s._id_scheme = object_scheme_id
--
-- USE:
--   FROM _scheme_metadata_cache c
--   WHERE c._scheme_id = object_scheme_id
-- 
-- AVAILABLE FIELDS:
--   _scheme_id, _structure_id, _parent_structure_id, _id_override
--   _name, _alias
--   _type_id, _list_id, type_name, db_type, type_semantic
--   _scheme_type, scheme_type_name (scheme type: Class/Array/Dictionary/JsonDocument/XDocument)
--   _order, _collection_type, collection_type_name (collection type: Array/Dictionary/NULL)
--   _key_type, key_type_name (key type for Dictionary)
--   _readonly, _allow_not_null, _is_compress, _store_null
--   _default_value, _default_editor
--
-- COLLECTION CHECK (instead of _is_array):
--   _collection_type IS NOT NULL = this is a collection (array or dictionary)
--   _collection_type IS NULL = not a collection
-- ============================================================

-- ===== migrate_structure_type.sql =====
-- ============================================================
-- HELPER FUNCTION: get_value_column
-- Returns the _values column name for a given type name
-- ============================================================

-- DROP FUNCTION IF EXISTS public.get_value_column(text);

CREATE OR REPLACE FUNCTION public.get_value_column(p_type_name text)
RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
BEGIN
    RETURN CASE LOWER(p_type_name)
        WHEN 'string' THEN '_string'
        WHEN 'text' THEN '_string'
        WHEN 'mimetype' THEN '_string'
        WHEN 'filepath' THEN '_string'
        WHEN 'filename' THEN '_string'
        WHEN 'long' THEN '_long'
        WHEN 'int' THEN '_long'
        WHEN 'short' THEN '_long'
        WHEN 'byte' THEN '_long'
        WHEN 'object' THEN '_object'
        WHEN 'double' THEN '_double'
        WHEN 'float' THEN '_double'
        WHEN 'boolean' THEN '_boolean'
        WHEN 'datetime' THEN '_datetimeoffset'
        WHEN 'datetimeoffset' THEN '_datetimeoffset'
        WHEN 'dateonly' THEN '_datetimeoffset'
        WHEN 'timeonly' THEN '_datetimeoffset'
        WHEN 'timespan' THEN '_long'
        WHEN 'guid' THEN '_guid'
        WHEN 'bytearray' THEN '_bytearray'
        WHEN 'numeric' THEN '_numeric'
        WHEN 'listitem' THEN '_listitem'
        ELSE NULL
    END;
END;
$BODY$;

COMMENT ON FUNCTION public.get_value_column(text)
    IS 'Returns the _values column name for a given REDB type name.
Examples:
  SELECT get_value_column(''String'');  -- returns ''_string''
  SELECT get_value_column(''Long'');    -- returns ''_long''';

-- ============================================================
-- FUNCTION: public.migrate_structure_type(bigint, text, text, boolean)
-- ============================================================

-- DROP FUNCTION IF EXISTS public.migrate_structure_type(bigint, text, text, boolean);

CREATE OR REPLACE FUNCTION public.migrate_structure_type(
    p_structure_id bigint,
    p_old_type_name text,
    p_new_type_name text,
    p_dry_run boolean DEFAULT false)
    RETURNS TABLE(affected_rows integer, success_count integer, error_count integer, errors text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
    v_source_col TEXT;
    v_target_col TEXT;
    v_affected_rows INT := 0;
    v_success_count INT := 0;
    v_has_collision BOOLEAN;
    v_conversion_sql TEXT;
BEGIN
    -- Get column names
    v_source_col := get_value_column(p_old_type_name);
    v_target_col := get_value_column(p_new_type_name);
    
    -- Type validation
    IF v_source_col IS NULL THEN
        RETURN QUERY SELECT 0, 0, 0, format('Unknown source type: %s', p_old_type_name);
        RETURN;
    END IF;
    
    IF v_target_col IS NULL THEN
        RETURN QUERY SELECT 0, 0, 0, format('Unknown target type: %s', p_new_type_name);
        RETURN;
    END IF;
    
    -- Same columns - migration not needed (e.g., Int->Long both in _Long)
    IF v_source_col = v_target_col THEN
        RETURN QUERY SELECT 0, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- Check if structure exists
    IF NOT EXISTS (SELECT 1 FROM _structures WHERE _id = p_structure_id) THEN
        RETURN QUERY SELECT 0, 0, 0, format('Structure %s not found', p_structure_id);
        RETURN;
    END IF;
    
    -- Count affected rows
    EXECUTE format(
        'SELECT COUNT(*) FROM _values WHERE _id_structure = $1 AND %I IS NOT NULL',
        v_source_col
    ) INTO v_affected_rows USING p_structure_id;
    
    -- Dry run - only counting
    IF p_dry_run THEN
        RETURN QUERY SELECT v_affected_rows, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- ========================================
    -- COLLISION CHECK (key point!)
    -- If target is filled and source is empty - data was already migrated manually
    -- ========================================
    EXECUTE format(
        'SELECT EXISTS(
            SELECT 1 FROM _values 
            WHERE _id_structure = $1 
              AND %I IS NOT NULL
              AND %I IS NULL
            LIMIT 1
        )', v_target_col, v_source_col
    ) INTO v_has_collision USING p_structure_id;
    
    IF v_has_collision THEN
        RETURN QUERY SELECT v_affected_rows, 0, v_affected_rows, 
            format('TYPE_MIGRATION_COLLISION: Data already in %s but _id_type = %s. Fix manually: UPDATE _structures SET _id_type = (SELECT _id FROM _types WHERE _name = ''%s'') WHERE _id = %s',
                v_target_col, p_old_type_name, p_new_type_name, p_structure_id);
        RETURN;
    END IF;
    
    -- No data to migrate
    IF v_affected_rows = 0 THEN
        RETURN QUERY SELECT 0, 0, 0, NULL::TEXT;
        RETURN;
    END IF;
    
    -- ========================================
    -- CONVERSION MATRIX
    -- ========================================
    v_conversion_sql := NULL;
    
    -- STRING -> *
    IF v_source_col = '_string' THEN
        IF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+\.?[0-9]*$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL AND %I ~ ''^-?[0-9]+\.?[0-9]*$''',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_boolean' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN LOWER(%I) IN (''true'', ''1'', ''yes'', ''t'', ''y'') THEN TRUE WHEN LOWER(%I) IN (''false'', ''0'', ''no'', ''f'', ''n'') THEN FALSE ELSE NULL END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_datetimeoffset' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::timestamptz, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_guid' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::uuid, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- LONG -> *
    ELSIF v_source_col = '_long' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_boolean' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = (%I != 0), %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_datetimeoffset' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = to_timestamp(%I), %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- DOUBLE -> *
    ELSIF v_source_col = '_double' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = ROUND(%I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_numeric' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::numeric, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- NUMERIC -> *
    ELSIF v_source_col = '_numeric' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = ROUND(%I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_double' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::double precision, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- BOOLEAN -> *
    ELSIF v_source_col = '_boolean' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN %I THEN ''true'' ELSE ''false'' END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = CASE WHEN %I THEN 1 ELSE 0 END, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- DATETIMEOFFSET -> *
    ELSIF v_source_col = '_datetimeoffset' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        ELSIF v_target_col = '_long' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = EXTRACT(EPOCH FROM %I)::bigint, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    
    -- GUID -> *
    ELSIF v_source_col = '_guid' THEN
        IF v_target_col = '_string' THEN
            v_conversion_sql := format(
                'UPDATE _values SET %I = %I::text, %I = NULL WHERE _id_structure = $1 AND %I IS NOT NULL',
                v_target_col, v_source_col, v_source_col, v_source_col);
        END IF;
    END IF;
    
    -- Conversion not supported
    IF v_conversion_sql IS NULL THEN
        RETURN QUERY SELECT v_affected_rows, 0, v_affected_rows, 
            format('Conversion %s -> %s not supported', p_old_type_name, p_new_type_name);
        RETURN;
    END IF;
    
    -- Execute migration
    EXECUTE v_conversion_sql USING p_structure_id;
    GET DIAGNOSTICS v_success_count = ROW_COUNT;
    
    RETURN QUERY SELECT v_affected_rows, v_success_count, v_affected_rows - v_success_count, NULL::TEXT;
END;
$BODY$;

ALTER FUNCTION public.migrate_structure_type(bigint, text, text, boolean)
    OWNER TO postgres;

COMMENT ON FUNCTION public.migrate_structure_type(bigint, text, text, boolean)
    IS 'Atomic data migration when changing structure type.
Parameters:
  p_structure_id - structure ID in _structures
  p_old_type_name - old type name (String, Long, Double, etc.)
  p_new_type_name - new type name
  p_dry_run - TRUE for test run without changes

Returns:
  affected_rows - total rows with data
  success_count - successfully migrated
  error_count - failed to migrate
  errors - error text (NULL if success)

Returns TYPE_MIGRATION_COLLISION error if data is already in target column.

Examples:
  SELECT * FROM migrate_structure_type(12345, ''String'', ''Long'', TRUE);  -- dry run
  SELECT * FROM migrate_structure_type(12345, ''String'', ''Long'', FALSE); -- execute';

-- ===== migration_drop_deleted_objects.sql =====
-- =====================================================
-- MIGRATION: Drop legacy _deleted_objects archive system
-- Part of Background Deletion System migration
-- =====================================================
-- This script removes the old _deleted_objects table and its trigger.
-- The new soft-delete system uses @@__deleted scheme and trash containers.
-- Run this script on existing databases to migrate to new system.
-- =====================================================

-- 1. Drop the trigger first (depends on function)
DROP TRIGGER IF EXISTS TR__objects__deleted_objects ON _objects;

-- 2. Drop the archive function
DROP FUNCTION IF EXISTS ftr__objects__deleted_objects();

-- 3. Drop the archive table
DROP TABLE IF EXISTS _deleted_objects;

-- 4. Add the @@__deleted scheme if not exists
INSERT INTO _schemes (_id, _name, _alias, _type) 
VALUES (-10, '@@__deleted', 'Deleted Objects', -9223372036854775703)
ON CONFLICT (_id) DO NOTHING;

-- 5. Update validate_scheme_name function to allow @@ prefix
-- (This is done by re-creating the function - see redbPostgre.sql)

-- =====================================================
-- VERIFICATION
-- =====================================================
-- After running this script, verify:
-- SELECT * FROM _schemes WHERE _id = -10;
-- Expected: @@__deleted scheme exists
-- 
-- SELECT * FROM pg_trigger WHERE tgname = 'tr__objects__deleted_objects';
-- Expected: No rows (trigger removed)
-- =====================================================



-- ===== redb_json_objects.sql =====
DROP VIEW IF EXISTS v_objects_json;
DROP FUNCTION IF EXISTS get_object_json;
DROP FUNCTION IF EXISTS build_listitem_jsonb;
-- Drop old signatures with jsonb parameter (before optimization to _values[])
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, jsonb, integer, integer, bigint);
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, jsonb, integer, text, bigint);
-- Drop new signature with _values[] array (if exists)
DROP FUNCTION IF EXISTS build_hierarchical_properties_optimized(bigint, bigint, bigint, _values[], integer, text, bigint);

-- ===== HELPER: Build ListItem JSON (DRY - used in multiple places) =====
CREATE OR REPLACE FUNCTION build_listitem_jsonb(
    listitem_id bigint,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 10
STABLE
AS $BODY$
BEGIN
    IF listitem_id IS NULL THEN
        RETURN NULL;
    END IF;
    
    RETURN (SELECT jsonb_build_object(
        'id', li._id,
        'idList', li._id_list,
        'value', li._value,
        'alias', li._alias,
        'object', CASE 
            WHEN li._id_object IS NOT NULL THEN
                get_object_json(li._id_object, GREATEST(0, max_depth - 1))
            ELSE NULL 
        END
    )
    FROM _list_items li
    WHERE li._id = listitem_id);
END;
$BODY$;

-- ===== OPTIMIZED FUNCTIONS =====

-- Optimized function for building hierarchical properties with preloaded values array
-- 🚀 OPTIMIZATION: Uses _values[] array instead of jsonb - all data in memory, no repeated table queries
CREATE OR REPLACE FUNCTION build_hierarchical_properties_optimized(
    object_id bigint,
    parent_structure_id bigint,
    object_scheme_id bigint,
    all_values _values[],  -- 🚀 Array of _values records instead of jsonb
    max_depth integer DEFAULT 10,
    array_index text DEFAULT NULL, -- Text to support Dictionary string keys
    parent_value_id bigint DEFAULT NULL -- ID of parent element for nested arrays
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb := '{}'::jsonb;
    structure_record RECORD;
    current_value_record _values;  -- 🚀 Typed record instead of jsonb
    field_value jsonb;
    base_array_value_id bigint; -- ID of base array record for recursion
BEGIN
    -- Protection against infinite recursion for Class fields (hierarchical structures)
    -- IMPORTANT: This function is for Class fields (Address.Street, Contacts[].Email)
    -- max_depth is NOT checked here - Class fields are always loaded completely!
    -- max_depth is controlled only in get_object_json() for Object references (_RObject)
    IF max_depth < -100 THEN
        -- Protection against anomalous recursion (practically impossible)
        RETURN jsonb_build_object('error', 'Max recursion depth reached for hierarchical fields');
    END IF;
    
    -- 🔥 AUTOMATIC CACHE CHECK AND POPULATION
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = object_scheme_id LIMIT 1) THEN
        -- Cache is empty for this scheme → populate automatically
        PERFORM sync_metadata_cache_for_scheme(object_scheme_id);
        -- Auto-population without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- Collect all structures for given parent_structure_id (NO JOIN with _values!)
    -- 🚀 OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    FOR structure_record IN
        SELECT 
            c._structure_id as structure_id,
            c._name as field_name,
            c._collection_type as collection_type,  -- NULL = scalar, Array ID = array, Dictionary ID = dictionary
            c._collection_type = -9223372036854775668 as _is_array,  -- Array type ID
            c._collection_type = -9223372036854775667 as _is_dictionary,  -- Dictionary type ID
            c.type_name,
            c.db_type,
            c.type_semantic
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = object_scheme_id
          AND ((parent_structure_id IS NULL AND c._parent_structure_id IS NULL) 
               OR (parent_structure_id IS NOT NULL AND c._parent_structure_id = parent_structure_id))
        ORDER BY c._order, c._structure_id
    LOOP
        -- 🚀 OPTIMIZATION: Search value from preloaded array using unnest()
        IF parent_value_id IS NOT NULL THEN
            -- For nested fields inside array/dictionary elements - find by _array_parent_id
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_parent_id = parent_value_id
            LIMIT 1;
        ELSIF array_index IS NOT NULL THEN
            -- For array/dictionary elements - find value with specific array_index
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_index = array_index
            LIMIT 1;
        ELSE
            -- For regular fields or root fields - find first match by structure_id
            SELECT v.* INTO current_value_record
            FROM unnest(all_values) AS v
            WHERE v._id_structure = structure_record.structure_id
              AND v._array_index IS NULL
            LIMIT 1;
        END IF;
        
        -- ✅ Get ID of base array record for recursion
        base_array_value_id := NULL; -- Reset before each field
        
        IF structure_record._is_array = true OR structure_record._is_dictionary = true THEN
            -- Find base array/dictionary record from preloaded data
            IF parent_value_id IS NULL THEN
                -- Root array/dictionary: _array_parent_id must be NULL
                SELECT v._id INTO base_array_value_id
                FROM unnest(all_values) AS v
                WHERE v._id_structure = structure_record.structure_id
                  AND v._array_index IS NULL
                  AND v._array_parent_id IS NULL
                LIMIT 1;
            ELSE
                -- Nested array/dictionary: _array_parent_id must match parent_value_id
                SELECT v._id INTO base_array_value_id
                FROM unnest(all_values) AS v
                WHERE v._id_structure = structure_record.structure_id
                  AND v._array_index IS NULL
                  AND v._array_parent_id = parent_value_id
                LIMIT 1;
            END IF;
        END IF;
        
        -- Determine field value based on its type and preloaded data
        field_value := CASE 
            -- If this is an array - process relationally through _array_index
            WHEN structure_record._is_array = true THEN
                CASE 
                    -- No head record = array property is null (not empty [])
                    WHEN base_array_value_id IS NULL THEN NULL
                    -- Array of Class fields - build from relational data recursively
                    WHEN structure_record.type_semantic = 'Object' THEN
                        (
                            WITH array_elements AS (
                                -- Find all array elements with their indices from preloaded data
                                SELECT 
                                    v._array_index,
                                    -- Safe numeric sorting: only for numeric indices (Array), text keys (Dictionary) sort as 0
                                    CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END as array_index_int,
                                    v._id as element_value_id,
                                    v._array_parent_id,
                                    build_hierarchical_properties_optimized(
                                        object_id, 
                                        structure_record.structure_id, 
                                        object_scheme_id, 
                                        all_values,  -- 🚀 Pass array, not jsonb
                                        max_depth,
                                        v._array_index,
                                        v._id
                                    ) as element_json
                                FROM unnest(all_values) AS v  -- 🚀 From memory array
                                WHERE v._id_structure = structure_record.structure_id
                                  AND v._array_index IS NOT NULL
                                  AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                                ORDER BY array_index_int, v._array_index  -- numeric first, then text
                            )
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '[]'::jsonb  -- Empty array = []
                                ELSE jsonb_agg(element_json ORDER BY array_index_int)
                            END
                            FROM array_elements
                        )
                    -- Arrays of primitive types (String, Long, Boolean, etc.) - relationally
                    ELSE
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '[]'::jsonb  -- Empty array = []
                                ELSE jsonb_agg(
                                CASE 
                                    -- Object references (_RObject) - check by type_semantic
                                    WHEN structure_record.type_semantic = '_RObject' AND v._Object IS NOT NULL THEN
                                        get_object_json(v._Object, max_depth - 1)
                                    WHEN structure_record.db_type = 'String' THEN to_jsonb(v._String)
                                    WHEN structure_record.db_type = 'Long' THEN 
                                        -- If _ListItem is filled, process as ListItem (for backward compatibility)
                                        CASE 
                                            WHEN v._ListItem IS NOT NULL THEN
                                                build_listitem_jsonb(v._ListItem, max_depth)
                                            ELSE to_jsonb(v._Long)
                                        END
                                    WHEN structure_record.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                    WHEN structure_record.db_type = 'Double' THEN to_jsonb(v._Double)
                                    WHEN structure_record.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                    WHEN structure_record.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                    WHEN structure_record.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                    WHEN structure_record.db_type = 'ListItem' THEN
                                        build_listitem_jsonb(v._ListItem, max_depth)
                                    WHEN structure_record.db_type = 'ByteArray' THEN 
                                        to_jsonb(encode(decode(v._ByteArray::text, 'base64'), 'base64'))
                                    ELSE NULL
                                -- Safe sorting: numeric for Array, text for Dictionary
                                END ORDER BY CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END, v._array_index
                                )
                            END
                            FROM unnest(all_values) AS v  -- 🚀 From memory array
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                END
            
            -- Dictionary<K,V> fields - return JSON object with keys
            WHEN structure_record._is_dictionary = true THEN
                CASE 
                    -- No head record = dictionary property is null (not empty {})
                    WHEN base_array_value_id IS NULL THEN NULL
                    -- Dictionary of RedbObject references (_RObject)
                    WHEN structure_record.type_semantic = '_RObject' THEN
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    CASE 
                                        WHEN v._Object IS NOT NULL THEN get_object_json(v._Object, max_depth - 1)
                                        ELSE NULL
                                    END
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                    -- Dictionary of Class fields
                    WHEN structure_record.type_semantic = 'Object' THEN
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    build_hierarchical_properties_optimized(
                                        object_id, 
                                        structure_record.structure_id, 
                                        object_scheme_id, 
                                        all_values,
                                        max_depth,
                                        NULL,  -- array_index = NULL for nested Class fields!
                                        v._id  -- parent_value_id = element record ID
                                    )
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                    -- Dictionary of primitive types
                    ELSE
                        (
                            SELECT CASE 
                                WHEN COUNT(*) = 0 THEN '{}'::jsonb  -- Empty dictionary = {}
                                ELSE jsonb_object_agg(
                                    v._array_index,  -- Key as JSON key
                                    CASE 
                                        WHEN structure_record.db_type = 'String' THEN to_jsonb(v._String)
                                        WHEN structure_record.db_type = 'Long' THEN to_jsonb(v._Long)
                                        WHEN structure_record.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                        WHEN structure_record.db_type = 'Double' THEN to_jsonb(v._Double)
                                        WHEN structure_record.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                        WHEN structure_record.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                        WHEN structure_record.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                        ELSE NULL
                                    END
                                )
                            END
                            FROM unnest(all_values) AS v
                            WHERE v._id_structure = structure_record.structure_id
                              AND v._array_index IS NOT NULL
                              AND (base_array_value_id IS NULL OR v._array_parent_id = base_array_value_id)
                        )
                END
            
            -- Regular fields (not arrays or dictionaries)
            -- Object reference to another object
            WHEN structure_record.type_name = 'Object' AND structure_record.type_semantic = '_RObject' THEN
                CASE 
                    WHEN current_value_record._Object IS NOT NULL THEN 
                        get_object_json(current_value_record._Object, max_depth - 1)
                    ELSE NULL
                END
            
            -- Class field with hierarchical child fields  
            WHEN structure_record.type_semantic = 'Object' THEN
                CASE 
                    WHEN current_value_record._Guid IS NULL THEN 
                        NULL  -- Class field is truly NULL - don't build object
                    ELSE
                        build_hierarchical_properties_optimized(
                            object_id, 
                            structure_record.structure_id, 
                            object_scheme_id, 
                            all_values,  -- 🚀 Pass array, not jsonb
                            max_depth,  -- Don't decrease max_depth for Class fields!
                            NULL,  -- array_index = NULL for nested Class fields
                            current_value_record._id  -- IMPORTANT: pass ID of current Class field record!
                        )
                END
                
            -- Primitive types - direct access to typed record fields (no JSON parsing!)
            -- Check _id IS NOT NULL to verify record was found (prevents jsonb null instead of SQL NULL)
            WHEN structure_record.db_type = 'String' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._String) ELSE NULL END
            WHEN structure_record.db_type = 'Long' THEN 
                -- If _ListItem is filled, process as ListItem (for backward compatibility)
                CASE 
                    WHEN current_value_record._ListItem IS NOT NULL THEN 
                        -- This is ListItem saved in old schema with db_type=Long
                        build_listitem_jsonb(current_value_record._ListItem, max_depth)
                    WHEN current_value_record._id IS NOT NULL THEN 
                        to_jsonb(current_value_record._Long)
                    ELSE NULL 
                END
            WHEN structure_record.db_type = 'Guid' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Guid) ELSE NULL END
            WHEN structure_record.db_type = 'Double' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Double) ELSE NULL END
            WHEN structure_record.db_type = 'Numeric' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Numeric) ELSE NULL END
            WHEN structure_record.db_type = 'DateTimeOffset' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._DateTimeOffset) ELSE NULL END
            WHEN structure_record.db_type = 'Boolean' THEN 
                CASE WHEN current_value_record._id IS NOT NULL THEN to_jsonb(current_value_record._Boolean) ELSE NULL END
            WHEN structure_record.db_type = 'ListItem' OR current_value_record._ListItem IS NOT NULL THEN 
                -- Process as ListItem if db_type=ListItem OR if _ListItem is filled (backward compatibility)
                CASE 
                    WHEN current_value_record._ListItem IS NOT NULL THEN 
                        build_listitem_jsonb(current_value_record._ListItem, max_depth)
                    ELSE NULL
                END
            WHEN structure_record.db_type = 'ByteArray' THEN 
                CASE 
                    WHEN current_value_record._ByteArray IS NOT NULL THEN 
                        to_jsonb(encode(decode(current_value_record._ByteArray::text, 'base64'), 'base64'))
                    ELSE NULL
                END
            ELSE NULL
        END;
        -- Add field to result only if value is not NULL
        IF field_value IS NOT NULL THEN
            result_json := result_json || jsonb_build_object(structure_record.field_name, field_value);
        END IF;
        
    END LOOP;
    
    RETURN result_json;
END;
$BODY$;

-- OPTIMIZED function for getting object in JSON format with preloaded values array
-- 🚀 OPTIMIZATION: Loads all _values into typed array - no JSON parsing overhead
CREATE OR REPLACE FUNCTION get_object_json(
    object_id bigint,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb;
    object_exists boolean;
    base_info jsonb;
    properties_info jsonb;
    object_scheme_id bigint;
    all_values _values[];  -- 🚀 Typed array instead of jsonb
BEGIN
    -- Check if object exists - return NULL if not found
    SELECT EXISTS(SELECT 1 FROM _objects WHERE _id = object_id) INTO object_exists;
    
    IF NOT object_exists THEN
        RETURN NULL;
    END IF;
    
    -- Check recursion depth
    IF max_depth <= 0 THEN
        -- max_depth = 0: return ONLY base fields WITHOUT properties
        SELECT jsonb_build_object(
            'id', o._id,
            'name', o._name,
            'scheme_id', o._id_scheme,
            'scheme_name', sc._name,
            'parent_id', o._id_parent,
            'owner_id', o._id_owner,
            'who_change_id', o._id_who_change,
            'date_create', o._date_create,
            'date_modify', o._date_modify,
            'date_begin', o._date_begin,
            'date_complete', o._date_complete,
            'key', o._key,
            'value_long', o._value_long,
            'value_string', o._value_string,
            'value_guid', o._value_guid,
            'note', o._note,
            'value_bool', o._value_bool,
            'value_double', o._value_double,
            'value_numeric', o._value_numeric,
            'value_datetime', o._value_datetime,
            'value_bytes', o._value_bytes,
            'hash', o._hash
        ) INTO result_json
        FROM _objects o
        JOIN _schemes sc ON sc._id = o._id_scheme
        WHERE o._id = object_id;
        
        RETURN result_json;
    END IF;
    
    -- Collect base object info + get scheme_id
    SELECT jsonb_build_object(
        'id', o._id,
        'name', o._name,
        'scheme_id', o._id_scheme,
        'scheme_name', sc._name,
        'parent_id', o._id_parent,
        'owner_id', o._id_owner,
        'who_change_id', o._id_who_change,
        'date_create', o._date_create,
        'date_modify', o._date_modify,
        'date_begin', o._date_begin,
        'date_complete', o._date_complete,
        'key', o._key,
        'value_long', o._value_long,
        'value_string', o._value_string,
        'value_guid', o._value_guid,
        'note', o._note,
        'value_bool', o._value_bool,
        'value_double', o._value_double,
        'value_numeric', o._value_numeric,
        'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes,
        'hash', o._hash
    ), o._id_scheme
    INTO base_info, object_scheme_id
    FROM _objects o
    JOIN _schemes sc ON sc._id = o._id_scheme
    WHERE o._id = object_id;
    
    -- 🚀 OPTIMIZATION: Load ALL values into typed array - single query, no JSON overhead
    SELECT array_agg(v) INTO all_values
    FROM _values v
    WHERE v._id_object = object_id;
    
    -- No _values at all = properties is null (not fake empty {})
    IF all_values IS NULL THEN
        result_json := base_info || jsonb_build_object('properties', NULL);
        RETURN result_json;
    END IF;
    
    -- Use optimized function with preloaded values array
    SELECT build_hierarchical_properties_optimized(
        object_id, 
        NULL, 
        object_scheme_id, 
        COALESCE(all_values, ARRAY[]::_values[]),  -- 🚀 Pass typed array
        max_depth,
        NULL, -- array_index = NULL for root fields
        NULL  -- parent_value_id = NULL for root level
    ) INTO properties_info;
    
    -- Combine base info with properties
    result_json := base_info || jsonb_build_object('properties', COALESCE(properties_info, '{}'::jsonb));
    
    RETURN result_json;
END;
$BODY$;

-- BULK-OPTIMIZED VIEW for batch object retrieval in JSON format  
-- CREATE OR REPLACE VIEW v_objects_json AS
-- WITH 
-- -- Stage 1: BULK load values (optimal - GROUP BY only by ID)
-- all_values AS (
--     SELECT 
--         o._id,
--         COALESCE(
--             jsonb_object_agg(
--                 v._id_structure::text, 
--                 jsonb_build_object(
--                     '_String', v._String,
--                     '_Long', v._Long,
--                     '_Guid', v._Guid,
--                     '_Double', v._Double,
--                     '_DateTimeOffset', v._DateTimeOffset,
--                     '_Boolean', v._Boolean,
--                     '_ByteArray', v._ByteArray,
--                     '_array_parent_id', v._array_parent_id,
--                     '_array_index', v._array_index
--                 )
--             ) FILTER (WHERE v._id IS NOT NULL),
--             '{}'::jsonb
--         ) as all_values_json
--     FROM _objects o
--     LEFT JOIN _values v ON v._id_object = o._id
--     GROUP BY o._id  -- GROUP BY only by ID (fast!)
-- ),
-- -- Stage 2: Join with _objects fields and build JSON
-- objects_with_json AS (
--     SELECT 
--         o.*,  -- All _objects fields with single asterisk (efficient)
--         -- Full object JSON with properties
--         jsonb_build_object(
--             'id', o._id,
--             'name', o._name,
--             'scheme_id', o._id_scheme,
--             'scheme_name', s._name,
--             'parent_id', o._id_parent,
--             'owner_id', o._id_owner,
--             'who_change_id', o._id_who_change,
--             'date_create', o._date_create,
--             'date_modify', o._date_modify,
--             'date_begin', o._date_begin,
--             'date_complete', o._date_complete,
--             'key', o._key,
--             'value_long', o._value_long,
--             'value_string', o._value_string,
--             'value_guid', o._value_guid,
--             'note', o._note,
--             'value_bool', o._value_bool,
--             'hash', o._hash,
--             'properties', 
--             build_hierarchical_properties_optimized(
--                 o._id, 
--                 NULL, 
--                 o._id_scheme, 
--                 av.all_values_json,  -- Use preloaded data
--                 10,
--                 NULL -- array_index = NULL for root fields
--             )
--         ) as object_json
--     FROM _objects o
--     JOIN _schemes s ON s._id = o._id_scheme  
--     JOIN all_values av ON av._id = o._id  -- JOIN with preloaded values
-- )
-- SELECT * FROM objects_with_json ORDER BY _id;

-- -- Comments for OPTIMIZED functions and VIEWs for object retrieval
-- COMMENT ON VIEW v_objects_json IS 'MAXIMALLY OPTIMIZED VIEW for object retrieval. Two-stage architecture: 1) BULK _values aggregation with GROUP BY only by _id (fast!) 2) JOIN ready data with _objects via o.* (efficient). Returns ALL original _objects fields as columns PLUS full JSON with properties. Avoids heavy GROUP BY on 17 fields. Perfect for integration and API. Supports hierarchical Class fields.';

COMMENT ON FUNCTION build_hierarchical_properties_optimized(bigint, bigint, bigint, _values[], integer, text, bigint) IS 'Optimized function for recursive building of hierarchical JSON structure with preloaded _values[] array.
OPTIMIZATION: Uses typed _values[] array instead of jsonb - all data in memory, no repeated table queries!
IMPORTANT: max_depth is NOT decreased for Class fields - they are always loaded completely as part of object structure.
max_depth controls ONLY the depth of Object references (_RObject) in get_object_json().
Supports:
Relational arrays of Class fields
Nested arrays (arrays inside array elements) via array_index and parent_value_id
NO JOIN with _values in loop - uses unnest() from memory array!
5-10x faster for objects with arrays due to zero table queries.';

COMMENT ON FUNCTION get_object_json(bigint, integer) IS 'OPTIMIZED function for getting object in JSON format with SMART recursion depth:
max_depth = 0: only base fields WITHOUT properties (fast)
max_depth >= 1: base fields + properties
OPTIMIZATION: Loads ALL _values into typed array - single query, no JSON parsing overhead!
IMPORTANT: max_depth controls depth of Object references (_RObject):
  - Object references are called with max_depth-1
  - Class fields (Address, Contacts) are ALWAYS loaded COMPLETELY (max_depth not decreased)
  - ListItem._id_object also called with max_depth-1 (like regular Object references)
Supports:
Hierarchical Class fields (Address.Street, Contacts[].Email) - always fully
Object references (_RObject) - controlled depth via max_depth
ListItem with _id_object (base fields of linked object)
Relational arrays of all types
Optimal for objects with 10+ fields and arrays.';

-- ============================================================================
-- BULK get_objects_json: single-pass batch loader for many object IDs.
-- ----------------------------------------------------------------------------
-- Replaces the legacy "unnest($1::bigint[]) + get_object_json(id, N)" pattern
-- used by C# LazyPropsLoader.LoadPropsForManyAsync. The legacy pattern made
-- one full plpgsql invocation per object (EXISTS + base SELECT + _values
-- SELECT, then recursion), producing 3*N round-trips. This bulk variant does
-- one scan of _objects, one scan of _values grouped by _id_object, then a
-- single LEFT JOIN that materializes hierarchical properties per row inside
-- one execution plan.
-- ============================================================================
DROP FUNCTION IF EXISTS get_objects_json(bigint[], integer) CASCADE;

CREATE OR REPLACE FUNCTION get_objects_json(
    p_ids bigint[],
    p_max_depth integer DEFAULT 10
) RETURNS TABLE("Id" bigint, "JsonData" text)
LANGUAGE 'sql'
COST 200
VOLATILE
AS $BODY$
    WITH bases AS (
        SELECT
            o._id,
            o._id_scheme,
            jsonb_build_object(
                'id', o._id,
                'name', o._name,
                'scheme_id', o._id_scheme,
                'scheme_name', sc._name,
                'parent_id', o._id_parent,
                'owner_id', o._id_owner,
                'who_change_id', o._id_who_change,
                'date_create', o._date_create,
                'date_modify', o._date_modify,
                'date_begin', o._date_begin,
                'date_complete', o._date_complete,
                'key', o._key,
                'value_long', o._value_long,
                'value_string', o._value_string,
                'value_guid', o._value_guid,
                'note', o._note,
                'value_bool', o._value_bool,
                'value_double', o._value_double,
                'value_numeric', o._value_numeric,
                'value_datetime', o._value_datetime,
                'value_bytes', o._value_bytes,
                'hash', o._hash
            ) AS base
        FROM _objects o
        JOIN _schemes sc ON sc._id = o._id_scheme
        WHERE o._id = ANY(p_ids)
    ),
    grouped_values AS (
        SELECT v._id_object AS oid, array_agg(v) AS vs
        FROM _values v
        WHERE v._id_object = ANY(p_ids)
        GROUP BY v._id_object
    )
    SELECT
        b._id AS "Id",
        (
            b.base || jsonb_build_object(
                'properties',
                CASE
                    WHEN p_max_depth <= 0 THEN NULL
                    WHEN g.vs IS NULL THEN NULL
                    ELSE COALESCE(
                        build_hierarchical_properties_optimized(
                            b._id, NULL, b._id_scheme, g.vs, p_max_depth, NULL, NULL
                        ),
                        '{}'::jsonb
                    )
                END
            )
        )::text AS "JsonData"
    FROM bases b
    LEFT JOIN grouped_values g ON g.oid = b._id;
$BODY$;

COMMENT ON FUNCTION get_objects_json(bigint[], integer) IS
'Bulk JSON materializer for a set of object IDs. Single-plan replacement for
"SELECT get_object_json(id, N) FROM unnest($1::bigint[])". Aggregates _values
once with GROUP BY _id_object, joins to _objects + _schemes, then calls
build_hierarchical_properties_optimized per row from in-memory array. Returns
rows compatible with the C# ObjectJsonResult DTO ("Id", "JsonData"). Honors
max_depth = 0 (base fields only, properties=null) and absent _values rows
(properties=null) the same way as get_object_json.';

-- ===== SIMPLE VIEW FOR OBJECTS WITH JSON =====

-- Drop existing view if exists
DROP VIEW IF EXISTS v_objects_json;

-- COMMENTED OUT: v_objects_json is inefficient for bulk operations (calls get_object_json for each row)
-- For LoadAsync direct SELECT from _objects + LoadPropsForManyAsync (LAZY) or get_object_json batch via unnest (EAGER) is used

-- -- Simple view: all _objects fields + JSON via get_object_json
-- CREATE VIEW v_objects_json AS
-- SELECT 
--     o.*,  -- All _objects fields as is
--     get_object_json(o._id, 10) as object_json  -- JSON representation of object
-- FROM _objects o;
-- COMMENT ON VIEW v_objects_json IS 'Simple view for object retrieval: all _objects fields + full JSON via get_object_json. Convenient for viewing and debugging.';


-- ===== redb_migrations.sql =====
-- =====================================================
-- REDB Pro: Таблица истории миграций (PostgreSQL)
-- =====================================================

CREATE TABLE IF NOT EXISTS _migrations (
    _id BIGSERIAL PRIMARY KEY,
    _migration_id TEXT NOT NULL,                    -- уникальный ID миграции "OrderProps_TotalPrice_v1"
    _scheme_id BIGINT NOT NULL REFERENCES _schemes(_id) ON DELETE CASCADE,
    _structure_id BIGINT REFERENCES _structures(_id) ON DELETE SET NULL,  -- NULL = вся схема
    _property_name TEXT,                            -- имя свойства (для логов)
    _expression_hash TEXT,                          -- MD5 от Expression для детекции изменений
    _migration_type TEXT NOT NULL,                  -- ComputedFrom, TypeChange, DefaultValue, Transform
    _applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _applied_by TEXT,                               -- кто применил (user/system)
    _sql_executed TEXT,                             -- SQL который был выполнен (для аудита)
    _affected_rows INT,                             -- сколько записей затронуто
    _duration_ms INT,                               -- время выполнения
    _dry_run BOOLEAN NOT NULL DEFAULT FALSE,        -- это был dry-run?
    
    CONSTRAINT uq_migration_scheme UNIQUE(_scheme_id, _migration_id)
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_migrations_scheme ON _migrations(_scheme_id);
CREATE INDEX IF NOT EXISTS idx_migrations_applied ON _migrations(_applied_at DESC);

COMMENT ON TABLE _migrations IS 'История применённых миграций данных (Pro feature)';
COMMENT ON COLUMN _migrations._migration_id IS 'Уникальный ID миграции в формате SchemeType_PropertyName_vN';
COMMENT ON COLUMN _migrations._expression_hash IS 'MD5 хеш Expression для детекции изменений';
COMMENT ON COLUMN _migrations._sql_executed IS 'SQL запрос для аудита и отладки';


-- ===== redb_save_json_objects.sql =====
-- ============================================================
-- SAVE OBJECT FROM JSON (Postgres)
-- Inverse of get_object_json: JSON → _objects + _values
-- Strategy: DeleteInsert (delete all _values, then insert new)
-- ============================================================
-- Input format: same JSON structure as produced by get_object_json()
--
-- Two functions (mirroring read architecture):
--   save_object_json(jsonb) → bigint           [mirror of get_object_json]
--   save_hierarchical_properties(...)           [mirror of build_hierarchical_properties_optimized]
--
-- Supported field types (all branches from build_hierarchical_properties_optimized):
--   1. Array of Class          (type_semantic='Object',  _is_array)
--   2. Array of _RObject refs  (type_semantic='_RObject', _is_array)
--   3. Array of primitives     (String/Long/Double/Guid/Boolean/Numeric/DateTime/ByteArray/ListItem)
--   4. Dictionary of Class     (type_semantic='Object',  _is_dictionary)
--   5. Dictionary of _RObject  (type_semantic='_RObject', _is_dictionary)
--   6. Dictionary of primitives
--   7. Single _RObject ref     (type_name='Object', type_semantic='_RObject')
--   8. Class field             (type_semantic='Object', marker _Guid)
--   9. ListItem                (db_type='ListItem', or Long backward-compat)
--  10. Primitive scalars       (String/Long/Double/Guid/Boolean/Numeric/DateTime/ByteArray)
-- ============================================================

-- Drop existing functions (handle signature changes on re-deploy)
DROP FUNCTION IF EXISTS save_object_json(jsonb);
DROP FUNCTION IF EXISTS save_hierarchical_properties(bigint, bigint, bigint, jsonb, bigint);


-- ============================================================
-- 1. RECURSIVE PROPERTY WRITER
--    Mirror of build_hierarchical_properties_optimized (reader)
--    Walks _scheme_metadata_cache, extracts values from JSON,
--    INSERTs into _values with correct column routing
-- ============================================================
CREATE OR REPLACE FUNCTION save_hierarchical_properties(
    p_object_id bigint,            -- owning object _id
    p_parent_structure_id bigint,  -- NULL for root level, structure _id for nested Class
    p_scheme_id bigint,            -- scheme _id (for metadata cache lookup)
    p_properties_json jsonb,       -- JSON object: {"Name":"Alice","Tags":[...],"Address":{...}}
    p_parent_value_id bigint DEFAULT NULL  -- NULL for root, parent _values._id for nested
) RETURNS void
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    structure_record RECORD;
    field_json jsonb;
    v_value_id bigint;
    v_head_id bigint;
    v_element_id bigint;
    v_element jsonb;
    v_nested_id bigint;
    v_idx integer;
    v_dict_entry RECORD;
BEGIN
    -- Ensure metadata cache is populated (mirrors read function)
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = p_scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(p_scheme_id);
    END IF;

    -- Walk structures for this level
    -- Same query & ordering as build_hierarchical_properties_optimized
    FOR structure_record IN
        SELECT
            c._structure_id as structure_id,
            c._name as field_name,
            c._collection_type as collection_type,
            c._collection_type = -9223372036854775668 as _is_array,      -- Array type ID
            c._collection_type = -9223372036854775667 as _is_dictionary,  -- Dictionary type ID
            c.type_name,
            c.db_type,
            c.type_semantic
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = p_scheme_id
          AND ((p_parent_structure_id IS NULL AND c._parent_structure_id IS NULL)
               OR (p_parent_structure_id IS NOT NULL AND c._parent_structure_id = p_parent_structure_id))
        ORDER BY c._order, c._structure_id
    LOOP
        -- Extract field value from JSON by structure name
        field_json := p_properties_json -> structure_record.field_name;

        -- Skip if field not present (SQL NULL) or JSON null
        IF field_json IS NULL OR field_json = 'null'::jsonb THEN
            CONTINUE;
        END IF;

        -- ================================================================
        -- DISPATCH BY TYPE
        -- Mirrors the CASE tree in build_hierarchical_properties_optimized
        -- ================================================================

        IF structure_record._is_array THEN
            -- ========================================================
            -- ARRAY FIELDS  (branches 1-3)
            -- ========================================================
            IF jsonb_typeof(field_json) != 'array' THEN
                CONTINUE;  -- type mismatch guard
            END IF;

            -- Insert head record: marker "this array property exists"
            -- Read side: base_array_value_id IS NULL → property is null (not [])
            v_head_id := nextval('global_identity');
            INSERT INTO _values (_id, _id_structure, _id_object, _array_index, _array_parent_id)
            VALUES (v_head_id, structure_record.structure_id, p_object_id, NULL, p_parent_value_id);

            -- Empty array [] → head record alone (read returns [])
            IF jsonb_array_length(field_json) = 0 THEN
                CONTINUE;
            END IF;

            -- ----- Branch 1: Array of Class fields -----
            -- Mirror: build_hierarchical_properties_optimized recursion per element
            IF structure_record.type_semantic = 'Object' THEN
                FOR v_idx IN 0..jsonb_array_length(field_json) - 1
                LOOP
                    v_element := field_json -> v_idx;
                    IF v_element IS NULL OR v_element = 'null'::jsonb THEN
                        CONTINUE;
                    END IF;

                    -- Insert element record (_array_index = index, _array_parent_id = head)
                    v_element_id := nextval('global_identity');
                    INSERT INTO _values (_id, _id_structure, _id_object, _array_index, _array_parent_id)
                    VALUES (v_element_id, structure_record.structure_id, p_object_id,
                            v_idx::text, v_head_id);

                    -- Recurse into child properties of this array element
                    IF jsonb_typeof(v_element) = 'object' THEN
                        PERFORM save_hierarchical_properties(
                            p_object_id,
                            structure_record.structure_id,  -- parent structure = array structure
                            p_scheme_id,
                            v_element,
                            v_element_id  -- children's _array_parent_id → this element
                        );
                    END IF;
                END LOOP;

            -- ----- Branch 2: Array of Object references (_RObject) -----
            -- Mirror: get_object_json(v._Object, max_depth - 1) per element
            ELSIF structure_record.type_semantic = '_RObject' THEN
                FOR v_idx IN 0..jsonb_array_length(field_json) - 1
                LOOP
                    v_element := field_json -> v_idx;

                    -- Resolve: full object JSON → recursive save; number → ID directly
                    IF jsonb_typeof(v_element) = 'object' THEN
                        v_nested_id := save_object_json(v_element);
                    ELSIF v_element IS NOT NULL AND v_element != 'null'::jsonb THEN
                        v_nested_id := (v_element #>> '{}')::bigint;
                    ELSE
                        v_nested_id := NULL;
                    END IF;

                    v_element_id := nextval('global_identity');
                    INSERT INTO _values (
                        _id, _id_structure, _id_object, _Object, _array_index, _array_parent_id
                    ) VALUES (
                        v_element_id, structure_record.structure_id, p_object_id,
                        v_nested_id, v_idx::text, v_head_id
                    );
                END LOOP;

            -- ----- Branch 3: Array of primitive types -----
            -- Mirror: to_jsonb(v._String) / to_jsonb(v._Long) / ... per element
            ELSE
                FOR v_idx IN 0..jsonb_array_length(field_json) - 1
                LOOP
                    v_element := field_json -> v_idx;
                    v_element_id := nextval('global_identity');

                    INSERT INTO _values (
                        _id, _id_structure, _id_object, _array_index, _array_parent_id,
                        _String, _Long, _Guid, _Double, _Numeric,
                        _DateTimeOffset, _Boolean, _ByteArray, _ListItem
                    ) VALUES (
                        v_element_id, structure_record.structure_id, p_object_id,
                        v_idx::text, v_head_id,
                        -- Route value to correct column based on db_type
                        CASE WHEN structure_record.db_type = 'String'
                             THEN v_element #>> '{}' END,
                        CASE WHEN structure_record.db_type = 'Long'
                                  AND jsonb_typeof(v_element) != 'object'
                             THEN (v_element #>> '{}')::bigint END,
                        CASE WHEN structure_record.db_type = 'Guid'
                             THEN (v_element #>> '{}')::uuid END,
                        CASE WHEN structure_record.db_type = 'Double'
                             THEN (v_element #>> '{}')::float END,
                        CASE WHEN structure_record.db_type = 'Numeric'
                             THEN (v_element #>> '{}')::numeric END,
                        CASE WHEN structure_record.db_type IN ('DateTimeOffset', 'DateTime')
                             THEN (v_element #>> '{}')::timestamptz END,
                        CASE WHEN structure_record.db_type = 'Boolean'
                             THEN (v_element #>> '{}')::boolean END,
                        CASE WHEN structure_record.db_type = 'ByteArray'
                             THEN decode(v_element #>> '{}', 'base64') END,
                        -- _ListItem: ListItem type OR Long backward-compat (Long field with ListItem JSON)
                        CASE WHEN structure_record.db_type = 'ListItem' THEN
                                 CASE WHEN jsonb_typeof(v_element) = 'object'
                                      THEN (v_element ->> 'id')::bigint
                                      ELSE (v_element #>> '{}')::bigint END
                             WHEN structure_record.db_type = 'Long'
                                  AND jsonb_typeof(v_element) = 'object'
                             THEN (v_element ->> 'id')::bigint
                        END
                    );
                END LOOP;
            END IF;


        ELSIF structure_record._is_dictionary THEN
            -- ========================================================
            -- DICTIONARY FIELDS  (branches 4-6)
            -- ========================================================
            IF jsonb_typeof(field_json) != 'object' THEN
                CONTINUE;  -- type mismatch guard
            END IF;

            -- Insert head record (marker: "this dictionary property exists")
            v_head_id := nextval('global_identity');
            INSERT INTO _values (_id, _id_structure, _id_object, _array_index, _array_parent_id)
            VALUES (v_head_id, structure_record.structure_id, p_object_id, NULL, p_parent_value_id);

            -- Empty dict {} → head record alone (read returns {}); jsonb_each yields zero rows

            -- ----- Branch 4: Dictionary of Class fields -----
            -- Mirror: build_hierarchical_properties_optimized recursion per entry
            IF structure_record.type_semantic = 'Object' THEN
                FOR v_dict_entry IN SELECT * FROM jsonb_each(field_json)
                LOOP
                    v_element_id := nextval('global_identity');
                    INSERT INTO _values (_id, _id_structure, _id_object, _array_index, _array_parent_id)
                    VALUES (v_element_id, structure_record.structure_id, p_object_id,
                            v_dict_entry.key, v_head_id);

                    IF jsonb_typeof(v_dict_entry.value) = 'object' THEN
                        PERFORM save_hierarchical_properties(
                            p_object_id,
                            structure_record.structure_id,
                            p_scheme_id,
                            v_dict_entry.value,
                            v_element_id
                        );
                    END IF;
                END LOOP;

            -- ----- Branch 5: Dictionary of Object references (_RObject) -----
            -- Mirror: get_object_json(v._Object, max_depth - 1) per entry
            ELSIF structure_record.type_semantic = '_RObject' THEN
                FOR v_dict_entry IN SELECT * FROM jsonb_each(field_json)
                LOOP
                    IF jsonb_typeof(v_dict_entry.value) = 'object' THEN
                        v_nested_id := save_object_json(v_dict_entry.value);
                    ELSIF v_dict_entry.value IS NOT NULL AND v_dict_entry.value != 'null'::jsonb THEN
                        v_nested_id := (v_dict_entry.value #>> '{}')::bigint;
                    ELSE
                        v_nested_id := NULL;
                    END IF;

                    v_element_id := nextval('global_identity');
                    INSERT INTO _values (
                        _id, _id_structure, _id_object, _Object, _array_index, _array_parent_id
                    ) VALUES (
                        v_element_id, structure_record.structure_id, p_object_id,
                        v_nested_id, v_dict_entry.key, v_head_id
                    );
                END LOOP;

            -- ----- Branch 6: Dictionary of primitive types -----
            -- Mirror: jsonb_object_agg(key, to_jsonb(v._String / v._Long / ...))
            ELSE
                FOR v_dict_entry IN SELECT * FROM jsonb_each(field_json)
                LOOP
                    v_element_id := nextval('global_identity');

                    INSERT INTO _values (
                        _id, _id_structure, _id_object, _array_index, _array_parent_id,
                        _String, _Long, _Guid, _Double, _Numeric,
                        _DateTimeOffset, _Boolean, _ByteArray, _ListItem
                    ) VALUES (
                        v_element_id, structure_record.structure_id, p_object_id,
                        v_dict_entry.key, v_head_id,
                        CASE WHEN structure_record.db_type = 'String'
                             THEN v_dict_entry.value #>> '{}' END,
                        CASE WHEN structure_record.db_type = 'Long'
                             THEN (v_dict_entry.value #>> '{}')::bigint END,
                        CASE WHEN structure_record.db_type = 'Guid'
                             THEN (v_dict_entry.value #>> '{}')::uuid END,
                        CASE WHEN structure_record.db_type = 'Double'
                             THEN (v_dict_entry.value #>> '{}')::float END,
                        CASE WHEN structure_record.db_type = 'Numeric'
                             THEN (v_dict_entry.value #>> '{}')::numeric END,
                        CASE WHEN structure_record.db_type IN ('DateTimeOffset', 'DateTime')
                             THEN (v_dict_entry.value #>> '{}')::timestamptz END,
                        CASE WHEN structure_record.db_type = 'Boolean'
                             THEN (v_dict_entry.value #>> '{}')::boolean END,
                        CASE WHEN structure_record.db_type = 'ByteArray'
                             THEN decode(v_dict_entry.value #>> '{}', 'base64') END,
                        CASE WHEN structure_record.db_type = 'ListItem' THEN
                                 CASE WHEN jsonb_typeof(v_dict_entry.value) = 'object'
                                      THEN (v_dict_entry.value ->> 'id')::bigint
                                      ELSE (v_dict_entry.value #>> '{}')::bigint END
                        END
                    );
                END LOOP;
            END IF;


        ELSIF structure_record.type_name = 'Object' AND structure_record.type_semantic = '_RObject' THEN
            -- ========================================================
            -- Branch 7: SINGLE OBJECT REFERENCE
            -- Mirror: get_object_json(current_value_record._Object, max_depth - 1)
            -- ========================================================
            IF jsonb_typeof(field_json) = 'object' THEN
                -- Full nested object JSON → save recursively, get ID
                v_nested_id := save_object_json(field_json);
            ELSE
                -- Numeric ID reference
                v_nested_id := (field_json #>> '{}')::bigint;
            END IF;

            v_value_id := nextval('global_identity');
            INSERT INTO _values (_id, _id_structure, _id_object, _Object, _array_parent_id)
            VALUES (v_value_id, structure_record.structure_id, p_object_id,
                    v_nested_id, p_parent_value_id);


        ELSIF structure_record.type_semantic = 'Object' THEN
            -- ========================================================
            -- Branch 8: CLASS FIELD (nested class with child properties)
            -- Mirror: WHEN _Guid IS NULL THEN NULL ELSE build_hierarchical_...
            -- ========================================================
            IF jsonb_typeof(field_json) != 'object' THEN
                CONTINUE;  -- class fields must be JSON objects
            END IF;

            -- Insert marker record with _Guid (read checks: _Guid IS NULL → class is null)
            v_value_id := nextval('global_identity');
            INSERT INTO _values (_id, _id_structure, _id_object, _Guid, _array_parent_id)
            VALUES (v_value_id, structure_record.structure_id, p_object_id,
                    gen_random_uuid(), p_parent_value_id);

            -- Recurse: save child properties under this class structure
            PERFORM save_hierarchical_properties(
                p_object_id,
                structure_record.structure_id,  -- children are under this structure in _scheme_metadata_cache
                p_scheme_id,
                field_json,
                v_value_id  -- children's _array_parent_id → this marker record
            );


        ELSE
            -- ========================================================
            -- Branches 9-10: PRIMITIVE SCALAR FIELDS + LISTITEM
            -- Mirror: to_jsonb(v._String) / to_jsonb(v._Long) / build_listitem_jsonb / ...
            -- ========================================================
            v_value_id := nextval('global_identity');

            -- Branch 9: ListItem (db_type='ListItem', or Long backward-compat with ListItem JSON)
            IF structure_record.db_type = 'ListItem'
               OR (structure_record.db_type = 'Long' AND jsonb_typeof(field_json) = 'object') THEN

                INSERT INTO _values (_id, _id_structure, _id_object, _ListItem, _array_parent_id)
                VALUES (v_value_id, structure_record.structure_id, p_object_id,
                        (field_json ->> 'id')::bigint, p_parent_value_id);

            -- Branch 10: Standard primitive types
            ELSE
                INSERT INTO _values (
                    _id, _id_structure, _id_object, _array_parent_id,
                    _String, _Long, _Guid, _Double, _Numeric,
                    _DateTimeOffset, _Boolean, _ByteArray
                ) VALUES (
                    v_value_id, structure_record.structure_id, p_object_id, p_parent_value_id,
                    CASE WHEN structure_record.db_type = 'String'
                         THEN field_json #>> '{}' END,
                    CASE WHEN structure_record.db_type = 'Long'
                         THEN (field_json #>> '{}')::bigint END,
                    CASE WHEN structure_record.db_type = 'Guid'
                         THEN (field_json #>> '{}')::uuid END,
                    CASE WHEN structure_record.db_type = 'Double'
                         THEN (field_json #>> '{}')::float END,
                    CASE WHEN structure_record.db_type = 'Numeric'
                         THEN (field_json #>> '{}')::numeric END,
                    CASE WHEN structure_record.db_type IN ('DateTimeOffset', 'DateTime')
                         THEN (field_json #>> '{}')::timestamptz END,
                    CASE WHEN structure_record.db_type = 'Boolean'
                         THEN (field_json #>> '{}')::boolean END,
                    CASE WHEN structure_record.db_type = 'ByteArray'
                         THEN decode(field_json #>> '{}', 'base64') END
                );
            END IF;

        END IF;

    END LOOP;
END;
$BODY$;


-- ============================================================
-- 2. MAIN ENTRY POINT
--    Mirror of get_object_json (reader)
--    JSON → UPSERT _objects → DELETE _values → save properties
-- ============================================================
CREATE OR REPLACE FUNCTION save_object_json(
    p_json_data jsonb
) RETURNS bigint
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_object_id bigint;
    v_scheme_id bigint;
    v_properties jsonb;
    v_exists boolean;
BEGIN
    -- Extract required fields
    v_object_id := (p_json_data ->> 'id')::bigint;
    v_scheme_id := (p_json_data ->> 'scheme_id')::bigint;

    IF v_scheme_id IS NULL THEN
        RAISE EXCEPTION 'save_object_json: scheme_id is required';
    END IF;

    -- Verify scheme exists
    IF NOT EXISTS(SELECT 1 FROM _schemes WHERE _id = v_scheme_id) THEN
        RAISE EXCEPTION 'save_object_json: scheme_id=% not found', v_scheme_id;
    END IF;

    -- Generate new ID for new objects (id = 0 or absent)
    IF v_object_id IS NULL OR v_object_id = 0 THEN
        v_object_id := nextval('global_identity');
    END IF;

    -- Check if object already exists
    SELECT EXISTS(SELECT 1 FROM _objects WHERE _id = v_object_id) INTO v_exists;

    IF v_exists THEN
        -- ===== UPDATE existing object =====
        -- Mirror: base fields from get_object_json output
        -- Note: _date_create is preserved (not updated)
        UPDATE _objects SET
            _id_scheme       = v_scheme_id,
            _id_parent       = (p_json_data ->> 'parent_id')::bigint,
            _id_owner        = COALESCE((p_json_data ->> 'owner_id')::bigint, _id_owner),
            _id_who_change   = COALESCE((p_json_data ->> 'who_change_id')::bigint, _id_who_change),
            _name            = p_json_data ->> 'name',
            _note            = p_json_data ->> 'note',
            _key             = (p_json_data ->> 'key')::bigint,
            _hash            = (p_json_data ->> 'hash')::uuid,
            _date_modify     = now(),
            _date_begin      = (p_json_data ->> 'date_begin')::timestamptz,
            _date_complete   = (p_json_data ->> 'date_complete')::timestamptz,
            _value_long      = (p_json_data ->> 'value_long')::bigint,
            _value_string    = p_json_data ->> 'value_string',
            _value_guid      = (p_json_data ->> 'value_guid')::uuid,
            _value_bool      = (p_json_data ->> 'value_bool')::boolean,
            _value_double    = (p_json_data ->> 'value_double')::float,
            _value_numeric   = (p_json_data ->> 'value_numeric')::numeric,
            _value_datetime  = (p_json_data ->> 'value_datetime')::timestamptz,
            _value_bytes     = decode(p_json_data ->> 'value_bytes', 'base64')
        WHERE _id = v_object_id;

        -- DeleteInsert strategy: remove all old values before inserting new
        DELETE FROM _values WHERE _id_object = v_object_id;

    ELSE
        -- ===== INSERT new object =====
        INSERT INTO _objects (
            _id, _id_scheme, _id_parent, _id_owner, _id_who_change,
            _name, _note, _key, _hash,
            _date_create, _date_modify, _date_begin, _date_complete,
            _value_long, _value_string, _value_guid, _value_bool,
            _value_double, _value_numeric, _value_datetime, _value_bytes
        ) VALUES (
            v_object_id,
            v_scheme_id,
            (p_json_data ->> 'parent_id')::bigint,
            COALESCE((p_json_data ->> 'owner_id')::bigint, 1),
            COALESCE((p_json_data ->> 'who_change_id')::bigint, 1),
            p_json_data ->> 'name',
            p_json_data ->> 'note',
            (p_json_data ->> 'key')::bigint,
            (p_json_data ->> 'hash')::uuid,
            COALESCE((p_json_data ->> 'date_create')::timestamptz, now()),
            now(),
            (p_json_data ->> 'date_begin')::timestamptz,
            (p_json_data ->> 'date_complete')::timestamptz,
            (p_json_data ->> 'value_long')::bigint,
            p_json_data ->> 'value_string',
            (p_json_data ->> 'value_guid')::uuid,
            (p_json_data ->> 'value_bool')::boolean,
            (p_json_data ->> 'value_double')::float,
            (p_json_data ->> 'value_numeric')::numeric,
            (p_json_data ->> 'value_datetime')::timestamptz,
            decode(p_json_data ->> 'value_bytes', 'base64')
        );
    END IF;

    -- Save properties if present (not null, not JSON null)
    v_properties := p_json_data -> 'properties';
    IF v_properties IS NOT NULL AND v_properties != 'null'::jsonb THEN
        PERFORM save_hierarchical_properties(
            v_object_id,
            NULL,           -- root level (no parent structure)
            v_scheme_id,
            v_properties,
            NULL            -- root level (no parent value)
        );
    END IF;

    RETURN v_object_id;
END;
$BODY$;


-- ============================================================
-- DOCUMENTATION
-- ============================================================
COMMENT ON FUNCTION save_hierarchical_properties(bigint, bigint, bigint, jsonb, bigint) IS
'Recursive property writer — inverse of build_hierarchical_properties_optimized.
Walks _scheme_metadata_cache for the given scheme, extracts values from the properties JSON,
and INSERTs into _values with correct column routing based on db_type.
Handles all 10 field type branches:
  Arrays (Class / _RObject / primitives) with head+element records
  Dictionaries (Class / _RObject / primitives) with head+keyed-element records
  Single Object references (_RObject) — recursive save_object_json for nested objects
  Class fields — marker record with _Guid + recursive child properties
  ListItem — extract id from JSON object
  Primitives — type-based column routing (String/_Long/_Guid/_Double/etc.)
Mutual recursion: _RObject fields call save_object_json for nested objects.';

COMMENT ON FUNCTION save_object_json(jsonb) IS
'Save object from JSON — inverse of get_object_json.
Strategy: DeleteInsert (UPDATE/INSERT _objects, DELETE all _values, INSERT new _values).
Input: JSON in exactly the same format as produced by get_object_json().
Supports:
  New objects (id=0 or absent): generates ID via nextval(global_identity)
  Existing objects (id>0): UPDATE base fields, DELETE+re-INSERT values
  All property types: scalars, arrays, dictionaries, Class fields, Object references
  Nested objects in _RObject fields: saved recursively via mutual recursion
Returns: object _id (bigint).';


-- ===== redb_soft_delete.sql =====
-- =====================================================
-- SOFT DELETE FUNCTIONS FOR POSTGRESQL
-- Part of Background Deletion System
-- =====================================================

-- Drop existing functions if any
DROP FUNCTION IF EXISTS mark_for_deletion(bigint[], bigint);
DROP FUNCTION IF EXISTS mark_for_deletion(bigint[], bigint, bigint);
DROP FUNCTION IF EXISTS purge_trash(bigint, integer);

-- =====================================================
-- FUNCTION: mark_for_deletion
-- Marks objects for deletion by moving them under a trash container
-- Creates trash container, finds all descendants via CTE, updates parent and scheme
-- All operations in single transaction (atomic)
-- p_trash_parent_id: optional parent for trash container (NULL = root level)
-- =====================================================
CREATE OR REPLACE FUNCTION mark_for_deletion(
    p_object_ids bigint[],
    p_user_id bigint,
    p_trash_parent_id bigint DEFAULT NULL
) RETURNS TABLE(trash_id bigint, marked_count bigint) AS $$
DECLARE
    v_trash_id bigint;
    v_count bigint;
BEGIN
    -- 1. Create Trash container object with @@__deleted scheme
    -- Progress fields: _value_long=total, _key=deleted, _value_string=status
    INSERT INTO _objects (
        _id, _id_scheme, _id_parent, _id_owner, _id_who_change,
        _name, _date_create, _date_modify,
        _value_long, _key, _value_string
    ) VALUES (
        nextval('global_identity'), 
        -10,  -- @@__deleted scheme
        p_trash_parent_id,  -- user-specified parent or NULL
        p_user_id, 
        p_user_id,
        '__TRASH__' || p_user_id || '_' || extract(epoch from now())::bigint,
        NOW(), 
        NOW(),
        0,          -- _value_long = total (will be updated after count)
        0,          -- _key = deleted
        'pending'   -- _value_string = status
    ) RETURNING _id INTO v_trash_id;
    
    -- 2. CTE: find all objects and their descendants recursively
    -- 3. UPDATE: move all found objects under Trash container and change scheme
    WITH RECURSIVE all_descendants AS (
        -- Start with requested objects
        SELECT _id FROM _objects 
        WHERE _id = ANY(p_object_ids)
          AND _id_scheme != -10  -- skip already deleted
        
        UNION ALL
        
        -- Recursively find children
        SELECT o._id FROM _objects o
        INNER JOIN all_descendants d ON o._id_parent = d._id
        WHERE o._id_scheme != -10  -- skip already deleted
    )
    UPDATE _objects 
    SET _id_parent = v_trash_id,
        _id_scheme = -10,
        _date_modify = NOW()
    WHERE _id IN (SELECT _id FROM all_descendants);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- 4. Update trash container with total count
    UPDATE _objects 
    SET _value_long = v_count
    WHERE _id = v_trash_id;
    
    -- 5. Return trash container ID and count of marked objects
    RETURN QUERY SELECT v_trash_id, v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION mark_for_deletion(bigint[], bigint, bigint) IS 
'Marks objects for soft-deletion. Creates a trash container, moves all specified objects 
and their descendants under it with scheme=@@__deleted. Returns (trash_id, marked_count). 
p_trash_parent_id: optional parent ID for trash container (NULL = root level).
Atomic operation - all or nothing.';


-- =====================================================
-- FUNCTION: purge_trash
-- Physically deletes objects from a trash container in batches
-- ON DELETE CASCADE handles _values deletion automatically
-- Updates progress in trash container (_key=deleted, _value_string=status)
-- After all children deleted, removes the trash container itself
-- =====================================================
CREATE OR REPLACE FUNCTION purge_trash(
    p_trash_id bigint,
    p_batch_size integer DEFAULT 10
) RETURNS TABLE(deleted_count bigint, remaining_count bigint) AS $$
DECLARE
    v_deleted bigint;
    v_remaining bigint;
BEGIN
    -- Update status to 'running' if it was 'pending'
    UPDATE _objects 
    SET _value_string = 'running',
        _date_modify = NOW()
    WHERE _id = p_trash_id AND _value_string = 'pending';
    
    -- Delete a batch of objects (CASCADE handles _values)
    WITH to_delete AS (
        SELECT _id FROM _objects
        WHERE _id_parent = p_trash_id
        LIMIT p_batch_size
    )
    DELETE FROM _objects 
    WHERE _id IN (SELECT _id FROM to_delete);
    
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    
    -- Count remaining objects in this trash
    SELECT COUNT(*) INTO v_remaining
    FROM _objects 
    WHERE _id_parent = p_trash_id;
    
    -- Update progress in trash container
    UPDATE _objects 
    SET _key = _key + v_deleted,
        _value_string = CASE WHEN v_remaining = 0 THEN 'completed' ELSE 'running' END,
        _date_modify = NOW()
    WHERE _id = p_trash_id;
    
    -- If no more children, delete the trash container itself
    IF v_remaining = 0 THEN
        DELETE FROM _objects WHERE _id = p_trash_id;
    END IF;
    
    RETURN QUERY SELECT v_deleted, v_remaining;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION purge_trash(bigint, integer) IS 
'Physically deletes objects from a trash container in batches. 
p_trash_id: ID of the trash container created by mark_for_deletion.
p_batch_size: Number of objects to delete per call (default 10).
Returns (deleted_count, remaining_count). When remaining=0, trash container is also deleted.
Call repeatedly until remaining_count = 0.';



-- ===== redb_structure_tree.sql =====
-- ====================================================================================================
-- FUNCTIONS FOR WORKING WITH SCHEME STRUCTURE TREE
-- ====================================================================================================
-- Supports hierarchical navigation through structures: parent → children → descendants
-- Solves flat structure search problems in SaveAsync
-- ====================================================================================================

-- MAIN FUNCTION: Build scheme structure tree (SIMPLE APPROACH)
-- SIMPLE AND CLEAR LOGIC: get current layer → for each structure get children recursively
CREATE OR REPLACE FUNCTION get_scheme_structure_tree(
    scheme_id bigint,
    parent_id bigint DEFAULT NULL,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result jsonb := '[]'::jsonb;
    structure_record RECORD;
    children_json jsonb;
BEGIN
    -- Protection from infinite recursion
    IF max_depth <= 0 THEN
        RETURN jsonb_build_array(jsonb_build_object('error', 'Max recursion depth reached'));
    END IF;
    
    -- Check scheme existence
    IF NOT EXISTS(SELECT 1 FROM _schemes WHERE _id = scheme_id) THEN
        RETURN jsonb_build_array(jsonb_build_object('error', 'Scheme not found'));
    END IF;
    
    -- AUTOMATIC CACHE CHECK AND FILL
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-fill without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- SIMPLE LOGIC: Get structures of CURRENT LEVEL
    -- OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    FOR structure_record IN
        SELECT 
            c._structure_id as _id,
            c._name,
            c._order,
            c._collection_type IS NOT NULL as _is_array,  -- _collection_type != NULL = array/dict
            c._collection_type,
            c._store_null,
            c._allow_not_null,
            c.type_name,
            c.db_type,
            c.type_semantic
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = scheme_id
          AND ((parent_id IS NULL AND c._parent_structure_id IS NULL) 
               OR (parent_id IS NOT NULL AND c._parent_structure_id = parent_id))
        ORDER BY c._order, c._structure_id
    LOOP
        -- CHECK IF STRUCTURE HAS CHILDREN
        IF EXISTS(SELECT 1 FROM _structures 
                 WHERE _id_scheme = scheme_id 
                   AND _id_parent = structure_record._id) THEN
            -- RECURSIVELY get children (simple function call!)
            children_json := get_scheme_structure_tree(scheme_id, structure_record._id, max_depth - 1);
        ELSE
            -- No children - empty array
            children_json := '[]'::jsonb;
        END IF;
        
        -- ADD STRUCTURE TO RESULT (simple construction)
        result := result || jsonb_build_array(
            jsonb_build_object(
                'structure_id', structure_record._id,
                'name', structure_record._name,
                'order', structure_record._order,
                'is_array', structure_record._is_array,  -- For compatibility
                'collection_type', structure_record._collection_type,  -- New collection type
                'store_null', structure_record._store_null,
                'allow_not_null', structure_record._allow_not_null,
                'type_name', structure_record.type_name,
                'db_type', structure_record.db_type,
                'type_semantic', structure_record.type_semantic,
                'children', children_json  -- Recursively obtained children
            )
        );
    END LOOP;
    
    RETURN result;
END;
$BODY$;

-- HELPER FUNCTION: Get only direct child structures  
CREATE OR REPLACE FUNCTION get_structure_children(
    scheme_id bigint,
    parent_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    -- AUTOMATIC CACHE CHECK AND FILL
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(scheme_id);
        -- Auto-fill without NOTICE (use warmup_all_metadata_caches() for explicit warmup)
    END IF;
    
    -- OPTIMIZATION: Use _scheme_metadata_cache instead of JOIN _structures ← _types
    RETURN (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'structure_id', c._structure_id,
                'name', c._name,
                'order', c._order,
                'is_array', c._collection_type IS NOT NULL,  -- For compatibility
                'collection_type', c._collection_type,       -- New collection type
                'type_name', c.type_name,
                'db_type', c.db_type,
                'type_semantic', c.type_semantic
            ) ORDER BY c._order, c._structure_id
        ), '[]'::jsonb)
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = scheme_id
          AND c._parent_structure_id = parent_id
    );
END;
$BODY$;

-- DIAGNOSTIC FUNCTION: Validate structure tree for redundancy
CREATE OR REPLACE FUNCTION validate_structure_tree(
    scheme_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    validation_result jsonb;
    excessive_structures jsonb;
    orphaned_structures jsonb;
    circular_references jsonb;
BEGIN
    -- 1. Find excessive structures (structures without values connections)
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', s._id,
            'name', s._name,
            'parent_name', parent_s._name,
            'issue', 'No values found - possibly excessive structure'
        )
    ) INTO excessive_structures
    FROM _structures s
    LEFT JOIN _structures parent_s ON parent_s._id = s._id_parent
    LEFT JOIN _values v ON v._id_structure = s._id
    WHERE s._id_scheme = scheme_id
      AND v._id IS NULL  -- No values for this structure
      AND s._id_parent IS NOT NULL; -- Only child structures
    
    -- 2. Find orphaned structures (parent doesn't exist)
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', s._id,
            'name', s._name,
            'parent_id', s._id_parent,
            'issue', 'Parent structure does not exist'
        )
    ) INTO orphaned_structures
    FROM _structures s
    WHERE s._id_scheme = scheme_id
      AND s._id_parent IS NOT NULL
      AND NOT EXISTS(SELECT 1 FROM _structures parent_s WHERE parent_s._id = s._id_parent);
    
    -- 3. Simple check for circular references (structure references itself via chain)
    WITH RECURSIVE cycle_check AS (
        SELECT _id, _id_parent, ARRAY[_id] as path, false as has_cycle
        FROM _structures WHERE _id_scheme = scheme_id AND _id_parent IS NOT NULL
        
        UNION ALL
        
        SELECT s._id, s._id_parent, cc.path || s._id, s._id = ANY(cc.path)
        FROM _structures s
        JOIN cycle_check cc ON cc._id_parent = s._id
        WHERE NOT cc.has_cycle AND array_length(cc.path, 1) < 50
    )
    SELECT jsonb_agg(
        jsonb_build_object(
            'structure_id', _id,
            'path', path,
            'issue', 'Circular reference detected'
        )
    ) INTO circular_references
    FROM cycle_check 
    WHERE has_cycle;
    
    -- Form final report
    validation_result := jsonb_build_object(
        'scheme_id', scheme_id,
        'validation_date', NOW(),
        'excessive_structures', COALESCE(excessive_structures, '[]'::jsonb),
        'orphaned_structures', COALESCE(orphaned_structures, '[]'::jsonb), 
        'circular_references', COALESCE(circular_references, '[]'::jsonb),
        'total_structures', (SELECT COUNT(*) FROM _structures WHERE _id_scheme = scheme_id),
        'is_valid', (excessive_structures IS NULL AND orphaned_structures IS NULL AND circular_references IS NULL)
    );
    
    RETURN validation_result;
END;
$BODY$;

-- FUNCTION: Get all structure descendants (flat list)
CREATE OR REPLACE FUNCTION get_structure_descendants(
    scheme_id bigint,
    parent_id bigint
) RETURNS jsonb
LANGUAGE 'plpgsql'  
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    RETURN (
        WITH RECURSIVE descendants AS (
            -- Direct child structures
            SELECT _id, _name, _id_parent, 0 as level
            FROM _structures 
            WHERE _id_scheme = scheme_id AND _id_parent = parent_id
            
            UNION ALL
            
            -- Recursively all descendants
            SELECT s._id, s._name, s._id_parent, d.level + 1
            FROM _structures s
            JOIN descendants d ON d._id = s._id_parent
            WHERE s._id_scheme = scheme_id AND d.level < 10
        )
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'structure_id', _id,
                'name', _name, 
                'parent_id', _id_parent,
                'level', level
            ) ORDER BY level, _id
        ), '[]'::jsonb)
        FROM descendants
    );
END;
$BODY$;

-- Comments for structure tree functions
COMMENT ON FUNCTION get_scheme_structure_tree(bigint, bigint, integer) IS 'Build complete scheme structure tree with hierarchy. Supports recursion depth limit. Used by PostgresSchemeSyncProvider for correct structure traversal in SaveAsync.';

COMMENT ON FUNCTION get_structure_children(bigint, bigint) IS 'Get only direct child structures without recursion. Fast function for simple tree navigation cases.';

COMMENT ON FUNCTION validate_structure_tree(bigint) IS 'Structure tree diagnostics: find excessive structures, orphaned references, circular dependencies. Helps identify issues like with Address.Details.Tags1.';

COMMENT ON FUNCTION get_structure_descendants(bigint, bigint) IS 'Get all structure descendants in flat format with nesting level indication. Useful for analyzing deep hierarchies.';


-- ===== 00_module_init.sql =====
-- =====================================================================
-- v2-pvt module init
-- =====================================================================
-- Purpose: PVT-based search engine for REDB free (PostgreSQL).
-- Owner  : redb core team. Forked helpers in 01..07 mirror legacy
--          redb_facets_search.sql / redb_lazy_loading_search.sql.
-- Version: see pvt_module_version() at the bottom of this file.
--
-- This file must be applied FIRST. It performs three things:
--   1. Verifies that system infrastructure of REDB is in place
--      (core tables and two system functions).
--   2. Drops every function this module owns (CASCADE) so the module
--      can be redeployed cleanly.
--   3. Creates pvt_module_version() — used by the C# client to verify
--      compatibility on InitializeAsync(). No runtime fallback.
-- =====================================================================

-- ---------- 1. System infrastructure check ------------------------------
DO $$
BEGIN
    -- Required system function: scheme metadata reader. Source lives in
    -- redbPostgre.sql; ships in the generated bundle redb_init.sql.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'get_scheme_definition'
          AND n.nspname = 'public'
    ) THEN
        RAISE EXCEPTION
            'v2-pvt: required system function public.get_scheme_definition(bigint) is missing. Deploy the REDB core schema first (redbPostgre.sql / generated redb_init.sql).';
    END IF;

    -- Required system function: object JSON materializer.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = 'get_object_json'
          AND n.nspname = 'public'
    ) THEN
        RAISE EXCEPTION
            'v2-pvt: required system function public.get_object_json(bigint, integer) is missing. Deploy the REDB core schema first (redbPostgre.sql / generated redb_init.sql).';
    END IF;

    -- Required core tables.
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_objects') THEN
        RAISE EXCEPTION 'v2-pvt: required table public._objects is missing.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_values') THEN
        RAISE EXCEPTION 'v2-pvt: required table public._values is missing.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_structures') THEN
        RAISE EXCEPTION 'v2-pvt: required table public._structures is missing.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_list_items') THEN
        RAISE EXCEPTION 'v2-pvt: required table public._list_items is missing.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name = '_scheme_metadata_cache') THEN
        RAISE EXCEPTION
            'v2-pvt: required cache table public._scheme_metadata_cache is missing. Deploy redb_metadata_cache.sql first.';
    END IF;
END $$;

-- ---------- 2. DROP every pvt_* function this module owns ---------------
-- Universal drop: enumerate all functions in the public schema whose name
-- starts with `pvt_` and drop them with their actual signatures. This
-- protects the module against signature drift between releases.
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT p.oid::regprocedure::text AS sig
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname LIKE 'pvt\_%' ESCAPE '\'
    LOOP
        EXECUTE 'DROP FUNCTION IF EXISTS ' || r.sig || ' CASCADE';
    END LOOP;
END $$;

-- ---------- 3. Module version function ---------------------------------
-- semver: bump MAJOR on breaking changes to entry-point signatures or
-- result shape; bump MINOR on additive features; bump PATCH on bug fixes.
CREATE OR REPLACE FUNCTION pvt_module_version()
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
BEGIN
    -- 0.6.2 — Nested-dict object-set pushdown (mixed scalar+nested):
    --   * 12_pvt_cte_builder.sql now folds the object-set restriction
    --     (scheme + base pushdown + tree filter) into every
    --     nested_dict_N CTE's WHERE, not just the nested-only path.
    --     Mixed scalar+nested queries were previously scanning the
    --     full parent_sid partition of _values and gating via the
    --     outer JOIN; with this change PG prunes dp rows by
    --     _id_scheme BEFORE the LEFT JOIN nv expansion.
    -- 0.6.1 — ListItem.Value/.Alias Pro-parity perf:
    --   * pvt_build_cte_sql and the inline GROUP BY path in
    --     pvt_build_groupby_sql now emit a single
    --     `LEFT JOIN _list_items li ON li._id = v._ListItem` on the
    --     pivot source whenever any field projects `list_item_prop`
    --     in (Value, Alias). Per-column expressions reference bare
    --     `li._value` / `li._alias` and aggregate via
    --     `array_agg(li.<col>) FILTER (...)`.
    --   * Replaces N per-column correlated subselects
    --     `(SELECT li._value FROM _list_items li WHERE li._id = v._ListItem)`
    --     with one JOIN per pivot — matches Pro PivotSqlGenerator.
    --   * Scalar Value/Alias pivot column still holds resolved text
    --     (Free LINQ passes string literals; comparison is `= '...'::text`).
    -- 0.6.0 — Pro-parity perf rewrite (large-scale ops):
    --   * #1 Filter pushdown: pvt_split_filter detects narrow filter sets
    --     that contain no base refs and inlines the residual WHERE
    --     inside _pvt_cte with an explicit `SELECT pvt._id_object, pvt."col", ...`
    --     wrapper (pvt_filter_has_base_refs gate + explicit-cols
    --     projection in pvt_build_cte_sql). Outer WHERE collapses to TRUE.
    --   * #2 GROUP BY inline subquery: pvt_build_groupby_sql now skips
    --     the CTE for pure-scalar narrow shapes and emits
    --     `SELECT pvt.<grp>, agg(...) FROM (<inline pivot>) pvt`.
    --     `v._array_index IS NULL` is lifted from per-column FILTER into
    --     the inline subquery's outer WHERE — index-friendly at 100M+ rows.
    --     pvt_build_column_expr gained p_array_index_in_outer for this.
    --   * #3 Nested-dict side CTE: a single LEFT JOIN _values + per-field
    --     `array_agg(...) FILTER (...)` replaces N correlated subselects.
    --     SID list collapses to `IN (...)` (or `= sid`) with dedup.
    -- 0.5.0 — Expression engine (Pro parity, capability):
    --   * 17_pvt_expr.sql introduces pvt_build_scalar_expr (recursive
    --     compiler for $field/$const/arithmetic/Math/String/Concat/
    --     Coalesce/Cast) and pvt_build_expr_predicate (full predicate
    --     family $eq..$gte / $like / $ilike / $in / $nin / $between /
    --     $null / $notNull / $contains[IgnoreCase] / $startsWith / $endsWith).
    --   * pvt_build_where_from_json and pvt_split_filter route
    --     filter-level expression-form predicates through the new engine.
    --   * pvt_extract_field_pairs harvests $field references from
    --     expression subtrees so pvt_collect_fields resolves them.
    --   * Pushdown: expression predicates are pushed iff every $field
    --     reference inside resolves to kind=base (pvt_expr_is_base_only).
    -- 0.4.0 — Base-field pushdown (Pro parity, perf):
    --   * pvt_split_filter walks the filter and peels off base/hierarchical
    --     predicates into a SQL fragment over `_objects o.*`.
    --   * pvt_build_cte_sql accepts p_extra_where and ANDs it into the
    --     inner WHERE so PG can use system-column indexes BEFORE the
    --     JOIN with _values and the GROUP BY agg.
    --   * pvt_build_field_condition gained p_base_prefix; passed as 'o.'
    --     in pushdown context, '' (default) in the outer CTE WHERE.
    --   * $or/$not are pushed only when every leaf inside is base —
    --     mixed branches keep the original semantics.
    -- 0.3.0 — Pro parity rewrite:
    --   * `(array_agg(v.<col>) FILTER (...))[1]` idiom (works for bool/uuid/etc).
    --   * `_array_index IS NULL` filter for scalars (NOT `_array_parent_id IS NULL`).
    --   * `0$:` base-field prefix stripping in pvt_normalize_base_field_name.
    --   * full collection / nested / dictionary / ListItem.Value/Alias / array-op support.
    RETURN '0.6.2';
END;
$BODY$;

COMMENT ON FUNCTION pvt_module_version() IS
    'Returns the semver of the v2-pvt module. Used by the C# client on InitializeAsync to enforce compatibility (major must match, deployed minor >= required).';

-- ---------- 4. Shared legacy helpers used by pvt_* code ----------------
-- Forked verbatim from sql/deprecated/redb_facets_search.sql. They are
-- referenced by pvt_build_inner_condition / pvt_build_single_facet_condition
-- and were left in the legacy file before the PG free path was rewritten on
-- top of v2-pvt. Kept here (not under deprecated/) so the module is fully
-- self-contained — the bundled redb_init.sql no longer ships the legacy
-- facets_search file. Names keep the underscore prefix to avoid touching
-- every call site inside the pvt_* functions.

DROP TYPE IF EXISTS structure_info_type CASCADE;
CREATE TYPE structure_info_type AS (
    root_structure_id bigint,
    nested_structure_id bigint,
    root_type_info jsonb,
    nested_type_info jsonb
);

CREATE OR REPLACE FUNCTION _format_json_array_for_in(
    array_data jsonb
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    in_values text := '';
    json_element jsonb;
    first_item boolean := true;
    element_text text;
BEGIN
    IF jsonb_typeof(array_data) != 'array' THEN
        RAISE EXCEPTION 'JSON array expected, got: %', jsonb_typeof(array_data);
    END IF;

    FOR json_element IN SELECT value FROM jsonb_array_elements(array_data) LOOP
        IF NOT first_item THEN
            in_values := in_values || ', ';
        END IF;
        first_item := false;

        CASE jsonb_typeof(json_element)
            WHEN 'string' THEN
                element_text := quote_literal(json_element #>> '{}');
            WHEN 'number' THEN
                element_text := json_element::text;
            WHEN 'boolean' THEN
                element_text := CASE WHEN (json_element)::boolean THEN 'true' ELSE 'false' END;
            ELSE
                element_text := quote_literal(json_element #>> '{}');
        END CASE;

        in_values := in_values || element_text;
    END LOOP;

    RETURN in_values;
END;
$BODY$;

COMMENT ON FUNCTION _format_json_array_for_in(jsonb) IS
    'Converts JSONB array to string of values for SQL IN clause. Forked from redb_facets_search.sql into the v2-pvt module bundle (00_module_init.sql).';

-- pvt_resolve_field_path_table: TABLE-returning resolver used by
-- 26_pvt_array_groupby.sql. Forked verbatim from
-- sql/deprecated/redb_aggregation.sql (resolve_field_path). The PVT module
-- also ships pvt_resolve_field_path(bigint, text) RETURNS jsonb (see
-- 01_pvt_field_path.sql) — that one mirrors C# SchemeFieldResolver and is
-- used by the rest of pvt_*. Keep both: the table form is what the
-- array_groupby builder consumes (structure_id / db_type / is_array /
-- array_index / dict_key / is_dictionary).
CREATE OR REPLACE FUNCTION pvt_resolve_field_path_table(
    p_scheme_id bigint,
    p_field_path text
)
RETURNS TABLE(structure_id bigint, db_type text, is_array boolean, array_index int, dict_key text, is_dictionary boolean)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    v_segments text[];
    v_segment text;
    v_clean_segment text;
    v_current_parent_id bigint := NULL;
    v_structure_id bigint;
    v_db_type text;
    v_is_collection boolean := false;
    v_is_dictionary boolean := false;
    v_found_collection_type bigint;
    v_array_index int := NULL;
    v_dict_key text := NULL;
    v_index_match text[];
    v_key_match text[];
    v_collection_type_name text;
BEGIN
    v_index_match := regexp_match(p_field_path, '\[(\d+)\]');
    IF v_index_match IS NOT NULL THEN
        v_array_index := v_index_match[1]::int;
    END IF;

    v_key_match := regexp_match(p_field_path, '\[([A-Za-z_][A-Za-z0-9_-]*)\]');
    IF v_key_match IS NOT NULL THEN
        v_dict_key := v_key_match[1];
    END IF;

    v_segments := string_to_array(regexp_replace(p_field_path, '\[[^\]]*\]', '', 'g'), '.');

    FOREACH v_segment IN ARRAY v_segments
    LOOP
        v_clean_segment := trim(v_segment);
        IF v_clean_segment = '' THEN
            CONTINUE;
        END IF;

        SELECT c._structure_id, c.db_type, c._collection_type
        INTO v_structure_id, v_db_type, v_found_collection_type
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = p_scheme_id
          AND c._name = v_clean_segment
          AND (
              (v_current_parent_id IS NULL AND c._parent_structure_id IS NULL)
              OR c._parent_structure_id = v_current_parent_id
          )
        LIMIT 1;

        IF v_structure_id IS NULL THEN
            RAISE EXCEPTION 'Field segment "%" not found in path "%" (scheme=%). Check cache: SELECT * FROM warmup_all_metadata_caches();',
                v_clean_segment, p_field_path, p_scheme_id;
        END IF;

        IF v_found_collection_type IS NOT NULL THEN
            v_is_collection := true;
            SELECT t._name INTO v_collection_type_name
            FROM _types t WHERE t._id = v_found_collection_type;
            IF v_collection_type_name = 'Dictionary' THEN
                v_is_dictionary := true;
            END IF;
        END IF;

        v_current_parent_id := v_structure_id;
    END LOOP;

    structure_id := v_structure_id;
    db_type := v_db_type;
    is_array := v_is_collection OR (p_field_path ~ '\[[^\]]*\]');
    array_index := v_array_index;
    dict_key := v_dict_key;
    is_dictionary := v_is_dictionary;
    RETURN NEXT;
END;
$BODY$;

COMMENT ON FUNCTION pvt_resolve_field_path_table(bigint, text) IS
    'TABLE-returning field-path resolver forked from redb_aggregation.sql. Consumed by pvt_build_array_groupby_sql (26_pvt_array_groupby.sql).';

DO $$
BEGIN
    RAISE NOTICE 'v2-pvt module init OK, version: %', pvt_module_version();
END $$;


-- ===== 01_pvt_field_path.sql =====
-- =====================================================================
-- pvt_normalize_base_field_name: map C# base field names to _objects columns
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L51 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_normalize_base_field_name(field_name text)
RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    v_had_prefix boolean := false;
    v_result     text;
BEGIN
    -- FacetFilterBuilder marks base fields by prepending '0$:' to the raw name.
    -- Strip it before the lookup so '0$:Id', '0$:Name' resolve to base columns.
    IF field_name LIKE '0$:%' THEN
        v_had_prefix := true;
        field_name := substring(field_name from 4);
    END IF;

    v_result := CASE field_name
        -- ID fields
        WHEN 'id' THEN '_id'
        WHEN 'Id' THEN '_id'
        WHEN '_id' THEN '_id'
        WHEN 'parent_id' THEN '_id_parent'
        WHEN 'ParentId' THEN '_id_parent'
        WHEN 'id_parent' THEN '_id_parent'
        WHEN '_id_parent' THEN '_id_parent'
        WHEN 'scheme_id' THEN '_id_scheme'
        WHEN 'SchemeId' THEN '_id_scheme'
        WHEN 'id_scheme' THEN '_id_scheme'
        WHEN '_id_scheme' THEN '_id_scheme'
        WHEN 'owner_id' THEN '_id_owner'
        WHEN 'OwnerId' THEN '_id_owner'
        WHEN '_id_owner' THEN '_id_owner'
        WHEN 'who_change_id' THEN '_id_who_change'
        WHEN 'WhoChangeId' THEN '_id_who_change'
        WHEN '_id_who_change' THEN '_id_who_change'
        -- Value fields (RedbPrimitive<T> support)
        WHEN 'value_long' THEN '_value_long'
        WHEN 'ValueLong' THEN '_value_long'
        WHEN '_value_long' THEN '_value_long'
        WHEN 'value_string' THEN '_value_string'
        WHEN 'ValueString' THEN '_value_string'
        WHEN '_value_string' THEN '_value_string'
        WHEN 'value_guid' THEN '_value_guid'
        WHEN 'ValueGuid' THEN '_value_guid'
        WHEN '_value_guid' THEN '_value_guid'
        -- Other base fields
        WHEN 'key' THEN '_key'
        WHEN 'Key' THEN '_key'
        WHEN '_key' THEN '_key'
        WHEN 'name' THEN '_name'
        WHEN 'Name' THEN '_name'
        WHEN '_name' THEN '_name'
        WHEN 'note' THEN '_note'
        WHEN 'Note' THEN '_note'
        WHEN '_note' THEN '_note'
        WHEN 'value_bool' THEN '_value_bool'
        WHEN 'ValueBool' THEN '_value_bool'
        WHEN '_value_bool' THEN '_value_bool'
        -- New RedbPrimitive<T> value fields
        WHEN 'value_double' THEN '_value_double'
        WHEN 'ValueDouble' THEN '_value_double'
        WHEN '_value_double' THEN '_value_double'
        WHEN 'value_numeric' THEN '_value_numeric'
        WHEN 'ValueNumeric' THEN '_value_numeric'
        WHEN '_value_numeric' THEN '_value_numeric'
        WHEN 'value_datetime' THEN '_value_datetime'
        WHEN 'ValueDatetime' THEN '_value_datetime'
        WHEN '_value_datetime' THEN '_value_datetime'
        WHEN 'value_bytes' THEN '_value_bytes'
        WHEN 'ValueBytes' THEN '_value_bytes'
        WHEN '_value_bytes' THEN '_value_bytes'
        WHEN 'hash' THEN '_hash'
        WHEN 'Hash' THEN '_hash'
        WHEN '_hash' THEN '_hash'
        -- DateTime fields
        WHEN 'date_create' THEN '_date_create'
        WHEN 'DateCreate' THEN '_date_create'
        WHEN '_date_create' THEN '_date_create'
        WHEN 'date_modify' THEN '_date_modify'
        WHEN 'DateModify' THEN '_date_modify'
        WHEN '_date_modify' THEN '_date_modify'
        WHEN 'date_begin' THEN '_date_begin'
        WHEN 'DateBegin' THEN '_date_begin'
        WHEN '_date_begin' THEN '_date_begin'
        WHEN 'date_complete' THEN '_date_complete'
        WHEN 'DateComplete' THEN '_date_complete'
        WHEN '_date_complete' THEN '_date_complete'
        -- Not a base field - return NULL
        ELSE NULL
    END;

    -- Disambiguation: bare names like `Name`, `Id`, `Key` collide with
    -- legitimate user-defined Props fields of the same name. Only honor
    -- the base-column mapping when the caller explicitly opted in via
    -- the `0$:` prefix, OR the input already starts with an underscore
    -- (system column naming convention).
    IF v_result IS NOT NULL
       AND NOT v_had_prefix
       AND substring(field_name from 1 for 1) <> '_' THEN
        RETURN NULL;
    END IF;

    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION pvt_normalize_base_field_name(text) IS 'Normalizes C# base field names to SQL column names in _objects. Returns NULL if field is not a base field.';

-- =====================================================================
-- pvt_parse_field_path: split dotted/bracketed field paths into components
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L185 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_parse_field_path(
    field_path text
) RETURNS TABLE (
    root_field text,
    nested_field text, 
    is_array boolean,
    is_nested boolean,
    dict_key text  -- NEW: Dictionary key for AddressBook[home] -> 'home'
)
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    bracket_pos int;
    dot_after_bracket int;
    key_end int;
BEGIN
    -- Initialize dict_key as NULL
    dict_key := NULL;
    
    -- Determine if field is array (contains [] WITHOUT key inside)
    is_array := field_path LIKE '%[]%';
    
    -- Check for Dictionary path: AddressBook[home] or AddressBook[home].City
    -- Dictionary differs from array by having non-empty key in brackets
    IF field_path ~ '\[[^\]]+\]' AND NOT is_array THEN
        -- This is a Dictionary path: AddressBook[home] or AddressBook[home].City
        bracket_pos := position('[' in field_path);
        key_end := position(']' in field_path);
        
        -- Extract root field (before bracket)
        root_field := substring(field_path from 1 for bracket_pos - 1);
        
        -- Extract dict key (inside brackets)
        dict_key := substring(field_path from bracket_pos + 1 for key_end - bracket_pos - 1);
        
        -- Check for nested field after bracket: AddressBook[home].City
        dot_after_bracket := position('.' in substring(field_path from key_end + 1));
        IF dot_after_bracket > 0 THEN
            nested_field := substring(field_path from key_end + 2); -- +2 to skip '].'
            is_nested := true;
        ELSE
            nested_field := NULL;
            is_nested := false;
        END IF;
        
        RETURN QUERY SELECT root_field, nested_field, false, is_nested, dict_key;
        RETURN;
    END IF;
    
    -- Determine if field is nested (contains dot)
    is_nested := field_path LIKE '%.%';
    
    IF is_nested THEN
        IF is_array THEN
            -- Case: "Contacts[].Email" -> root="Contacts", nested="Email", is_array=true
            root_field := split_part(replace(field_path, '[]', ''), '.', 1);
            nested_field := split_part(replace(field_path, '[]', ''), '.', 2);
        ELSE
            -- Case: "Contact.Name" -> root="Contact", nested="Name", is_array=false  
            root_field := split_part(field_path, '.', 1);
            nested_field := split_part(field_path, '.', 2);
        END IF;
    ELSE
        IF is_array THEN
            -- Case: "Tags[]" -> root="Tags", nested=NULL, is_array=true
            root_field := replace(field_path, '[]', '');
            nested_field := NULL;
        ELSE
            -- Case: "Name" -> root="Name", nested=NULL, is_array=false
            root_field := field_path;
            nested_field := NULL;
        END IF;
    END IF;
    
    RETURN QUERY SELECT root_field, nested_field, is_array, is_nested, dict_key;
END;
$BODY$;

COMMENT ON FUNCTION pvt_parse_field_path(text) IS 'Parses field path to support Class fields, arrays and Dictionary. Supports: "Name", "Contact.Name", "Tags[]", "Contacts[].Email", "PhoneBook[home]", "AddressBook[home].City". Returns path components for further processing.';


-- =====================================================================
-- pvt_normalize_field_name: rewrite `<dict>.ContainsKey` predicate paths
-- ---------------------------------------------------------------------
-- Mirrors redb.Core.Pro.Query.ProSqlBuilderBase.NormalizeDictionaryFieldName:
-- when the C# predicate is `Dict.ContainsKey("home")`, the builder emits a
-- field name of "<Dict>.ContainsKey" with a string operand "home". The PVT
-- layer rewrites that to "<Dict>[home]" so the existing dictionary-pivot
-- machinery handles it and the operator collapses to IS NOT NULL.
--
-- Returns the rewritten path. When the conditions are not met, returns
-- the input path unchanged.
-- =====================================================================
CREATE OR REPLACE FUNCTION pvt_normalize_field_name(
    p_path text,
    p_op_value text
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
DECLARE
    v_base text;
BEGIN
    IF p_path IS NULL THEN
        RETURN NULL;
    END IF;
    IF right(p_path, length('.ContainsKey')) <> '.ContainsKey' THEN
        RETURN p_path;
    END IF;
    IF p_op_value IS NULL OR p_op_value = '' THEN
        RETURN p_path;
    END IF;

    v_base := left(p_path, length(p_path) - length('.ContainsKey'));
    RETURN v_base || '[' || p_op_value || ']';
END;
$BODY$;

COMMENT ON FUNCTION pvt_normalize_field_name(text, text) IS
    'If path ends with ".ContainsKey" and a string operand is provided, rewrites to "<base>[<key>]" so dictionary-pivot path resolution kicks in. Mirrors Pro.ProSqlBuilderBase.NormalizeDictionaryFieldName.';


-- =====================================================================
-- pvt_peek_contains_key_value: extract a string operand for ContainsKey
-- ---------------------------------------------------------------------
-- The operand of `Dict.ContainsKey(...)` can arrive either as a bare
-- JSON string (shorthand $eq) or as an object `{ "$eq": "<key>" }`.
-- Returns the underlying text, or NULL if the operand is not a string.
-- =====================================================================
CREATE OR REPLACE FUNCTION pvt_peek_contains_key_value(p_op jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
DECLARE
    v jsonb;
BEGIN
    IF p_op IS NULL THEN
        RETURN NULL;
    END IF;
    IF jsonb_typeof(p_op) = 'string' THEN
        RETURN p_op #>> '{}';
    END IF;
    IF jsonb_typeof(p_op) = 'object' THEN
        v := p_op -> '$eq';
        IF v IS NOT NULL AND jsonb_typeof(v) = 'string' THEN
            RETURN v #>> '{}';
        END IF;
    END IF;
    RETURN NULL;
END;
$BODY$;

COMMENT ON FUNCTION pvt_peek_contains_key_value(jsonb) IS
    'Returns the string operand of a ContainsKey predicate (bare string or {"$eq": "..."}), else NULL.';



-- ===== 02_pvt_type_info.sql =====
-- =====================================================================
-- pvt_get_listitem_field_type_info: resolve type info for ListItem-typed fields
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L268 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_get_listitem_field_type_info(field_name text)
RETURNS jsonb
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
BEGIN
    CASE field_name
        WHEN 'Value' THEN 
            RETURN jsonb_build_object('db_type', 'String', 'type_semantic', 'String', 'is_array', false);
        WHEN 'Alias' THEN 
            RETURN jsonb_build_object('db_type', 'String', 'type_semantic', 'String', 'is_array', false);
        WHEN 'IdObject' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        WHEN 'IdList' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        WHEN 'Id' THEN 
            RETURN jsonb_build_object('db_type', 'Long', 'type_semantic', 'Long', 'is_array', false);
        ELSE
            RETURN NULL;
    END CASE;
END;
$BODY$;



-- ===== 03_pvt_structure_info.sql =====
-- =====================================================================
-- pvt_find_structure_info: look up structure metadata by field path
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L291 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_find_structure_info(
    scheme_id bigint,
    root_field text,
    nested_field text DEFAULT NULL
) RETURNS TABLE (
    root_structure_id bigint,
    nested_structure_id bigint,
    root_type_info jsonb,
    nested_type_info jsonb
)
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF  
AS $BODY$
DECLARE
    scheme_def jsonb;
BEGIN
    -- Get scheme definition using existing function
    SELECT get_scheme_definition(scheme_id) INTO scheme_def;
    
    -- Find root structure
    SELECT 
        (struct->>'_id')::bigint,
        jsonb_build_object(
            'type_name', struct->>'_type_name',
            'db_type', struct->>'_type_db_type', 
            'type_semantic', struct->>'_type_dotnet_type',
            'is_array', (struct->>'_collection_type')::bigint IS NOT NULL
        )
    INTO root_structure_id, root_type_info
    FROM jsonb_array_elements(scheme_def->'structures') AS struct
    WHERE struct->>'_name' = root_field
      AND struct->>'_id_parent' IS NULL;
    
    -- If there is a nested field, find its structure
    IF nested_field IS NOT NULL AND root_structure_id IS NOT NULL THEN
        -- 🎯 NEW: Special handling for ListItem fields
        IF root_type_info->>'type_semantic' = '_RListItem' THEN
            -- For ListItem nested fields (Value, Alias, etc.) are not structures
            -- They are stored as columns in _list_items
            nested_structure_id := NULL;
            nested_type_info := pvt_get_listitem_field_type_info(nested_field);
        ELSE
            -- Normal logic for other types
            SELECT 
                (struct->>'_id')::bigint,
                jsonb_build_object(
                    'type_name', struct->>'_type_name',
                    'db_type', struct->>'_type_db_type',
                    'type_semantic', struct->>'_type_dotnet_type', 
                    'is_array', (struct->>'_collection_type')::bigint IS NOT NULL
                )
            INTO nested_structure_id, nested_type_info
            FROM jsonb_array_elements(scheme_def->'structures') AS struct
            WHERE struct->>'_name' = nested_field
              AND (struct->>'_id_parent')::bigint = root_structure_id;
        END IF;
    ELSE
        nested_structure_id := NULL;
        nested_type_info := NULL;
    END IF;
    
    RETURN QUERY SELECT root_structure_id, nested_structure_id, root_type_info, nested_type_info;
END;
$BODY$;

COMMENT ON FUNCTION pvt_find_structure_info(bigint, text, text) IS 'Finds structure information for Class fields using get_scheme_definition. Returns structure IDs and type metadata for root and nested fields.';



-- ===== 04_pvt_inner_condition.sql =====
-- =====================================================================
-- pvt_build_inner_condition: build SQL operator/value fragment for a typed value column
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L362 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_build_inner_condition(
    operator_name text,
    operator_value text,
    type_info jsonb  -- Type information from pvt_find_structure_info
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE
AS $BODY$
DECLARE
    op_symbol text;
    pattern text;
    in_values_list text;
    db_type text := type_info->>'db_type';
    is_array boolean := (type_info->>'is_array')::boolean;
BEGIN
    -- Numeric and DateTime operators
    IF operator_name IN ('$gt', '$lt', '$gte', '$lte') THEN
        CASE operator_name
            WHEN '$gt' THEN op_symbol := '>';
            WHEN '$lt' THEN op_symbol := '<';
            WHEN '$gte' THEN op_symbol := '>=';
            WHEN '$lte' THEN op_symbol := '<=';
        END CASE;
        
        -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            -- TimeSpan: convert to INTERVAL for correct comparison
            RETURN format('fv._String::interval %s %L::interval', op_symbol, operator_value);
        
        ELSIF operator_value ~ '^\d{4}-\d{2}-\d{2}' OR db_type = 'DateTimeOffset' THEN
            RETURN format('fv._DateTimeOffset %s %L::timestamptz', op_symbol, operator_value);
        ELSIF db_type = 'Long' THEN
            -- Known Long type (int, long)
            RETURN format('fv._Long %s %L::bigint', op_symbol, operator_value);
        ELSIF db_type = 'Double' THEN
            -- Known Double type (float, double)
            RETURN format('fv._Double %s %L::double precision', op_symbol, operator_value);
        ELSIF db_type = 'Numeric' THEN
            -- Known Numeric type (decimal)
            RETURN format('fv._Numeric %s %L::numeric', op_symbol, operator_value);
        ELSE
            -- Type unknown - check all numeric types (fallback)
            RETURN format('((fv._Long %s %L::bigint) OR (fv._Double %s %L::double precision) OR (fv._Numeric %s %L::numeric))',
                op_symbol, operator_value, op_symbol, operator_value, op_symbol, operator_value);
        END IF;
    
    -- String operators (case-sensitive)
    -- 🚀 OPTIMIZATION: removed fs.db_type check - type already known for string operators
    ELSIF operator_name IN ('$startsWith', '$endsWith', '$contains') THEN
        CASE operator_name
            WHEN '$startsWith' THEN pattern := operator_value || '%';
            WHEN '$endsWith' THEN pattern := '%' || operator_value;
            WHEN '$contains' THEN pattern := '%' || operator_value || '%';
        END CASE;
        
        RETURN format('fv._String LIKE %L', pattern);
    
    -- String operators (case-insensitive)
    -- 🚀 OPTIMIZATION: removed fs.db_type check
    ELSIF operator_name IN ('$startsWithIgnoreCase', '$endsWithIgnoreCase', '$containsIgnoreCase') THEN
        CASE operator_name
            WHEN '$startsWithIgnoreCase' THEN pattern := operator_value || '%';
            WHEN '$endsWithIgnoreCase' THEN pattern := '%' || operator_value;
            WHEN '$containsIgnoreCase' THEN pattern := '%' || operator_value || '%';
        END CASE;
        
        RETURN format('fv._String ILIKE %L', pattern);
    
    -- IN operator
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$in' THEN
        in_values_list := _format_json_array_for_in(operator_value::jsonb);
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            -- TimeSpan: convert to INTERVAL for correct comparison
            RETURN format('fv._String::interval IN (%s)', 
                regexp_replace(in_values_list, '([^,]+)', '\1::interval', 'g'));
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            -- ListItem: values stored in _listitem column as list element IDs
            RETURN format('fv._listitem IN (%s)', in_values_list);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String IN (%s)', in_values_list);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long IN (%s)', in_values_list);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double IN (%s)', in_values_list);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric IN (%s)', in_values_list);
        ELSIF db_type = 'Boolean' THEN
            RETURN format('fv._Boolean IN (%s)', in_values_list);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('fv._DateTimeOffset IN (%s)', in_values_list);
        ELSE
            -- Fallback: try all types (only if type unknown)
            RETURN format('(fv._String IN (%s) OR fv._Long IN (%s) OR fv._Double IN (%s) OR fv._Numeric IN (%s) OR fv._Boolean IN (%s) OR fv._DateTimeOffset IN (%s))',
                in_values_list, in_values_list, in_values_list, in_values_list, in_values_list, in_values_list);
        END IF;
    
    -- NOT EQUAL operator - requires special handling
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$ne' THEN
        -- For $ne null this is a special case - look for existing records (in EAV null = no record)
        IF operator_value IS NULL OR operator_value = 'null' OR operator_value = '' THEN
            RETURN 'TRUE';  -- Any existing record means "not null"
        ELSE
            -- $ne specific value - build positive condition for negation via NOT EXISTS
            IF type_info->>'type_semantic' = '_RListItem' THEN
                RETURN format('fv._listitem = %L::bigint', operator_value);
            ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
                RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
            ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
                RETURN format('fv._Guid = %L::uuid', operator_value);
            ELSIF db_type = 'Long' THEN
                RETURN format('fv._Long = %L::bigint', operator_value);
            ELSIF db_type = 'Double' THEN
                RETURN format('fv._Double = %L::double precision', operator_value);
            ELSIF db_type = 'Numeric' THEN
                RETURN format('fv._Numeric = %L::numeric', operator_value);
            ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
                RETURN format('fv._Boolean = %L::boolean', operator_value);
            ELSIF db_type = 'String' THEN
                RETURN format('fv._String = %L', operator_value);
            ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
                -- Fallback: numeric value, type unknown
                IF operator_value ~ '^-?\d+$' THEN
                    RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                        operator_value, operator_value, operator_value);
                ELSE
                    RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                        operator_value, operator_value);
                END IF;
            ELSE
                RETURN format('fv._String = %L', operator_value);
            END IF;
        END IF;
    
    -- Explicit equality operator
    -- 🚀 OPTIMIZATION: removed fs.db_type checks - use db_type from type_info directly
    ELSIF operator_name = '$eq' THEN
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            RETURN format('fv._listitem = %L::bigint', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
        ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            RETURN format('fv._Guid = %L::uuid', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric = %L::numeric', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String = %L', operator_value);
        ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
            -- Fallback: numeric value, type unknown
            IF operator_value ~ '^-?\d+$' THEN
                RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value, operator_value);
            ELSE
                RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value);
            END IF;
        ELSE
            RETURN format('fv._String = %L', operator_value);
        END IF;
    
    -- 🚀 EXTENDED RELATIONAL ARRAY OPERATORS
    -- ✅ OPTIMIZATION: Simple condition instead of nested EXISTS
    -- _build_exists_condition already creates fv context with fv._array_index IS NOT NULL
    ELSIF operator_name = '$arrayContains' THEN
        -- 🎯 Search value in relational array with SMART typing
        IF db_type = 'Long' OR (operator_value ~ '^-?\d+$' AND type_info->>'type_semantic' != '_RListItem') THEN
            -- Numeric value (Long)
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            -- Boolean value
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            -- DateTime value
            RETURN format('fv._DateTimeOffset = %L::timestamp', operator_value);
        ELSIF type_info->>'type_semantic' = '_RListItem' THEN
            -- ListItem array: search by _listitem column (stores ListItem ID)
            RETURN format('fv._listitem = %L::bigint', operator_value);
        ELSE
            -- String array (default)
            RETURN format('fv._String = %L', operator_value);
        END IF;
    
    -- Non-empty array check operator  
    ELSIF operator_name = '$arrayAny' THEN
        RETURN 'EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        )';
    
    -- Empty array check operator
    ELSIF operator_name = '$arrayEmpty' THEN
        RETURN 'NOT EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure  
              AND av._array_index IS NOT NULL
        )';
    
    -- 📊 ARRAY ELEMENT COUNT OPERATORS
    ELSIF operator_name = '$arrayCount' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) = %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountGt' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) > %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountGte' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) >= %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountLt' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) < %L::int', operator_value::int);
    
    ELSIF operator_name = '$arrayCountLte' THEN
        RETURN format('(
            SELECT COUNT(*) FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
        ) <= %L::int', operator_value::int);
    
    -- 🎯 OPERATORS FOR RELATIONAL ARRAYS
    ELSIF operator_name = '$arrayAt' THEN
        -- Get array element by index
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index = %L
        )', operator_value::text);
    
    ELSIF operator_name = '$arrayFirst' THEN
        -- Check first array element
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'String' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._String = %L)', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Long = %L::bigint)', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Double = %L::double precision)', operator_value);
        ELSIF db_type = 'Boolean' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._Boolean = %L::boolean)', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND av._DateTimeOffset = %L::timestamp)', operator_value);
        ELSE
            -- Fallback: check all types
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = ''0'' AND (av._String = %L OR av._Long = %L::bigint OR av._Double = %L::double precision))', operator_value, operator_value, operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayLast' THEN
        -- Check last array element
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'String' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._String = %L)', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._Long = %L::bigint)', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND av._Double = %L::double precision)', operator_value);
        ELSE
            -- Fallback
            RETURN format('EXISTS(SELECT 1 FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index = (SELECT MAX(av2._array_index::int)::text FROM _values av2 WHERE av2._id_object = fv._id_object AND av2._id_structure = fv._id_structure AND av2._array_index IS NOT NULL) AND (av._String = %L OR av._Long = %L::bigint))', operator_value, operator_value);
        END IF;
    
    -- 🔍 ARRAY SEARCH OPERATORS
    -- 🚀 OPTIMIZATION: removed JOIN and fs._collection_type
    ELSIF operator_name = '$arrayStartsWith' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String LIKE %L
        )', operator_value || '%');
    
    ELSIF operator_name = '$arrayEndsWith' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String LIKE %L
        )', '%' || operator_value);
    
    ELSIF operator_name = '$arrayMatches' THEN
        RETURN format('EXISTS(
            SELECT 1 FROM _values av
            WHERE av._id_object = fv._id_object
              AND av._id_structure = fv._id_structure
              AND av._array_index IS NOT NULL
              AND av._String ~ %L
        )', operator_value);
    
    -- 📈 ARRAY AGGREGATION OPERATORS
    -- 🚀 OPTIMIZATION: use db_type from type_info
    ELSIF operator_name = '$arraySum' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Long), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Double), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT COALESCE(SUM(av._Numeric), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSE
            -- Fallback: Long + Double
            RETURN format('(SELECT COALESCE(SUM(COALESCE(av._Long, 0) + COALESCE(av._Double, 0)), 0) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayAvg' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT AVG(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT AVG(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT AVG(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSE
            RETURN format('(SELECT AVG(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayMin' THEN
        -- 🚀 OPTIMIZATION: use db_type from type_info
        IF db_type = 'Long' THEN
            RETURN format('(SELECT MIN(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT MIN(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT MIN(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('(SELECT MIN(av._DateTimeOffset) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::timestamp', operator_value);
        ELSE
            RETURN format('(SELECT MIN(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSIF operator_name = '$arrayMax' THEN
        IF db_type = 'Long' THEN
            RETURN format('(SELECT MAX(av._Long) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('(SELECT MAX(av._Double) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('(SELECT MAX(av._Numeric) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        ELSIF db_type = 'DateTimeOffset' THEN
            RETURN format('(SELECT MAX(av._DateTimeOffset) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::timestamp', operator_value);
        ELSE
            RETURN format('(SELECT MAX(COALESCE(av._Long, av._Double)) FROM _values av WHERE av._id_object = fv._id_object AND av._id_structure = fv._id_structure AND av._array_index IS NOT NULL) = %L::numeric', operator_value);
        END IF;
    
    ELSE
        -- 🚀 OPTIMIZATION: Simple equality - use db_type from type_info if known
        IF type_info->>'type_semantic' = 'TimeSpan' THEN
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF db_type = 'DateTimeOffset' OR operator_value ~ '^\d{4}-\d{2}-\d{2}' THEN
            RETURN format('fv._DateTimeOffset = %L::timestamptz', operator_value);
        ELSIF db_type = 'Guid' OR operator_value ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            RETURN format('fv._Guid = %L::uuid', operator_value);
        ELSIF operator_value ~ '^\d+(\.\d+)?:\d{2}:\d{2}' THEN
            -- TimeSpan format (HH:MM:SS)
            RETURN format('fv._String::interval = %L::interval', operator_value);
        ELSIF db_type = 'Long' THEN
            RETURN format('fv._Long = %L::bigint', operator_value);
        ELSIF db_type = 'Double' THEN
            RETURN format('fv._Double = %L::double precision', operator_value);
        ELSIF db_type = 'Numeric' THEN
            RETURN format('fv._Numeric = %L::numeric', operator_value);
        ELSIF db_type = 'Boolean' OR operator_value IN ('true', 'false') THEN
            RETURN format('fv._Boolean = %L::boolean', operator_value);
        ELSIF db_type = 'String' THEN
            RETURN format('fv._String = %L', operator_value);
        ELSIF operator_value ~ '^-?\d+(\.\d+)?$' THEN
            -- Fallback: numeric value, type unknown
            IF operator_value ~ '^-?\d+$' THEN
                RETURN format('(fv._Long = %L::bigint OR fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value, operator_value);
            ELSE
                RETURN format('(fv._Double = %L::double precision OR fv._Numeric = %L::numeric)', 
                    operator_value, operator_value);
            END IF;
        ELSE
            -- String value by default
            RETURN format('fv._String = %L', operator_value);
        END IF;
    END IF;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_inner_condition(text, text, jsonb) IS '🚀 EXTENDED core system of LINQ operators. Supports 25+ operators: 
📊 Numeric: $gt, $gte, $lt, $lte, $ne, $in (Long, Double, Numeric)
📝 String: $contains, $startsWith, $endsWith  
⏱️ TimeSpan: $gt, $lt, $eq (conversion to INTERVAL for correct comparison)
📅 DateTimeOffset: $gt, $lt, $eq (timestamptz with timezone)
🔢 Arrays (basic): $arrayContains, $arrayAny, $arrayEmpty, $arrayCount*
🎯 Arrays (position): $arrayAt, $arrayFirst, $arrayLast
🔍 Arrays (search): $arrayStartsWith, $arrayEndsWith, $arrayMatches
📈 Arrays (aggregation): $arraySum, $arrayAvg, $arrayMin, $arrayMax
All operators adapted for relational arrays via _array_index. Supports distinguishing _RObject vs Object types. Auto type detection by value format. Special handling for TimeSpan via INTERVAL.';



-- ===== 05_pvt_single_facet.sql =====
-- =====================================================================
-- pvt_build_single_facet_condition: build a single field facet WHERE fragment (legacy EXISTS engine; used as fallback for complex ops in PVT)
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L1311 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_build_single_facet_condition(
    facet_condition jsonb,
    scheme_id bigint,
    table_alias text DEFAULT 'o',
    max_depth integer DEFAULT 50
) RETURNS text
LANGUAGE 'plpgsql'
IMMUTABLE  
AS $BODY$
DECLARE
    condition_key text;
    condition_value jsonb;
    field_path text;
    parsed_path RECORD;
    structure_info structure_info_type;
    operator_name text;
    operator_value text;
    inner_condition_sql text;
    all_conditions text := '';
    single_condition text;
BEGIN
    -- Recursion depth check
    IF max_depth <= 0 THEN
        RAISE EXCEPTION 'Maximum recursion depth (50) reached for filter. Simplify JSON filter.';
    END IF;
    
    -- Check input data type
    IF jsonb_typeof(facet_condition) != 'object' THEN
        RETURN '';
    END IF;
    
    -- Process each key-value pair
    FOR condition_key, condition_value IN SELECT * FROM jsonb_each(facet_condition) LOOP
        -- Logical operators - ACCUMULATE instead of RETURN to process ALL keys at same level
        -- NOTE: Pass max_depth WITHOUT decrement, because _build_and/or/not decrement themselves when calling _build_single
        IF condition_key = '$and' THEN
            single_condition := _build_and_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        ELSIF condition_key = '$or' THEN
            single_condition := _build_or_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        ELSIF condition_key = '$not' THEN
            single_condition := _build_not_condition(condition_value, scheme_id, table_alias, max_depth);
            IF single_condition != '' THEN
                single_condition := ltrim(single_condition, ' AND ');
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || single_condition;
                ELSE
                    all_conditions := single_condition;
                END IF;
            END IF;
            CONTINUE;
        
        -- Hierarchical operators (processed separately)
        ELSIF condition_key IN ('$hasAncestor', '$hasDescendant', '$level', '$isRoot', '$isLeaf', '$childrenOf') THEN
            CONTINUE; -- Skip, they are processed in pvt_build_hierarchical_conditions
        
        -- 🆕 Property functions: Field.$length, Field[].$count
        -- p.Name.Length > 3  → {"Name.$length": {"$gt": 3}}
        -- p.Tags.Count >= 5  → {"Tags[].$count": {"$gte": 5}}
        ELSIF condition_key ~ '\.\$length$' OR condition_key ~ '\.\$count$' THEN
            DECLARE
                func_is_length boolean := condition_key ~ '\.\$length$';
                func_field_name text;
                func_structure_id bigint;
                func_condition text;
                func_op_name text;
                func_op_value text;
                func_compare_op text;
            BEGIN
                -- Extract field name: "Name.$length" -> "Name", "Tags[].$count" -> "Tags"
                IF func_is_length THEN
                    func_field_name := regexp_replace(condition_key, '\.\$length$', '');
                ELSE
                    func_field_name := regexp_replace(condition_key, '\[\]\.\$count$', '');
                    func_field_name := regexp_replace(func_field_name, '\.\$count$', '');
                END IF;
                
                -- Find structure
                SELECT _id INTO func_structure_id
                FROM _structures
                WHERE _id_scheme = scheme_id 
                  AND _name = func_field_name
                  AND _id_parent IS NULL;
                
                IF func_structure_id IS NULL THEN
                    CONTINUE; -- Field not found, skip
                END IF;
                
                -- Process operators
                IF jsonb_typeof(condition_value) = 'object' THEN
                    FOR func_op_name, func_op_value IN SELECT key, value #>> '{}' FROM jsonb_each(condition_value) LOOP
                        func_compare_op := CASE func_op_name
                            WHEN '$eq' THEN '='
                            WHEN '$ne' THEN '<>'
                            WHEN '$gt' THEN '>'
                            WHEN '$gte' THEN '>='
                            WHEN '$lt' THEN '<'
                            WHEN '$lte' THEN '<='
                            ELSE '='
                        END;
                        
                        IF func_is_length THEN
                            -- String length: LENGTH(v._String)
                            func_condition := format(
                                'EXISTS (SELECT 1 FROM _values fv WHERE fv._id_object = %I._id AND fv._id_structure = %L AND fv._array_index IS NULL AND LENGTH(fv._String) %s %L::integer)',
                                table_alias, func_structure_id, func_compare_op, func_op_value
                            );
                        ELSE
                            -- Array count
                            func_condition := format(
                                '(SELECT COUNT(*) FROM _values fv WHERE fv._id_object = %I._id AND fv._id_structure = %L AND fv._array_index IS NOT NULL) %s %L::integer',
                                table_alias, func_structure_id, func_compare_op, func_op_value
                            );
                        END IF;
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || func_condition;
                        ELSE
                            all_conditions := func_condition;
                        END IF;
                    END LOOP;
                END IF;
            END;
        
        -- 🚀 _objects TABLE BASE FIELDS (with "0$:" prefix)
        -- 🆕 CRITICAL BUG FIX: Now base fields are EXPLICITLY marked with "0$:" prefix
        -- This resolves name conflicts: Props.Name vs RedbObject.name
        -- Prefix "0$:" is impossible as identifier in any programming language
        -- C# passes: "0$:name", "0$:parent_id", "0$:Id" etc.
        ELSIF condition_key LIKE '0$:%' THEN
            DECLARE
                raw_field_name text := substring(condition_key from 4);  -- remove '0$:'
                sql_field_name text := pvt_normalize_base_field_name(raw_field_name);
                base_condition text := '';
            BEGIN
                -- 🛡️ PROTECTION: Check that field is recognized as base
                IF sql_field_name IS NULL THEN
                    RAISE EXCEPTION 'Unknown RedbObject base field: "%" (passed as "0$:%"). Valid fields: id, parent_id, scheme_id, owner_id, who_change_id, date_create, date_modify, date_begin, date_complete, key, value_long, value_string, value_guid, value_bool, value_double, value_numeric, value_datetime, value_bytes, name, note, hash', 
                        raw_field_name, raw_field_name;
                END IF;
                
                -- Determine field type and build condition
                -- Numeric fields (bigint): _id, _id_parent, _id_scheme, _id_owner, _id_who_change, _value_long, _key
                -- 🛡️ SECURITY: Use %L::bigint instead of %s to protect against SQL injection
                IF sql_field_name IN ('_id', '_id_parent', '_id_scheme', '_id_owner', '_id_who_change', '_value_long', '_key') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::bigint', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::bigint', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::bigint', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::bigint', table_alias, sql_field_name, operator_value)
                                -- 🎯 $exists for base fields: IS NULL / IS NOT NULL
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'array' THEN
                        base_condition := format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value));
                    ELSIF jsonb_typeof(condition_value) = 'number' THEN
                        base_condition := format('%I.%I = %L::bigint', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- String fields (text): _value_string, _name, _note
                ELSIF sql_field_name IN ('_value_string', '_name', '_note') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$contains' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, '%' || operator_value || '%')
                                WHEN '$containsIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, '%' || operator_value || '%')
                                WHEN '$startsWith' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, operator_value || '%')
                                WHEN '$startsWithIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, operator_value || '%')
                                WHEN '$endsWith' THEN format('%I.%I LIKE %L', table_alias, sql_field_name, '%' || operator_value)
                                WHEN '$endsWithIgnoreCase' THEN format('%I.%I ILIKE %L', table_alias, sql_field_name, '%' || operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'array' THEN
                        base_condition := format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value));
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- UUID fields: _value_guid, _hash
                ELSIF sql_field_name IN ('_value_guid', '_hash') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::uuid', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::uuid', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$in' THEN format('%I.%I IN (%s)', table_alias, sql_field_name, _format_json_array_for_in(condition_value->'$in'))
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L::uuid', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- DateTime fields: _date_create, _date_modify, _date_begin, _date_complete
                ELSIF sql_field_name IN ('_date_create', '_date_modify', '_date_begin', '_date_complete', '_value_datetime') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::timestamptz', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::timestamptz', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::timestamptz', table_alias, sql_field_name, operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'string' THEN
                        base_condition := format('%I.%I = %L::timestamptz', table_alias, sql_field_name, condition_value #>> '{}');
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- 🛡️ SECURITY: Boolean field with %L::boolean
                -- Boolean field: _value_bool
                ELSIF sql_field_name = '_value_bool' THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::boolean', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::boolean', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'boolean' THEN
                        base_condition := format('%I.%I = %L::boolean', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- Double/Numeric fields: _value_double, _value_numeric
                ELSIF sql_field_name IN ('_value_double', '_value_numeric') THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$eq' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I = %L::numeric', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$ne' THEN 
                                    CASE WHEN operator_value = 'null' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I != %L::numeric', table_alias, sql_field_name, operator_value)
                                    END
                                WHEN '$gt' THEN format('%I.%I > %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$gte' THEN format('%I.%I >= %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$lt' THEN format('%I.%I < %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$lte' THEN format('%I.%I <= %L::numeric', table_alias, sql_field_name, operator_value)
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF jsonb_typeof(condition_value) = 'number' THEN
                        base_condition := format('%I.%I = %L::numeric', table_alias, sql_field_name, condition_value::text);
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                
                -- Bytes field: _value_bytes (only $exists supported)
                ELSIF sql_field_name = '_value_bytes' THEN
                    IF jsonb_typeof(condition_value) = 'object' THEN
                        FOR operator_name, operator_value IN SELECT key, COALESCE(value #>> '{}', 'null') FROM jsonb_each(condition_value) LOOP
                            single_condition := CASE operator_name
                                WHEN '$exists' THEN 
                                    CASE WHEN operator_value = 'true' THEN format('%I.%I IS NOT NULL', table_alias, sql_field_name)
                                         ELSE format('%I.%I IS NULL', table_alias, sql_field_name)
                                    END
                                ELSE NULL
                            END;
                            IF single_condition IS NOT NULL THEN
                                IF base_condition != '' THEN base_condition := base_condition || ' AND ' || single_condition;
                                ELSE base_condition := single_condition; END IF;
                            END IF;
                        END LOOP;
                    ELSIF condition_value = 'null'::jsonb THEN
                        base_condition := format('%I.%I IS NULL', table_alias, sql_field_name);
                    END IF;
                END IF;
                
                -- ✅ ACCUMULATE base condition in all_conditions (instead of RETURN)
                IF base_condition != '' THEN
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || base_condition;
                    ELSE
                        all_conditions := base_condition;
                    END IF;
                END IF;
            END;
            -- Do NOT RETURN - continue processing other fields!
        
        -- 🆕 Collection != null / == null: Check that Array/Dictionary has/does not have elements
        -- Triggers when condition_value = {"$ne": null} or {"$exists": true/false} for collection field
        -- ⚠️ Only match $ne when the value IS null (not a specific value like "TEST")
        --    Otherwise $ne with specific value must fall through to standard ELSE handler
        ELSIF jsonb_typeof(condition_value) = 'object' 
              AND (
                  (condition_value ? '$ne' AND (condition_value->>'$ne' IS NULL OR condition_value->>'$ne' = 'null'))
                  OR condition_value ? '$exists'
              )
              AND condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
            DECLARE
                coll_field_name text := condition_key;
                coll_structure_id bigint;
                coll_collection_type text;
                coll_condition text;
                coll_is_ne_null boolean;
                eav_ne_null_value text;
                eav_exists_value text;
            BEGIN
                -- Check if this is a Collection field (Array or Dictionary)
                SELECT s._id, s._collection_type INTO coll_structure_id, coll_collection_type
                FROM _structures s
                WHERE s._id_scheme = scheme_id 
                  AND s._name = coll_field_name
                  AND s._id_parent IS NULL;
                
                -- Only handle if it's a Collection (Array or Dictionary)
                IF coll_structure_id IS NOT NULL AND coll_collection_type IS NOT NULL THEN
                    -- Determine if checking for "not null" or "null"
                    coll_is_ne_null := (condition_value->>'$ne' IS NULL AND condition_value ? '$ne')
                                    OR (condition_value->>'$exists' = 'true');
                    
                    IF coll_is_ne_null OR condition_value ? '$ne' THEN
                        -- Collection != null: at least one element exists
                        coll_condition := format(
                            'EXISTS (SELECT 1 FROM _values cv WHERE cv._id_object = %I._id AND cv._id_structure = %L AND cv._array_index IS NOT NULL)',
                            table_alias, coll_structure_id
                        );
                    ELSE
                        -- Collection == null: no elements
                        coll_condition := format(
                            'NOT EXISTS (SELECT 1 FROM _values cv WHERE cv._id_object = %I._id AND cv._id_structure = %L AND cv._array_index IS NOT NULL)',
                            table_alias, coll_structure_id
                        );
                    END IF;
                    
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || coll_condition;
                    ELSE
                        all_conditions := coll_condition;
                    END IF;
                    
                    CONTINUE;  -- Skip to next field, don't fall through to standard handling
                
                -- 🆕 FIX: Regular EAV fields with $ne null or $exists (NOT collections!)
                ELSIF coll_structure_id IS NOT NULL THEN
                    -- This is a regular EAV field (not a collection)
                    eav_ne_null_value := condition_value->>'$ne';
                    eav_exists_value := condition_value->>'$exists';
                    
                    -- $ne null OR $exists true = "field exists" (record exists in _values)
                    IF (condition_value ? '$ne' AND (eav_ne_null_value IS NULL OR eav_ne_null_value = 'null'))
                       OR (eav_exists_value = 'true') THEN
                        -- In EAV model: record existence = field exists and not null
                        coll_condition := format(
                            'EXISTS (SELECT 1 FROM _values ev WHERE ev._id_object = %I._id AND ev._id_structure = %L AND ev._array_index IS NULL)',
                            table_alias, coll_structure_id
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || coll_condition;
                        ELSE
                            all_conditions := coll_condition;
                        END IF;
                        CONTINUE;
                    
                    -- $exists false = "field does NOT exist" (no record in _values)
                    ELSIF eav_exists_value = 'false' THEN
                        coll_condition := format(
                            'NOT EXISTS (SELECT 1 FROM _values ev WHERE ev._id_object = %I._id AND ev._id_structure = %L AND ev._array_index IS NULL)',
                            table_alias, coll_structure_id
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || coll_condition;
                        ELSE
                            all_conditions := coll_condition;
                        END IF;
                        CONTINUE;
                    END IF;
                    -- Guard already filters: only $ne null or $exists reach here
                END IF;
                -- If structure not found, fall through to standard handling
            END;
        
        -- 🆕 Dictionary indexer: FieldName[key] (example: "PhoneBook[home]": {"$eq": "+7-999..."})
        -- Direct implementation without pvt_build_inner_condition (it uses incompatible aliases fs/fv)
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\[.+\]$' THEN
            DECLARE
                dict_field_name text;
                dict_key text;
                dict_structure_id bigint;
                dict_type_info jsonb;
                dict_db_type text;
                dict_condition text;
                dict_op_name text;
                dict_op_value text;
                dict_value text;
                dict_value_condition text;
            BEGIN
                -- Parse: "PhoneBook[home]" -> field="PhoneBook", key="home"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\[');
                dict_key := substring(condition_key from '\[(.+)\]$');
                
                -- Find structure and type_info for dictionary field
                SELECT fi.root_structure_id, fi.root_type_info
                INTO dict_structure_id, dict_type_info
                FROM pvt_find_structure_info(scheme_id, dict_field_name, NULL) AS fi
                LIMIT 1;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Get db_type from type_info for correct column selection
                dict_db_type := dict_type_info->>'db_type';
                
                -- Process condition_value - build value comparison
                IF jsonb_typeof(condition_value) = 'object' THEN
                    -- Complex condition: {"$eq": "value"}, {"$ne": "x"}, {"$contains": "substr"}, {"$in": [...]}, etc.
                    FOR dict_op_name, dict_op_value IN SELECT key, value FROM jsonb_each_text(condition_value) LOOP
                        -- Build value condition based on operator
                        IF dict_op_name = '$in' THEN
                            -- $in operator: value in list (use db_type for correct column)
                            dict_value_condition := CASE dict_db_type
                                WHEN 'Long' THEN format('dv._Long IN (SELECT (jsonb_array_elements_text(%L::jsonb))::bigint)', dict_op_value)
                                WHEN 'Numeric' THEN format('dv._Numeric IN (SELECT (jsonb_array_elements_text(%L::jsonb))::numeric)', dict_op_value)
                                WHEN 'Double' THEN format('dv._Double IN (SELECT (jsonb_array_elements_text(%L::jsonb))::double precision)', dict_op_value)
                                ELSE format('dv._String IN (SELECT jsonb_array_elements_text(%L::jsonb))', dict_op_value)
                            END;
                        ELSIF dict_op_name = '$nin' THEN
                            -- $nin operator: value not in list (use db_type for correct column)
                            dict_value_condition := CASE dict_db_type
                                WHEN 'Long' THEN format('dv._Long NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::bigint)', dict_op_value)
                                WHEN 'Numeric' THEN format('dv._Numeric NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::numeric)', dict_op_value)
                                WHEN 'Double' THEN format('dv._Double NOT IN (SELECT (jsonb_array_elements_text(%L::jsonb))::double precision)', dict_op_value)
                                ELSE format('dv._String NOT IN (SELECT jsonb_array_elements_text(%L::jsonb))', dict_op_value)
                            END;
                        ELSE
                            -- Numeric comparisons - use db_type to select correct column
                            IF dict_op_name IN ('$gt', '$gte', '$lt', '$lte') THEN
                                dict_value_condition := CASE dict_db_type
                                    WHEN 'Numeric' THEN format('dv._Numeric %s %L::numeric', 
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END, 
                                        dict_op_value)
                                    WHEN 'Double' THEN format('dv._Double %s %L::double precision',
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END,
                                        dict_op_value)
                                    ELSE format('dv._Long %s %L::bigint',
                                        CASE dict_op_name WHEN '$gt' THEN '>' WHEN '$gte' THEN '>=' WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END,
                                        dict_op_value)
                                END;
                            ELSE
                                dict_value_condition := CASE dict_op_name
                                    -- Equality/Inequality - use db_type for correct column
                                    WHEN '$eq' THEN CASE dict_db_type
                                        WHEN 'Long' THEN format('dv._Long = %L::bigint', dict_op_value)
                                        WHEN 'Numeric' THEN format('dv._Numeric = %L::numeric', dict_op_value)
                                        WHEN 'Double' THEN format('dv._Double = %L::double precision', dict_op_value)
                                        ELSE format('dv._String = %L', dict_op_value)
                                    END
                                    WHEN '$ne' THEN CASE dict_db_type
                                        WHEN 'Long' THEN format('dv._Long <> %L::bigint', dict_op_value)
                                        WHEN 'Numeric' THEN format('dv._Numeric <> %L::numeric', dict_op_value)
                                        WHEN 'Double' THEN format('dv._Double <> %L::double precision', dict_op_value)
                                        ELSE format('dv._String <> %L', dict_op_value)
                                    END
                                    -- String operations
                                    WHEN '$contains' THEN format('dv._String LIKE %L', '%' || dict_op_value || '%')
                                    WHEN '$startsWith' THEN format('dv._String LIKE %L', dict_op_value || '%')
                                    WHEN '$endsWith' THEN format('dv._String LIKE %L', '%' || dict_op_value)
                                    WHEN '$containsIgnoreCase' THEN format('dv._String ILIKE %L', '%' || dict_op_value || '%')
                                    WHEN '$startsWithIgnoreCase' THEN format('dv._String ILIKE %L', dict_op_value || '%')
                                    WHEN '$endsWithIgnoreCase' THEN format('dv._String ILIKE %L', '%' || dict_op_value)
                                    -- Regex
                                    WHEN '$regex' THEN format('dv._String ~ %L', dict_op_value)
                                    WHEN '$iregex' THEN format('dv._String ~* %L', dict_op_value)
                                    -- Explicit Double comparisons (legacy support)
                                    WHEN '$gtDouble' THEN format('dv._Double > %L', dict_op_value::double precision)
                                    WHEN '$gteDouble' THEN format('dv._Double >= %L', dict_op_value::double precision)
                                    WHEN '$ltDouble' THEN format('dv._Double < %L', dict_op_value::double precision)
                                    WHEN '$lteDouble' THEN format('dv._Double <= %L', dict_op_value::double precision)
                                    ELSE format('dv._String = %L', dict_op_value)  -- fallback to equality
                                END;
                            END IF;
                        END IF;
                        
                        dict_condition := format(
                            'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L AND %s)',
                            table_alias, dict_structure_id, dict_key, dict_value_condition
                        );
                        
                        IF all_conditions != '' THEN
                            all_conditions := all_conditions || ' AND ' || dict_condition;
                        ELSE
                            all_conditions := dict_condition;
                        END IF;
                    END LOOP;
                ELSE
                    -- Simple value: direct equality (use db_type for correct column)
                    dict_value := condition_value #>> '{}';
                    dict_condition := format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L AND %s)',
                        table_alias, dict_structure_id, dict_key,
                        CASE dict_db_type
                            WHEN 'Long' THEN format('dv._Long = %L::bigint', dict_value)
                            WHEN 'Numeric' THEN format('dv._Numeric = %L::numeric', dict_value)
                            WHEN 'Double' THEN format('dv._Double = %L::double precision', dict_value)
                            ELSE format('dv._String = %L', dict_value)
                        END
                    );
                    
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ' || dict_condition;
                    ELSE
                        all_conditions := dict_condition;
                    END IF;
                END IF;
            END;
        
        -- 🆕 Dictionary ContainsKey: FieldName.ContainsKey (example: "PhoneBook.ContainsKey": "home")
        -- ⚠️ For Dictionary<K, Class> records are stored in child structures, not in the structure itself!
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.ContainsKey$' THEN
            DECLARE
                dict_field_name text;
                dict_key text;
                dict_structure_id bigint;
                dict_condition text;
            BEGIN
                -- Parse: "PhoneBook.ContainsKey" -> field="PhoneBook"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.ContainsKey$');
                -- Get the key (handle both {"$eq": "key"} and "key")
                IF condition_value ? '$eq' THEN
                    dict_key := condition_value->>'$eq';
                ELSE
                    dict_key := condition_value #>> '{}';
                END IF;
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build EXISTS condition
                -- 🆕 FIX: Check BOTH cases:
                --   1) Dictionary<K, primitive>: records in the structure itself (_id_structure = dict_id)
                --   2) Dictionary<K, Class>: records in child structures (_id_parent = dict_id)
                dict_condition := format(
                    'EXISTS (SELECT 1 FROM _values dv JOIN _structures s ON dv._id_structure = s._id WHERE dv._id_object = %I._id AND (s._id = %L OR s._id_parent = %L) AND dv._array_index = %L)',
                    table_alias, dict_structure_id, dict_structure_id, dict_key
                );
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- 🆕 Dictionary aggregation operators: FieldName.$dictCount, FieldName.$dictSum, etc.
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.\$dict(Count|Sum|Avg|Min|Max|Keys|Values|HasValue)$' THEN
            DECLARE
                dict_field_name text;
                dict_op text;
                dict_structure_id bigint;
                dict_condition text;
                dict_value text;
            BEGIN
                -- Parse: "PhoneBook.$dictCount" -> field="PhoneBook", op="Count"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.\$dict');
                dict_op := substring(condition_key from '\$dict([A-Za-z]+)$');
                dict_value := condition_value #>> '{}';
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build aggregation condition
                dict_condition := CASE dict_op
                    WHEN 'Count' THEN format(
                        '(SELECT COUNT(*) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::int',
                        table_alias, dict_structure_id, dict_value::int)
                    WHEN 'Sum' THEN format(
                        '(SELECT COALESCE(SUM(dv._Long), 0) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Avg' THEN format(
                        '(SELECT AVG(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::numeric',
                        table_alias, dict_structure_id, dict_value::numeric)
                    WHEN 'Min' THEN format(
                        '(SELECT MIN(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Max' THEN format(
                        '(SELECT MAX(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) = %L::bigint',
                        table_alias, dict_structure_id, dict_value::bigint)
                    WHEN 'Keys' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index = %L)',
                        table_alias, dict_structure_id, dict_value)
                    WHEN 'Values' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._String = %L)',
                        table_alias, dict_structure_id, dict_value)
                    WHEN 'HasValue' THEN format(
                        'EXISTS (SELECT 1 FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._String = %L)',
                        table_alias, dict_structure_id, dict_value)
                    ELSE 'TRUE'
                END;
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- 🆕 Dictionary comparison operators: FieldName.$dictCountGt, FieldName.$dictCountLt, etc.
        ELSIF condition_key ~ '^[A-Za-z_][A-Za-z0-9_]*\.\$dict(Count|Sum|Avg|Min|Max)(Gt|Gte|Lt|Lte)$' THEN
            DECLARE
                dict_field_name text;
                dict_agg text;
                dict_cmp text;
                dict_structure_id bigint;
                dict_condition text;
                dict_value text;
                dict_cmp_op text;
            BEGIN
                -- Parse: "PhoneBook.$dictCountGt" -> field="PhoneBook", agg="Count", cmp="Gt"
                dict_field_name := substring(condition_key from '^([A-Za-z_][A-Za-z0-9_]*)\.\$dict');
                dict_agg := substring(condition_key from '\$dict(Count|Sum|Avg|Min|Max)');
                dict_cmp := substring(condition_key from '(Gt|Gte|Lt|Lte)$');
                dict_value := condition_value #>> '{}';
                
                dict_cmp_op := CASE dict_cmp
                    WHEN 'Gt' THEN '>'
                    WHEN 'Gte' THEN '>='
                    WHEN 'Lt' THEN '<'
                    WHEN 'Lte' THEN '<='
                END;
                
                -- Find structure for dictionary field
                SELECT _id INTO dict_structure_id
                FROM _structures 
                WHERE _id_scheme = scheme_id 
                  AND _name = dict_field_name
                  AND _id_parent IS NULL;
                
                IF dict_structure_id IS NULL THEN
                    RAISE EXCEPTION 'Dictionary field "%" not found in scheme %', dict_field_name, scheme_id;
                END IF;
                
                -- Build aggregation condition with comparison
                dict_condition := CASE dict_agg
                    WHEN 'Count' THEN format(
                        '(SELECT COUNT(*) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::int',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::int)
                    WHEN 'Sum' THEN format(
                        '(SELECT COALESCE(SUM(dv._Long), 0) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    WHEN 'Avg' THEN format(
                        '(SELECT AVG(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::numeric',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::numeric)
                    WHEN 'Min' THEN format(
                        '(SELECT MIN(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    WHEN 'Max' THEN format(
                        '(SELECT MAX(dv._Long) FROM _values dv WHERE dv._id_object = %I._id AND dv._id_structure = %L AND dv._array_index IS NOT NULL) %s %L::bigint',
                        table_alias, dict_structure_id, dict_cmp_op, dict_value::bigint)
                    ELSE 'TRUE'
                END;
                
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ' || dict_condition;
                ELSE
                    all_conditions := dict_condition;
                END IF;
            END;
        
        -- Field operators
        ELSE
            -- Parse field path
            field_path := condition_key;
            SELECT * INTO parsed_path FROM pvt_parse_field_path(field_path);
            
            -- Get structure information for all fields
            -- 🎯 FIX: Explicit assignment of fields from TABLE-returning function to RECORD
            SELECT 
                fi.root_structure_id,
                fi.nested_structure_id,
                fi.root_type_info,
                fi.nested_type_info
            INTO 
                structure_info
            FROM pvt_find_structure_info(scheme_id, parsed_path.root_field, parsed_path.nested_field) AS fi
            LIMIT 1;
            
            -- Process field value
            IF jsonb_typeof(condition_value) = 'object' THEN
                -- Complex condition with operators like {"$gt": 100, "$lt": 200}
                FOR operator_name, operator_value IN SELECT key, value FROM jsonb_each_text(condition_value) LOOP
                    inner_condition_sql := pvt_build_inner_condition(
                        operator_name, 
                        operator_value, 
                        CASE 
                            WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                            ELSE structure_info.root_type_info
                        END
                    );
                    
                    -- $ne for specific value (not null) requires NOT EXISTS
                    single_condition := _build_exists_condition(
                        field_path, 
                        inner_condition_sql, 
                        operator_name = '$ne' AND operator_value IS NOT NULL AND operator_value != 'null' AND operator_value != '',
                        scheme_id, 
                        table_alias
                    );
                    
                    -- Accumulate conditions via AND
                    IF all_conditions != '' THEN
                        all_conditions := all_conditions || ' AND ';
                    END IF;
                    all_conditions := all_conditions || ltrim(single_condition, ' AND ');
                END LOOP;
            
            ELSIF jsonb_typeof(condition_value) = 'array' THEN
                -- Array of values - process as $in
                inner_condition_sql := pvt_build_inner_condition(
                    '$in', 
                    condition_value::text,
                    CASE 
                        WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                        ELSE structure_info.root_type_info
                    END
                );
                
                single_condition := _build_exists_condition(field_path, inner_condition_sql, false, scheme_id, table_alias);
                
                -- Accumulate conditions via AND
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ';
                END IF;
                all_conditions := all_conditions || ltrim(single_condition, ' AND ');
            
            ELSE
                -- Simple value - equality
                inner_condition_sql := pvt_build_inner_condition(
                    '=', 
                    -- Remove extra quotes from string values
                    CASE 
                        WHEN jsonb_typeof(condition_value) = 'string' THEN condition_value #>> '{}'
                        ELSE condition_value::text 
                    END,
                    CASE 
                        WHEN parsed_path.is_nested THEN structure_info.nested_type_info
                        ELSE structure_info.root_type_info
                    END
                );
                
                single_condition := _build_exists_condition(field_path, inner_condition_sql, false, scheme_id, table_alias);
                
                -- Accumulate conditions via AND
                IF all_conditions != '' THEN
                    all_conditions := all_conditions || ' AND ';
                END IF;
                all_conditions := all_conditions || ltrim(single_condition, ' AND ');
            END IF;
        END IF;
    END LOOP;
    
    -- Return all accumulated conditions
    IF all_conditions != '' THEN
        RETURN ' AND (' || all_conditions || ')';
    END IF;
    RETURN '';
END;
$BODY$;



-- ===== 06a_pvt_legacy_helpers.sql =====
-- =====================================================================
-- pvt_build_level_condition / pvt_build_level_condition_with_operators
-- ---------------------------------------------------------------------
-- Forked from redb_init.sql (deprecated) on 2026-05-23.
-- Required by pvt_build_hierarchical_conditions (06_pvt_hierarchical.sql).
-- The legacy bundle that originally defined `build_level_condition`/
-- `build_level_condition_with_operators` was removed in PG free
-- (v2-pvt is now the only engine), so we re-host these helpers under
-- the pvt_* namespace.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_build_level_condition(
    target_level integer,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
BEGIN
    -- Calculate level going UP from object to root.
    -- Level 0 = root (where _id_parent IS NULL).
    -- Level 1 = direct child of root, etc.
    RETURN format(
        ' AND (
            SELECT COUNT(*)::integer FROM (
                WITH RECURSIVE ancestors AS (
                    SELECT %s._id_parent as parent_id
                    UNION ALL
                    SELECT o._id_parent
                    FROM _objects o
                    JOIN ancestors ON o._id = ancestors.parent_id
                    WHERE o._id_parent IS NOT NULL
                )
                SELECT parent_id FROM ancestors WHERE parent_id IS NOT NULL
            ) AS a
        ) = %s',
        table_alias, target_level
    );
END;
$BODY$;

CREATE OR REPLACE FUNCTION pvt_build_level_condition_with_operators(
    level_operators jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
IMMUTABLE
AS $BODY$
DECLARE
    operator_name text;
    operator_value text;
    level_condition text := '';
    op_symbol text;
BEGIN
    FOR operator_name, operator_value IN
        SELECT key, value FROM jsonb_each_text(level_operators)
    LOOP
        CASE operator_name
            WHEN '$gt' THEN op_symbol := '>';
            WHEN '$gte' THEN op_symbol := '>=';
            WHEN '$lt' THEN op_symbol := '<';
            WHEN '$lte' THEN op_symbol := '<=';
            WHEN '$eq' THEN op_symbol := '=';
            WHEN '$ne' THEN op_symbol := '!=';
            ELSE
                CONTINUE;
        END CASE;

        IF level_condition != '' THEN
            level_condition := level_condition || ' AND ';
        END IF;

        level_condition := level_condition || format(
            '(
                SELECT COUNT(*)::integer FROM (
                    WITH RECURSIVE ancestors AS (
                        SELECT %s._id_parent as parent_id
                        UNION ALL
                        SELECT o._id_parent
                        FROM _objects o
                        JOIN ancestors ON o._id = ancestors.parent_id
                        WHERE o._id_parent IS NOT NULL
                    )
                    SELECT parent_id FROM ancestors WHERE parent_id IS NOT NULL
                ) AS a
            ) %s %s',
            table_alias, op_symbol, operator_value
        );
    END LOOP;

    IF level_condition != '' THEN
        RETURN ' AND (' || level_condition || ')';
    END IF;

    RETURN '';
END;
$BODY$;


-- ===== 06_pvt_hierarchical.sql =====
-- =====================================================================
-- pvt_build_hierarchical_conditions: build tree predicates ($hasAncestor, $hasDescendant, $level, ...)
-- ---------------------------------------------------------------------
-- Forked from redb_facets_search.sql L2818 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_build_hierarchical_conditions(
    facet_filters jsonb,
    table_alias text DEFAULT 'o'
) RETURNS text
LANGUAGE 'plpgsql'
COST 50
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    where_conditions text := '';
    ancestor_id bigint;
    descendant_id bigint;
    target_level integer;
BEGIN
    IF facet_filters IS NOT NULL AND jsonb_typeof(facet_filters) = 'object' THEN
        -- $hasAncestor: Polymorphic ancestor search with condition, scheme_id and max_depth
        IF facet_filters ? '$hasAncestor' THEN
            where_conditions := where_conditions || build_has_ancestor_condition(facet_filters->'$hasAncestor', table_alias);
        END IF;
        
        -- $hasDescendant: Polymorphic descendant search with condition, scheme_id and max_depth
        IF facet_filters ? '$hasDescendant' THEN
            where_conditions := where_conditions || build_has_descendant_condition(facet_filters->'$hasDescendant', table_alias);
        END IF;
        
        -- $level: Support for comparison operators {"$gt": 2}, {"$eq": 3} etc.
        IF facet_filters ? '$level' THEN
            -- ✅ FIX: Processing JSON operators for $level
            IF jsonb_typeof(facet_filters->'$level') = 'object' THEN
                -- Complex condition with operators like {"$gt": 2}, {"$lt": 5}
                where_conditions := where_conditions || pvt_build_level_condition_with_operators(facet_filters->'$level', table_alias);
            ELSE
                -- Simple value - exact equality
                target_level := (facet_filters->>'$level')::integer;
                where_conditions := where_conditions || pvt_build_level_condition(target_level, table_alias);
            END IF;
        END IF;
        
        -- $isRoot
        IF facet_filters ? '$isRoot' AND (facet_filters->>'$isRoot')::boolean THEN
            where_conditions := where_conditions || format(' AND %s._id_parent IS NULL', table_alias);
        END IF;
        
        -- $isLeaf  
        IF facet_filters ? '$isLeaf' AND (facet_filters->>'$isLeaf')::boolean THEN
            where_conditions := where_conditions || format(
                ' AND NOT EXISTS (SELECT 1 FROM _objects child WHERE child._id_parent = %s._id)', 
                table_alias
            );
        END IF;
        
        -- $childrenOf - direct children of specified parent
        IF facet_filters ? '$childrenOf' THEN
            where_conditions := where_conditions || format(
                ' AND %s._id_parent = %s', 
                table_alias,
                (facet_filters->>'$childrenOf')::bigint
            );
        END IF;
    END IF;
    
    RETURN where_conditions;
END;
$BODY$;



-- ===== 07_pvt_base_fields.sql =====
-- =====================================================================
-- pvt_get_object_base_fields: return JSONB with all base fields of a single object (no Props)
-- ---------------------------------------------------------------------
-- Forked from redb_lazy_loading_search.sql L17 on 2026-05-18.
-- Legacy version stays untouched. Mirror legacy bug fixes here.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_get_object_base_fields(object_id bigint)
RETURNS jsonb
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT jsonb_build_object(
        'id', o._id,
        'name', o._name,
        'scheme_id', o._id_scheme,
        'parent_id', o._id_parent,
        'owner_id', o._id_owner,
        'who_change_id', o._id_who_change,
        'date_create', o._date_create,
        'date_modify', o._date_modify,
        'date_begin', o._date_begin,
        'date_complete', o._date_complete,
        'key', o._key,
        'value_long', o._value_long,
        'value_string', o._value_string,
        'value_guid', o._value_guid,
        'note', o._note,
        'value_bool', o._value_bool,
        'value_double', o._value_double,
        'value_numeric', o._value_numeric,
        'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes,
        'hash', o._hash  -- CRITICAL for cache!
    )
    FROM _objects o
    WHERE o._id = object_id;
$$;

COMMENT ON FUNCTION pvt_get_object_base_fields(bigint) IS 
'Returns base object fields WITHOUT Props for lazy loading.
Includes hash for cache validation. 10-50x faster than get_object_json().
ATTENTION: Function kept for compatibility and direct use.
In aggregate queries (search_*_base), direct JOIN is used instead of function call for optimization.';



-- ===== 10_pvt_field_collection.sql =====
-- =====================================================================
-- 10_pvt_field_collection.sql
-- ---------------------------------------------------------------------
-- Field-name collection from filter/order JSON and metadata resolution
-- via _scheme_metadata_cache. Produces the `fields` jsonb consumed by
-- pvt_build_column_expr / pvt_build_cte_sql / pvt_build_*_condition.
--
-- Functions:
--   pvt_extract_field_names(p_filter jsonb) RETURNS SETOF text
--   pvt_collect_fields(p_scheme_id, p_filter, p_order, p_include_all) RETURNS jsonb
--   pvt_has_null_check(p_filter jsonb) RETURNS boolean
--
-- The result of pvt_collect_fields is a jsonb object:
--   {
--     "<field_name>": {
--       "semantic": "base"|"scalar"|"listitem"|"collection"|"nested",
--       "column":   "_id_parent"           -- only when semantic = "base"
--       "sid":      42,                    -- _structure_id
--       "db_type":  "Long",                -- db_type from cache
--       "type_semantic": "TimeSpan"|null,  -- type_semantic from cache
--       "collection_type": null|<int>,
--       "list_id":  null|<int>,
--       "parent_sid": null|<int>
--     },
--     ...
--   }
-- =====================================================================

-- ---------- pvt_extract_field_names ------------------------------------
-- Walk a filter JSON recursively and return every field-name reference
-- (a key whose name does NOT start with '$'). Logical operators ($and,
-- $or, $not, ...) are recursed into but never emitted themselves.
CREATE OR REPLACE FUNCTION pvt_extract_field_names(p_filter jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text;
    v jsonb;
    elem jsonb;
BEGIN
    IF p_filter IS NULL OR jsonb_typeof(p_filter) <> 'object' THEN
        RETURN;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF left(k, 1) = '$' THEN
            -- $case has shape: [ {when:<bool>, then:<expr>}, ..., {else:<expr>}? ].
            -- Iterate VALUES of each branch object, not its keys, so that
            -- "when" / "then" / "else" never get harvested as field paths.
            IF lower(k) = '$case' AND jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF jsonb_typeof(elem) = 'object' THEN
                        IF elem ? 'when' THEN
                            RETURN QUERY SELECT pvt_extract_field_names(elem->'when');
                        END IF;
                        IF elem ? 'then' THEN
                            RETURN QUERY SELECT pvt_extract_field_names(elem->'then');
                        END IF;
                        IF elem ? 'else' THEN
                            RETURN QUERY SELECT pvt_extract_field_names(elem->'else');
                        END IF;
                    END IF;
                END LOOP;
                CONTINUE;
            END IF;
            -- $fts object form: { "query": <expr>, "fields": [<expr>...], "language": "..." }.
            -- Only `query` and `fields` carry field references; `language` is a literal.
            IF lower(k) = '$fts' AND jsonb_typeof(v) = 'object' THEN
                IF v ? 'query'  THEN RETURN QUERY SELECT pvt_extract_field_names(v->'query'); END IF;
                IF v ? 'fields' AND jsonb_typeof(v->'fields') = 'array' THEN
                    FOR elem IN SELECT value FROM jsonb_array_elements(v->'fields') LOOP
                        RETURN QUERY SELECT pvt_extract_field_names(elem);
                    END LOOP;
                END IF;
                CONTINUE;
            END IF;
            -- $cast(["<sql-type>", expr]) -- type literal must not be walked.
            IF lower(k) = '$cast' AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 2 THEN
                RETURN QUERY SELECT pvt_extract_field_names(v->1);
                CONTINUE;
            END IF;
            -- $dateAdd/$dateSub/$dateDiff/$dateTrunc: first element is a unit string literal.
            IF lower(k) IN ('$dateadd','$datesub','$datediff','$datetrunc')
               AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 1 THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) WITH ORDINALITY AS t(value, ord)
                              WHERE ord >= 2 LOOP
                    RETURN QUERY SELECT pvt_extract_field_names(elem);
                END LOOP;
                CONTINUE;
            END IF;
            -- Logical operator: recurse into children.
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    RETURN QUERY SELECT pvt_extract_field_names(elem);
                END LOOP;
            ELSIF jsonb_typeof(v) = 'object' THEN
                RETURN QUERY SELECT pvt_extract_field_names(v);
            END IF;
        ELSE
            -- Field reference.
            RETURN NEXT k;
        END IF;
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_extract_field_names(jsonb) IS
    'Recursively extracts every field-name reference from a filter JSON. Logical operators ($and/$or/$not/...) are descended into but never returned.';


-- ---------- pvt_extract_field_pairs -----------------------------------
-- Walk a filter JSON recursively and emit (path, op_value) pairs.
-- `op_value` is the underlying string operand when path ends with
-- `.ContainsKey` (Pro-style dictionary normalization); NULL otherwise.
CREATE OR REPLACE FUNCTION pvt_extract_field_pairs(p_filter jsonb)
RETURNS TABLE (path text, op_value text)
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text;
    v jsonb;
    elem jsonb;
BEGIN
    IF p_filter IS NULL OR jsonb_typeof(p_filter) <> 'object' THEN
        RETURN;
    END IF;

    -- Expression node shortcuts: only `$field` yields a path; `$const`
    -- carries a literal and contributes no field references.
    IF p_filter ? '$field' THEN
        RETURN QUERY SELECT (p_filter->>'$field')::text, NULL::text;
        RETURN;
    END IF;
    IF p_filter ? '$const' THEN
        RETURN;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF left(k, 1) = '$' THEN
            -- $case has shape: [ {when:<bool>, then:<expr>}, ..., {else:<expr>}? ].
            -- Descend into branch values only, never treat "when"/"then"/"else" as paths.
            IF lower(k) = '$case' AND jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF jsonb_typeof(elem) = 'object' THEN
                        IF elem ? 'when' THEN
                            RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem->'when') pp;
                        END IF;
                        IF elem ? 'then' THEN
                            RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem->'then') pp;
                        END IF;
                        IF elem ? 'else' THEN
                            RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem->'else') pp;
                        END IF;
                    END IF;
                END LOOP;
                CONTINUE;
            END IF;
            -- $fts object form: descend only into `query` and `fields` array.
            IF lower(k) = '$fts' AND jsonb_typeof(v) = 'object' THEN
                IF v ? 'query'  THEN
                    RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(v->'query') pp;
                END IF;
                IF v ? 'fields' AND jsonb_typeof(v->'fields') = 'array' THEN
                    FOR elem IN SELECT value FROM jsonb_array_elements(v->'fields') LOOP
                        RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem) pp;
                    END LOOP;
                END IF;
                CONTINUE;
            END IF;
            -- $cast(["<sql-type>", expr]): skip the type literal.
            IF lower(k) = '$cast' AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 2 THEN
                RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(v->1) pp;
                CONTINUE;
            END IF;
            -- $dateAdd/$dateSub/$dateDiff/$dateTrunc: skip the unit literal.
            IF lower(k) IN ('$dateadd','$datesub','$datediff','$datetrunc')
               AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 1 THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) WITH ORDINALITY AS t(value, ord)
                              WHERE ord >= 2 LOOP
                    RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem) pp;
                END LOOP;
                CONTINUE;
            END IF;
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(elem) pp;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'object' THEN
                RETURN QUERY SELECT pp.path, pp.op_value FROM pvt_extract_field_pairs(v) pp;
            END IF;
        ELSE
            IF right(k, length('.ContainsKey')) = '.ContainsKey' THEN
                RETURN QUERY SELECT k, pvt_peek_contains_key_value(v);
            ELSE
                RETURN QUERY SELECT k, NULL::text;
            END IF;
        END IF;
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_extract_field_pairs(jsonb) IS
    'Recursive walker: yields (path, op_value) pairs. op_value carries the operand text for `*.ContainsKey` predicates so the caller can normalize them to `<base>[<key>]`.';


-- ---------- pvt_collect_fields ----------------------------------------
-- Resolve metadata for every field mentioned in filter/order. When
-- p_include_all is true, ALL scheme fields are collected (used by the
-- full-result entry point that may need to project arbitrary fields).
CREATE OR REPLACE FUNCTION pvt_collect_fields(
    p_scheme_id   bigint,
    p_filter      jsonb,
    p_order       jsonb,
    p_include_all boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_pair       RECORD;
    v_normalized text;
    v_meta       jsonb;
    v_order_path text;
    v_all_name   text;
    v_result     jsonb := '{}'::jsonb;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_collect_fields: p_scheme_id is required';
    END IF;

    -- ---------- p_include_all: project every top-level field ----------
    IF p_include_all THEN
        FOR v_all_name IN
            SELECT DISTINCT _name
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id
               AND _parent_structure_id IS NULL
        LOOP
            v_meta := pvt_resolve_field_path(p_scheme_id, v_all_name);
            v_result := v_result || jsonb_build_object(v_all_name, v_meta);
        END LOOP;
        RETURN v_result;
    END IF;

    -- ---------- Collect from filter (ContainsKey-aware) ---------------
    FOR v_pair IN
        SELECT DISTINCT path, op_value
          FROM pvt_extract_field_pairs(p_filter)
         WHERE path IS NOT NULL
    LOOP
        -- Detect .$length / .$count suffix: targets count of the base
        -- array. We register BOTH the base field (so the CTE pivots it)
        -- AND a modifier entry that the condition builder rewrites to
        -- COALESCE(array_length(<base>, 1), 0).
        DECLARE
            v_mod_kind text := NULL;
            v_base     text := NULL;
            v_base_meta jsonb;
        BEGIN
            IF right(v_pair.path, length('.$length')) = '.$length' THEN
                v_mod_kind := 'length';
                v_base := left(v_pair.path, length(v_pair.path) - length('.$length'));
            ELSIF right(v_pair.path, length('.$count')) = '.$count' THEN
                v_mod_kind := 'count';
                v_base := left(v_pair.path, length(v_pair.path) - length('.$count'));
            END IF;

            IF v_mod_kind IS NOT NULL THEN
                IF NOT (v_result ? v_base) THEN
                    v_base_meta := pvt_resolve_field_path(p_scheme_id, v_base);
                    v_result := v_result || jsonb_build_object(v_base, v_base_meta);
                ELSE
                    v_base_meta := v_result -> v_base;
                END IF;
                IF NOT (v_result ? v_pair.path) THEN
                    v_result := v_result || jsonb_build_object(
                        v_pair.path,
                        v_base_meta
                            || jsonb_build_object(
                                'length_modifier', true,
                                'modifier_kind', v_mod_kind,
                                'base_name', v_base));
                END IF;
                CONTINUE;
            END IF;
        END;

        v_normalized := pvt_normalize_field_name(v_pair.path, v_pair.op_value);
        IF v_result ? v_normalized THEN
            CONTINUE;
        END IF;
        v_meta := pvt_resolve_field_path(p_scheme_id, v_normalized);
        IF v_normalized <> v_pair.path THEN
            -- Tag ContainsKey rewrites: WHERE builder emits IS NOT NULL.
            v_meta := v_meta || jsonb_build_object('was_contains_key', true);
        END IF;
        v_result := v_result || jsonb_build_object(v_normalized, v_meta);
    END LOOP;

    -- ---------- Collect from order paths -----------------------------
    -- Each order entry may carry either:
    --   * plain path: { "field": "X" } or legacy { "field_path": "X" }
    --   * expression node: { "$expr": <scalar-expr-node>, "dir": "..." }
    -- For expressions we walk every `$field` inside via
    -- pvt_expr_field_names (defined in 17_pvt_expr.sql) so its metadata
    -- gets resolved and the field is pivoted into the CTE just like
    -- WHERE-referenced fields.
    IF p_order IS NOT NULL THEN
        FOR v_order_path IN
            SELECT path FROM (
                SELECT COALESCE(e->>'field', e->>'field_path') AS path
                  FROM jsonb_array_elements(p_order) AS e
                 WHERE e ? 'field' OR e ? 'field_path'
                UNION ALL
                SELECT pvt_expr_field_names(e->'$expr') AS path
                  FROM jsonb_array_elements(p_order) AS e
                 WHERE e ? '$expr'
            ) s
            WHERE s.path IS NOT NULL
        LOOP
            IF v_order_path IS NULL OR v_result ? v_order_path THEN
                CONTINUE;
            END IF;
            v_meta := pvt_resolve_field_path(p_scheme_id, v_order_path);
            v_result := v_result || jsonb_build_object(v_order_path, v_meta);
        END LOOP;
    END IF;

    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION pvt_collect_fields(bigint, jsonb, jsonb, boolean) IS
    'Walks filter/order JSON, normalizes Dict.ContainsKey paths, resolves each via pvt_resolve_field_path, and returns a jsonb keyed by the (possibly rewritten) field name. ContainsKey-rewritten entries carry was_contains_key=true so the WHERE builder collapses the predicate to IS NOT NULL.';


-- ---------- pvt_has_null_check ----------------------------------------
-- True iff the filter contains an explicit null-aware predicate. Used
-- by the CTE builder to decide LEFT JOIN (need NULL surface) vs INNER
-- JOIN (faster, suffices when all predicates require a present value).
CREATE OR REPLACE FUNCTION pvt_has_null_check(p_filter jsonb)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text; v jsonb; sk text; sv jsonb; elem jsonb;
BEGIN
    IF p_filter IS NULL OR jsonb_typeof(p_filter) <> 'object' THEN
        RETURN false;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF left(k, 1) = '$' THEN
            IF k IN ('$null', '$exists', '$isNull', '$notNull') THEN
                RETURN true;
            END IF;
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF pvt_has_null_check(elem) THEN RETURN true; END IF;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'object' THEN
                IF pvt_has_null_check(v) THEN RETURN true; END IF;
            END IF;
        ELSE
            -- Field leaf: inspect its operator object.
            IF jsonb_typeof(v) = 'object' THEN
                FOR sk, sv IN SELECT key, value FROM jsonb_each(v) LOOP
                    IF sk IN ('$null', '$exists', '$isNull', '$notNull') THEN
                        RETURN true;
                    END IF;
                    IF sk IN ('$eq', '$ne') AND jsonb_typeof(sv) = 'null' THEN
                        RETURN true;
                    END IF;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'null' THEN
                RETURN true;
            END IF;
        END IF;
    END LOOP;

    RETURN false;
END;
$BODY$;

COMMENT ON FUNCTION pvt_has_null_check(jsonb) IS
    'Returns true if the filter contains any explicit null-aware predicate ($null/$exists/$isNull/$notNull or $eq/$ne with null). The CTE builder uses this to pick LEFT vs INNER JOIN on _values.';


-- ---------- pvt_has_absence_check -------------------------------------
-- Strict subset of pvt_has_null_check: returns true only for predicates
-- that REQUIRE detecting absent _values rows (i.e. an object without
-- any value for the field must still appear in the outer result set).
-- These predicates demand the legacy wide+LEFT JOIN shape:
--
--   * `$null` / `$isNull`                  : field must be NULL.
--   * `{$eq: null}`                        : same as $null.
--   * `$exists` (any value, incl. false)   : tests presence; safest
--                                            kept in the "absence"
--                                            bucket.
--
-- Predicates that can be satisfied by an INNER JOIN over _values are
-- intentionally NOT flagged here:
--
--   * `$notNull`         : "must be present" -- INNER drops absent rows
--                          automatically (the desired outcome).
--   * `{$ne: null}`      : equivalent to "must be present".
--
-- The narrow Pro-shape pivot CTE inherently CANNOT represent absent
-- objects (its `FROM _values v` GROUP BY v._id_object only yields
-- object ids that have at least one matching row); hence the narrow
-- branch is gated on `NOT pvt_has_absence_check(...)` rather than the
-- broader pvt_has_null_check.
CREATE OR REPLACE FUNCTION pvt_has_absence_check(p_filter jsonb)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text; v jsonb; sk text; sv jsonb; elem jsonb;
BEGIN
    IF p_filter IS NULL OR jsonb_typeof(p_filter) <> 'object' THEN
        RETURN false;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF left(k, 1) = '$' THEN
            IF k IN ('$null', '$isNull', '$exists') THEN
                RETURN true;
            END IF;
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF pvt_has_absence_check(elem) THEN RETURN true; END IF;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'object' THEN
                IF pvt_has_absence_check(v) THEN RETURN true; END IF;
            END IF;
        ELSE
            IF jsonb_typeof(v) = 'object' THEN
                FOR sk, sv IN SELECT key, value FROM jsonb_each(v) LOOP
                    IF sk IN ('$null', '$isNull', '$exists') THEN
                        RETURN true;
                    END IF;
                    IF sk = '$eq' AND jsonb_typeof(sv) = 'null' THEN
                        RETURN true;
                    END IF;
                END LOOP;
            ELSIF jsonb_typeof(v) = 'null' THEN
                RETURN true;
            END IF;
        END IF;
    END LOOP;

    RETURN false;
END;
$BODY$;

COMMENT ON FUNCTION pvt_has_absence_check(jsonb) IS
    'Strict subset of pvt_has_null_check: true only for predicates that require detecting absent _values rows ($null/$isNull/$exists/{$eq:null}). $notNull and {$ne:null} are NOT flagged because INNER JOIN already drops absent rows. Used to gate the narrow Pro-shape pivot, which cannot represent missing objects.';


-- =====================================================================
-- pvt_resolve_field_path: port of redb.Core.Pro.Schema.SchemeFieldResolver
-- ---------------------------------------------------------------------
-- Resolves a logical field path (e.g. "Name", "Dict[key]", "Status.Value",
-- "Auction.Costs", "Roles[].Value") to a rich FieldInfo jsonb consumed
-- by pvt_build_column_expr / pvt_build_cte_sql / pvt_build_field_condition.
--
-- Returned shape:
--   { "kind":"base", "column":"_id_parent", "name":"<raw>",
--     "is_array":false, "list_item_prop":null, "dict_key":null,
--     "parent_sid":null, "sid":null, "db_type":null, "db_column":null }
-- OR
--   { "kind":"field", "sid":<bigint>, "db_type":"String|...",
--     "db_column":"_String|_Long|...", "name":"<raw>",
--     "is_array":bool, "list_item_prop":null|"Id"|"Value"|"Alias",
--     "dict_key":null|"<key>", "parent_sid":null|<bigint> }
-- =====================================================================
CREATE OR REPLACE FUNCTION pvt_resolve_field_path(
    p_scheme_id bigint,
    p_path      text
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_dict_match     text[];
    v_root_name      text;
    v_dict_key       text;
    v_rest           text;
    v_child_name     text;
    v_parts          text[];
    v_last           text;
    v_root_li        text;
    v_base_col       text;
    v_db_col         text;
    v_sid            bigint;
    v_db_type        text;
    v_is_array       boolean;
    v_parent_sid     bigint := NULL;
    v_list_item_prop text := NULL;
    v_cur_sid        bigint;
    v_base_name      text;
    v_force_array    boolean := false;
    i                int;
BEGIN
    IF p_scheme_id IS NULL OR p_path IS NULL OR p_path = '' THEN
        RAISE EXCEPTION 'pvt_resolve_field_path: scheme_id/path are required (got %, %)', p_scheme_id, p_path;
    END IF;

    -- 0. Base field?
    v_base_col := pvt_normalize_base_field_name(p_path);
    IF v_base_col IS NOT NULL THEN
        RETURN jsonb_build_object(
            'kind','base','column',v_base_col,'name',p_path,
            'is_array',false,'list_item_prop',NULL,'dict_key',NULL,
            'parent_sid',NULL,'sid',NULL,'db_type',NULL,'db_column',NULL);
    END IF;

    -- 1. Dictionary path: Name[key] or Name[key].Child
    v_dict_match := regexp_match(p_path, '^([A-Za-z_][A-Za-z0-9_]*)\[([^\]]+)\](.*)$');
    IF v_dict_match IS NOT NULL THEN
        v_root_name := v_dict_match[1];
        v_dict_key  := v_dict_match[2];
        v_rest      := v_dict_match[3];

        SELECT _structure_id, db_type
          INTO v_sid, v_db_type
          FROM _scheme_metadata_cache
         WHERE _scheme_id = p_scheme_id AND _name = v_root_name AND _parent_structure_id IS NULL
         LIMIT 1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'pvt_resolve_field_path: dict root field "%" not found in scheme %', v_root_name, p_scheme_id;
        END IF;

        IF v_rest IS NOT NULL AND v_rest LIKE '.%' THEN
            v_parent_sid := v_sid;
            v_child_name := substring(v_rest from 2);
            SELECT _structure_id, db_type, (_collection_type IS NOT NULL)
              INTO v_sid, v_db_type, v_is_array
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id AND _name = v_child_name AND _parent_structure_id = v_parent_sid
             LIMIT 1;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'pvt_resolve_field_path: nested-dict child "%" not found under "%"', v_child_name, v_root_name;
            END IF;
        ELSE
            v_is_array := false;
        END IF;

        v_db_col := pvt_db_type_to_value_column(v_db_type);
        RETURN jsonb_build_object(
            'kind','field','sid',v_sid,'db_type',v_db_type,'db_column',v_db_col,
            'name',p_path,'is_array',v_is_array,
            'list_item_prop',NULL,'dict_key',v_dict_key,'parent_sid',v_parent_sid);
    END IF;

    -- 2. Dotted paths.
    IF position('.' in p_path) > 0 THEN
        v_parts := string_to_array(p_path, '.');
        v_last  := v_parts[array_length(v_parts,1)];

        -- 2a. Roles[].Value / .Alias / .Id  (ListItem array accessors)
        IF array_length(v_parts,1) = 2
           AND v_last IN ('Id','Value','Alias')
           AND v_parts[1] LIKE '%[]' THEN
            v_root_li := left(v_parts[1], length(v_parts[1]) - 2);
            SELECT _structure_id, db_type, (_collection_type IS NOT NULL)
              INTO v_sid, v_db_type, v_is_array
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id AND _name = v_root_li AND _parent_structure_id IS NULL
             LIMIT 1;
            IF FOUND AND v_db_type = 'ListItem' AND v_is_array THEN
                RETURN jsonb_build_object(
                    'kind','field','sid',v_sid,'db_type','ListItem','db_column','_ListItem',
                    'name',p_path,'is_array',true,
                    'list_item_prop',v_last,'dict_key',NULL,'parent_sid',NULL);
            END IF;
        END IF;

        -- 2b. Status.Value / .Alias / .Id  (ListItem scalar accessors)
        IF array_length(v_parts,1) = 2 AND v_last IN ('Id','Value','Alias') THEN
            SELECT _structure_id, db_type, (_collection_type IS NOT NULL)
              INTO v_sid, v_db_type, v_is_array
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id AND _name = v_parts[1] AND _parent_structure_id IS NULL
             LIMIT 1;
            IF FOUND AND v_db_type = 'ListItem' THEN
                RETURN jsonb_build_object(
                    'kind','field','sid',v_sid,'db_type','ListItem','db_column','_ListItem',
                    'name',p_path,'is_array',v_is_array,
                    'list_item_prop',v_last,'dict_key',NULL,'parent_sid',NULL);
            END IF;
        END IF;

        -- 2c. Generic nested: walk parent chain.
        SELECT _structure_id INTO v_cur_sid
          FROM _scheme_metadata_cache
         WHERE _scheme_id = p_scheme_id AND _name = v_parts[1] AND _parent_structure_id IS NULL
         LIMIT 1;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'pvt_resolve_field_path: root "%" of nested path "%" not found in scheme %', v_parts[1], p_path, p_scheme_id;
        END IF;

        FOR i IN 2..array_length(v_parts,1) LOOP
            SELECT _structure_id INTO v_cur_sid
              FROM _scheme_metadata_cache
             WHERE _scheme_id = p_scheme_id AND _name = v_parts[i] AND _parent_structure_id = v_cur_sid
             LIMIT 1;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'pvt_resolve_field_path: nested segment "%" of path "%" not found', v_parts[i], p_path;
            END IF;
        END LOOP;

        SELECT db_type, (_collection_type IS NOT NULL)
          INTO v_db_type, v_is_array
          FROM _scheme_metadata_cache
         WHERE _structure_id = v_cur_sid
         LIMIT 1;
        v_db_col := pvt_db_type_to_value_column(v_db_type);
        RETURN jsonb_build_object(
            'kind','field','sid',v_cur_sid,'db_type',v_db_type,'db_column',v_db_col,
            'name',p_path,'is_array',v_is_array,
            'list_item_prop',NULL,'dict_key',NULL,'parent_sid',NULL);
    END IF;

    -- 3. Bare root field (possibly `Foo[]` for arrays).
    v_base_name := p_path;
    IF p_path LIKE '%[]' THEN
        v_base_name   := left(p_path, length(p_path) - 2);
        v_force_array := true;
    END IF;
    SELECT _structure_id, db_type, (_collection_type IS NOT NULL)
      INTO v_sid, v_db_type, v_is_array
      FROM _scheme_metadata_cache
     WHERE _scheme_id = p_scheme_id AND _name = v_base_name AND _parent_structure_id IS NULL
     LIMIT 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'pvt_resolve_field_path: field "%" not found in scheme %', p_path, p_scheme_id;
    END IF;
    IF v_force_array THEN
        v_is_array := true;
    END IF;

    IF v_db_type = 'ListItem' THEN
        v_db_col := '_ListItem';
        v_list_item_prop := 'Id';  -- bare `Status` defaults to `.Id` (bigint FK)
    ELSE
        v_db_col := pvt_db_type_to_value_column(v_db_type);
    END IF;

    RETURN jsonb_build_object(
        'kind','field','sid',v_sid,'db_type',v_db_type,'db_column',v_db_col,
        'name',p_path,'is_array',v_is_array,
        'list_item_prop',v_list_item_prop,'dict_key',NULL,'parent_sid',NULL);
END;
$BODY$;

COMMENT ON FUNCTION pvt_resolve_field_path(bigint, text) IS
    'Resolves a logical field path to a rich FieldInfo jsonb (kind/sid/db_type/db_column/is_array/list_item_prop/dict_key/parent_sid). Mirrors redb.Core.Pro.Schema.SchemeFieldResolver. Throws when the path cannot be matched against the scheme metadata cache.';


-- ===== 11_pvt_column_expr.sql =====
-- =====================================================================
-- 11_pvt_column_expr.sql
-- ---------------------------------------------------------------------
-- Build a single column expression for the PVT CTE: each scheme field
-- becomes a `MAX(v._<TypedCol>) FILTER (WHERE v._id_structure = <sid>)
-- AS "<field>"` aggregate; base fields are projected from `o.*`
-- directly without aggregation.
--
-- Functions:
--   pvt_db_type_to_value_column(p_db_type text) RETURNS text
--   pvt_build_column_expr(p_field_name text, p_field_meta jsonb) RETURNS text
-- =====================================================================

-- ---------- pvt_db_type_to_value_column -------------------------------
-- Maps a logical db_type (as stored in _scheme_metadata_cache.db_type)
-- to the actual physical column name on the _values table. Returns
-- NULL for unsupported types so the caller can raise a clear error.
CREATE OR REPLACE FUNCTION pvt_db_type_to_value_column(p_db_type text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
BEGIN
    RETURN CASE p_db_type
        WHEN 'String'         THEN '_String'
        WHEN 'Long'           THEN '_Long'
        WHEN 'Double'         THEN '_Double'
        WHEN 'Numeric'        THEN '_Numeric'
        WHEN 'Boolean'        THEN '_Boolean'
        WHEN 'Guid'           THEN '_Guid'
        WHEN 'DateTimeOffset' THEN '_DateTimeOffset'
        WHEN 'ByteArray'      THEN '_ByteArray'
        WHEN 'ListItem'       THEN '_ListItem'
        WHEN 'Object'         THEN '_Object'
        -- Legacy / type_semantic aliases.
        WHEN 'TimeSpan'       THEN '_String'    -- TimeSpan stored as text
        WHEN 'DateTime'       THEN '_DateTimeOffset'
        ELSE NULL
    END;
END;
$BODY$;

COMMENT ON FUNCTION pvt_db_type_to_value_column(text) IS
    'Maps a logical db_type to the physical typed column name on _values. Returns NULL if the type is unknown.';


-- ---------- pvt_build_column_expr -------------------------------------
-- Builds a single SELECT-list expression for the PVT pivot CTE.
-- Dispatches on the rich FieldInfo emitted by pvt_resolve_field_path
-- (kind/sid/db_type/db_column/is_array/list_item_prop/dict_key/parent_sid).
--
-- Adopts the Pro pivot idiom `(array_agg(...) FILTER (...))[1]` for
-- scalars (works for booleans/uuids/non-orderable types) and the
-- "_array_index IS NULL" / "IS NOT NULL" / "= '<key>'" filter
-- conventions to distinguish scalar / array / dict element values.
--
-- Nested-dict fields (parent_sid AND dict_key both set) are NOT
-- emitted as pivot columns; they live in a side CTE generated by
-- pvt_build_cte_sql and reach the outer query via a LEFT JOIN alias.
CREATE OR REPLACE FUNCTION pvt_build_column_expr(
    p_field_name text,
    p_field_meta jsonb,
    p_array_index_in_outer boolean DEFAULT false  -- when true, omit `AND v._array_index IS NULL` from scalar FILTER; caller hoists it to an outer WHERE for inline-pivot shape (Pro-parity GROUP BY).
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_kind       text;
    v_sid        bigint;
    v_db_type    text;
    v_col        text;
    v_alias      text;
    v_li_prop    text;
    v_dict_key   text;
    v_parent_sid bigint;
    v_is_array   boolean;
BEGIN
    IF p_field_meta IS NULL THEN
        RAISE EXCEPTION 'pvt_build_column_expr: p_field_meta is NULL for field "%"', p_field_name;
    END IF;

    v_alias      := quote_ident(p_field_name);
    v_kind       := p_field_meta->>'kind';
    v_li_prop    := p_field_meta->>'list_item_prop';
    v_dict_key   := p_field_meta->>'dict_key';
    v_parent_sid := NULLIF(p_field_meta->>'parent_sid', '')::bigint;
    v_is_array   := COALESCE((p_field_meta->>'is_array')::boolean, false);

    -- ---- Base field: straight projection from _objects. -------------
    IF v_kind = 'base' THEN
        RETURN format('o.%I AS %s', p_field_meta->>'column', v_alias);
    END IF;

    v_sid     := (p_field_meta->>'sid')::bigint;
    v_db_type := p_field_meta->>'db_type';

    -- ---- Nested-dict: handled by a side CTE; must not reach here. ---
    IF v_parent_sid IS NOT NULL AND v_dict_key IS NOT NULL THEN
        RAISE EXCEPTION
            'pvt_build_column_expr: nested-dict field "%" must be emitted via side CTE (caller bug)',
            p_field_name;
    END IF;

    -- ---- ListItem.Value / .Alias --------------------------------
    -- Scalar projection emits the raw _ListItem id (parity with Pro);
    -- consumers wrap it in a _list_items lookup as needed (WHERE-
    -- builder, ORDER BY-builder). GROUP BY / SELECT pass the id
    -- through untouched. Array variants still dereference so that
    -- $arrayContains 'admin' / && {...} keep string semantics.
    IF v_li_prop = 'Value' THEN
        -- Pivot source provides `LEFT JOIN _list_items li ON
        -- li._id = v._ListItem` (emitted by pvt_build_cte_sql when any
        -- field is Value/Alias). Scalar takes [1], array keeps array_agg.
        IF v_is_array THEN
            RETURN format(
                'array_agg(li._value) FILTER (WHERE v._id_structure = %s AND v._array_index IS NOT NULL) AS %s',
                v_sid::text, v_alias);
        END IF;
        IF p_array_index_in_outer THEN
            RETURN format(
                '(array_agg(li._value) FILTER (WHERE v._id_structure = %s))[1] AS %s',
                v_sid::text, v_alias);
        END IF;
        RETURN format(
            '(array_agg(li._value) FILTER (WHERE v._id_structure = %s AND v._array_index IS NULL))[1] AS %s',
            v_sid::text, v_alias);
    END IF;

    IF v_li_prop = 'Alias' THEN
        IF v_is_array THEN
            RETURN format(
                'array_agg(li._alias) FILTER (WHERE v._id_structure = %s AND v._array_index IS NOT NULL) AS %s',
                v_sid::text, v_alias);
        END IF;
        IF p_array_index_in_outer THEN
            RETURN format(
                '(array_agg(li._alias) FILTER (WHERE v._id_structure = %s))[1] AS %s',
                v_sid::text, v_alias);
        END IF;
        RETURN format(
            '(array_agg(li._alias) FILTER (WHERE v._id_structure = %s AND v._array_index IS NULL))[1] AS %s',
            v_sid::text, v_alias);
    END IF;

    -- ---- Resolve typed column ---------------------------------------
    v_col := pvt_db_type_to_value_column(v_db_type);
    IF v_col IS NULL THEN
        RAISE EXCEPTION 'pvt_build_column_expr: unsupported db_type "%" for field "%"', v_db_type, p_field_name;
    END IF;
    -- ListItem.Id: project the foreign key column itself (bigint).
    IF v_li_prop = 'Id' THEN
        v_col := '_ListItem';
    END IF;

    -- ---- Simple dictionary: PhoneBook[home] --> _array_index='<key>'
    -- NOTE: value column references use %s (bare) instead of %I (quoted).
    -- Free PG DDL declares _String/_Long/... unquoted, so the actual
    -- column names are case-folded to lowercase; quoting them via %I
    -- (which preserves mixed case) would break the lookup.
    IF v_dict_key IS NOT NULL AND v_parent_sid IS NULL THEN
        RETURN format(
            '(array_agg(v.%s) FILTER (WHERE v._id_structure = %s AND v._array_index = %L))[1] AS %s',
            v_col, v_sid::text, v_dict_key, v_alias);
    END IF;

    -- ---- Array pivot ------------------------------------------------
    IF v_is_array THEN
        RETURN format(
            'array_agg(v.%s) FILTER (WHERE v._id_structure = %s AND v._array_index IS NOT NULL) AS %s',
            v_col, v_sid::text, v_alias);
    END IF;

    -- ---- Scalar pivot (Pro idiom) -----------------------------------
    IF p_array_index_in_outer THEN
        RETURN format(
            '(array_agg(v.%s) FILTER (WHERE v._id_structure = %s))[1] AS %s',
            v_col, v_sid::text, v_alias);
    END IF;
    RETURN format(
        '(array_agg(v.%s) FILTER (WHERE v._id_structure = %s AND v._array_index IS NULL))[1] AS %s',
        v_col, v_sid::text, v_alias);
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_column_expr(text, jsonb, boolean) IS
    'Builds a single SELECT-list expression for the PVT pivot CTE. Dispatches on the rich FieldInfo jsonb (kind/sid/db_type/is_array/list_item_prop/dict_key/parent_sid). Supports base / scalar / array / simple-dict / ListItem.Id|Value|Alias (scalar+array). Nested-dict fields are deferred to a side CTE emitted by pvt_build_cte_sql. p_array_index_in_outer=true drops `AND v._array_index IS NULL` from scalar/ListItem.Id|Value|Alias FILTER clauses; caller must hoist it to an outer WHERE (Pro-parity inline GROUP BY shape).';;


-- ===== 12_pvt_cte_builder.sql =====
-- =====================================================================
-- 12_pvt_cte_builder.sql
-- ---------------------------------------------------------------------
-- Build the `WITH _pvt_cte AS (...)` clause that pivots _values into
-- one column per requested scheme field. All downstream predicates
-- (WHERE / ORDER BY) reference columns of this CTE.
--
-- Output shape (legacy WIDE, used for null-checks and tree mode):
--   WITH _pvt_cte AS (
--       SELECT
--           o._id, o._id_parent, ... <all 21 base cols>,
--           <one column per field via pvt_build_column_expr>
--       FROM _objects o
--       <LEFT|INNER> JOIN _values v ON v._id_object = o._id
--       WHERE o._id_scheme = <scheme_id>
--         [AND tree-restriction]
--         [AND <pushed base predicate>]
--       GROUP BY o._id, o._id_parent, ... (all _objects columns above)
--   )
--
-- Output shape (NARROW Pro-parity, used for flat + non-null queries):
--   WITH _pvt_cte AS (
--       SELECT
--           v._id_object,
--           <one column per field via pvt_build_column_expr>
--       FROM _values v
--       WHERE v._id_structure = ANY(ARRAY[<sid1>, <sid2>, ...]::bigint[])
--         AND v._id_object IN (
--             SELECT _id FROM _objects
--              WHERE _id_scheme = <scheme_id>
--                [AND <pushed base predicate>]
--                [AND o._id IN (SELECT _id FROM _pvt_tree)]
--         )
--       GROUP BY v._id_object
--   )
-- The narrow shape skips the wide GROUP BY (21 cols -> 1 col),
-- narrows the _values scan via the `_id_structure = ANY` index hint,
-- and lets the outer SELECT JOIN _objects so the planner can still
-- short-circuit by system-column indexes. Tree pushdown (Pro
-- CHANGELOG 2.0.1 parity): in tree mode the recursive `_pvt_tree`
-- CTE is emitted alongside `_pvt_cte` and its restriction is folded
-- INTO the narrow IN-subquery, so the recursive walk + base
-- pushdown both filter the object set BEFORE _values is touched.
--
-- Legacy WIDE always projects every _objects column the C# layer
-- needs to hydrate base fields. Narrow shape projects only
-- v._id_object; the outer query MUST `JOIN _objects o ON o._id =
-- _pvt_cte._id_object` to access base columns.
-- =====================================================================

-- Signature evolved in v0.6.0 (added p_include_seed + p_polymorphic and five
-- explicit tree modes). CREATE OR REPLACE cannot change argument lists, so
-- drop every legacy form here.
DROP FUNCTION IF EXISTS pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean);
DROP FUNCTION IF EXISTS pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean, text);
DROP FUNCTION IF EXISTS pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean, text, boolean);
DROP FUNCTION IF EXISTS pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean, text, boolean, boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_cte_sql(
    p_scheme_id     bigint,
    p_fields        jsonb,
    p_source_mode   text     DEFAULT 'flat',
    p_tree_ids      bigint[] DEFAULT NULL,
    p_max_depth     integer  DEFAULT NULL,
    p_force_outer   boolean  DEFAULT true,
    p_extra_where   text     DEFAULT NULL,  -- Pro-style base-field pushdown: extra predicate AND-ed into the inner WHERE.
    p_narrow        boolean  DEFAULT false, -- emit Pro-shape narrow pivot (FROM _values + IN (...) + GROUP BY 1).
    p_include_seed  boolean  DEFAULT true,  -- include the seed object(s) themselves in tree_descendants / tree (false matches old free `WHERE depth>0`).
    p_polymorphic   boolean  DEFAULT true,  -- when false, AND `_id_scheme = p_scheme_id` into the recursive walk (Pro keeps walk polymorphic).
    p_residual_where text    DEFAULT NULL   -- Pro-parity WHERE pushdown: residual predicate (refs pivot cols only) wrapped INSIDE _pvt_cte so it filters BEFORE the outer JOIN _objects.
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_pivot_cols text := '';
    v_join_kind  text;
    v_field_name text;
    v_field_meta jsonb;
    v_first       boolean := true;
    v_base_cols   constant text :=
        'o._id, o._id_parent, o._id_scheme, o._id_owner, o._id_who_change, '
        || 'o._name, o._date_create, o._date_modify, o._date_begin, o._date_complete, '
        || 'o._key, o._note, o._hash, '
        || 'o._value_long, o._value_string, o._value_guid, o._value_bool, '
        || 'o._value_double, o._value_numeric, o._value_datetime, o._value_bytes';
    v_where           text := format('o._id_scheme = %s', p_scheme_id::text);
    v_cte_parts       text[] := ARRAY[]::text[];
    v_has_recursive   boolean := false;
    v_nested_groups   jsonb := '{}'::jsonb;
    v_group_key       text;
    v_group_entry     jsonb;
    v_group_fields    jsonb;
    v_inner_field     jsonb;
    v_inner_meta      jsonb;
    v_inner_col       text;
    v_inner_sid       text;
    v_inner_alias     text;
    v_inner_is_array  boolean;
    v_inner_sub       text;
    v_inner_subs      text;
    v_group_idx       integer := 0;
    v_group_cte_name  text;
    v_nested_cols_sql text := '';
    v_nested_joins    text := '';
    v_inner_pivot     text;
    v_pvt_body        text;
    v_sids            bigint[] := ARRAY[]::bigint[];
    v_sid_text        text;
    v_objects_subq    text;
    v_tree_filter     text := '';
    -- Nested-only optimization (Pro-parity, see E062 in CHANGELOG):
    -- when v_pivot_cols is empty AND there's exactly one nested-dict
    -- group AND no force_outer AND narrow flag set, skip the heavy
    -- `pi` wrapper (SELECT 21 base cols FROM _objects ... GROUP BY 21)
    -- and emit `_pvt_cte AS (SELECT ndN._id_object, ndN.<cols> FROM
    -- nested_dict_N ndN)` directly. Object-set restriction (scheme +
    -- base pushdown + tree filter) is folded into the nested CTE's
    -- inner WHERE via v_nested_obj_filter.
    v_is_nested_only      boolean := false;
    v_nested_obj_filter   text    := '';
    v_flat_cols_sql       text    := '';
    v_pivot_cols_empty    boolean;
    -- Quoted aliases of every column projected by the pivot body
    -- (scalar + nested-dict). Used to emit Pro-shape `SELECT pvt._id_object,
    -- pvt."col1", pvt."col2" FROM (<inner>) pvt WHERE <residual>` when
    -- pvt_build_query_sql pushes a residual predicate inside _pvt_cte.
    v_pivot_aliases       text[]  := ARRAY[]::text[];
    -- True when any pivot field is ListItem.Value/.Alias (scalar or
    -- array). Triggers `LEFT JOIN _list_items li ON li._id = v._ListItem`
    -- on the pivot source so column exprs can reference `li._value`
    -- / `li._alias` instead of correlated subselects (Pro parity).
    v_has_listitem_join   boolean := false;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_cte_sql: p_scheme_id is required';
    END IF;
    IF p_source_mode NOT IN ('flat', 'tree', 'tree_descendants', 'tree_children', 'tree_roots', 'tree_leaves', 'tree_ancestors') THEN
        RAISE EXCEPTION 'pvt_build_cte_sql: invalid p_source_mode "%", expected flat|tree|tree_descendants|tree_children|tree_roots|tree_leaves|tree_ancestors', p_source_mode;
    END IF;

    -- ------- Partition requested fields: base / nested-dict / pivot --
    IF p_fields IS NOT NULL AND p_fields <> '{}'::jsonb THEN
        FOR v_field_name, v_field_meta IN
            SELECT key, value FROM jsonb_each(p_fields)
        LOOP
            -- Base fields are already in v_base_cols; skip to avoid dupes.
            IF v_field_meta->>'kind' = 'base' THEN
                CONTINUE;
            END IF;
            -- Modifier-only entries (.$length / .$count) project no
            -- column of their own; their base entry was inserted by
            -- pvt_collect_fields and will be emitted normally below.
            IF COALESCE((v_field_meta->>'length_modifier')::boolean, false) THEN
                CONTINUE;
            END IF;
            -- Collect structure id for the narrow Pro-shape
            -- `_id_structure = ANY(...)` filter. Skip nested-dict child
            -- fields (parent_sid set: they project from a side CTE
            -- scoped to the parent dict row, not the main _values scan).
            -- Simple-dict fields (dict_key set, parent_sid NULL) keep
            -- their sid here because pvt_build_column_expr's FILTER
            -- (_id_structure = X AND _array_index = 'key') reads
            -- straight from the main scan.
            IF (v_field_meta->>'sid') IS NOT NULL
               AND (v_field_meta->>'parent_sid') IS NULL THEN
                v_sids := v_sids || ((v_field_meta->>'sid')::bigint);
            END IF;
            -- Nested-dict: accumulate per (parent_sid, dict_key) group.
            IF (v_field_meta->>'dict_key') IS NOT NULL
               AND (v_field_meta->>'parent_sid') IS NOT NULL THEN
                v_group_key := (v_field_meta->>'parent_sid')
                               || '|' || (v_field_meta->>'dict_key');
                v_group_entry := COALESCE(
                    v_nested_groups -> v_group_key,
                    jsonb_build_object(
                        'parent_sid', v_field_meta->>'parent_sid',
                        'dict_key',   v_field_meta->>'dict_key',
                        'fields',     '[]'::jsonb));
                v_group_entry := jsonb_set(
                    v_group_entry,
                    '{fields}',
                    (v_group_entry->'fields')
                        || jsonb_build_array(
                            jsonb_build_object(
                                'name', v_field_name,
                                'meta', v_field_meta)));
                v_nested_groups := jsonb_set(
                    v_nested_groups,
                    ARRAY[v_group_key],
                    v_group_entry,
                    true);
                CONTINUE;
            END IF;
            -- Regular pivot column.
            IF NOT v_first THEN
                v_pivot_cols := v_pivot_cols || ',' || E'\n            ';
            ELSE
                v_pivot_cols := E',\n            ';
                v_first := false;
            END IF;
            v_pivot_cols := v_pivot_cols
                || pvt_build_column_expr(v_field_name, v_field_meta);
            v_pivot_aliases := v_pivot_aliases || quote_ident(v_field_name);
            IF (v_field_meta->>'list_item_prop') IN ('Value', 'Alias') THEN
                v_has_listitem_join := true;
            END IF;
        END LOOP;
    END IF;

    -- ------- JOIN kind: LEFT keeps objects with no _values rows -------
    v_join_kind := CASE WHEN p_force_outer THEN 'LEFT JOIN' ELSE 'INNER JOIN' END;

    -- ------- Tree-mode object-set restriction -------------------------
    -- Emit the `_pvt_tree(_id, depth)` CTE matching the requested
    -- tree mode (descendants / children / roots / leaves / ancestors),
    -- then capture the restriction `o._id IN (SELECT _id FROM
    -- _pvt_tree [WHERE depth>0])` in v_tree_filter so it can be
    -- AND-ed into either the wide WHERE or the narrow IN-subquery.
    --
    --   tree / tree_descendants  recursive walk DOWN from p_tree_ids.
    --                            depth=0 at seed; descendants depth>0.
    --                            p_max_depth caps walk; p_include_seed
    --                            controls whether seed itself stays.
    --   tree_children            single-level non-recursive: direct
    --                            children of p_tree_ids.
    --   tree_roots               non-recursive: scheme roots
    --                            (`_id_parent IS NULL`). p_tree_ids
    --                            optional and restricts the root set.
    --   tree_leaves              non-recursive: scheme objects with
    --                            no children. p_tree_ids optional.
    --   tree_ancestors           recursive walk UP from p_tree_ids.
    --                            seed depth=1 = direct parent; the
    --                            objects themselves are NOT in the
    --                            walk (seed=parent), so p_include_seed
    --                            does not apply here.
    --
    -- Polymorphic flag: when p_polymorphic = false AND scheme filter
    -- is folded into the recursive parts (Pro keeps walks polymorphic
    -- so cross-scheme parents can stitch hierarchies together).
    DECLARE
        v_seed_arr    text;
        v_scheme_pred text := '';
    BEGIN
        IF p_source_mode <> 'flat' THEN
            v_seed_arr := CASE
                WHEN p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL
                THEN NULL
                ELSE quote_literal(p_tree_ids::text) || '::bigint[]'
            END;
            IF NOT p_polymorphic THEN
                v_scheme_pred := ' AND o._id_scheme = ' || p_scheme_id::text;
            END IF;
        END IF;

        IF p_source_mode IN ('tree', 'tree_descendants') THEN
            IF v_seed_arr IS NULL THEN
                RAISE EXCEPTION 'pvt_build_cte_sql: p_tree_ids is required when p_source_mode = ''%''', p_source_mode;
            END IF;
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT _id, 0 FROM _objects WHERE _id = ANY(' || v_seed_arr || ')'
                || E'\n    UNION ALL'
                || E'\n    SELECT o._id, t.depth + 1 FROM _objects o '
                || 'JOIN _pvt_tree t ON o._id_parent = t._id'
                || CASE WHEN p_max_depth IS NOT NULL OR NOT p_polymorphic
                        THEN ' WHERE '
                             || CASE WHEN p_max_depth IS NOT NULL
                                     THEN 't.depth < ' || p_max_depth::text
                                     ELSE 'TRUE' END
                             || CASE WHEN NOT p_polymorphic
                                     THEN v_scheme_pred
                                     ELSE '' END
                        ELSE '' END
                || E'\n)');
            v_has_recursive := true;
            v_tree_filter := CASE WHEN p_include_seed
                                  THEN 'o._id IN (SELECT _id FROM _pvt_tree)'
                                  ELSE 'o._id IN (SELECT _id FROM _pvt_tree WHERE depth > 0)' END;

        ELSIF p_source_mode = 'tree_children' THEN
            IF v_seed_arr IS NULL THEN
                RAISE EXCEPTION 'pvt_build_cte_sql: p_tree_ids is required when p_source_mode = ''tree_children''';
            END IF;
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT o._id, 1 FROM _objects o '
                || 'WHERE o._id_parent = ANY(' || v_seed_arr || ')'
                || CASE WHEN NOT p_polymorphic THEN v_scheme_pred ELSE '' END
                || E'\n)');
            v_tree_filter := 'o._id IN (SELECT _id FROM _pvt_tree)';

        ELSIF p_source_mode = 'tree_roots' THEN
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT o._id, 0 FROM _objects o '
                || 'WHERE o._id_parent IS NULL AND o._id_scheme = ' || p_scheme_id::text
                || CASE WHEN v_seed_arr IS NOT NULL
                        THEN ' AND o._id = ANY(' || v_seed_arr || ')'
                        ELSE '' END
                || E'\n)');
            v_tree_filter := 'o._id IN (SELECT _id FROM _pvt_tree)';

        ELSIF p_source_mode = 'tree_leaves' THEN
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT o._id, 0 FROM _objects o '
                || 'WHERE o._id_scheme = ' || p_scheme_id::text
                || ' AND NOT EXISTS (SELECT 1 FROM _objects c WHERE c._id_parent = o._id)'
                || CASE WHEN v_seed_arr IS NOT NULL
                        THEN ' AND o._id = ANY(' || v_seed_arr || ')'
                        ELSE '' END
                || E'\n)');
            v_tree_filter := 'o._id IN (SELECT _id FROM _pvt_tree)';

        ELSIF p_source_mode = 'tree_ancestors' THEN
            IF v_seed_arr IS NULL THEN
                RAISE EXCEPTION 'pvt_build_cte_sql: p_tree_ids is required when p_source_mode = ''tree_ancestors''';
            END IF;
            -- Walk UP via _id_parent. Seed: immediate parents of
            -- p_tree_ids (depth=1). Recursive step: grandparents and
            -- so on until _id_parent IS NULL or depth cap reached.
            v_cte_parts := v_cte_parts || (
                '_pvt_tree(_id, depth) AS ('
                || E'\n    SELECT seed._id_parent, 1 FROM _objects seed '
                || 'WHERE seed._id = ANY(' || v_seed_arr || ') AND seed._id_parent IS NOT NULL'
                || E'\n    UNION ALL'
                || E'\n    SELECT o._id_parent, t.depth + 1 FROM _objects o '
                || 'JOIN _pvt_tree t ON o._id = t._id'
                || ' WHERE o._id_parent IS NOT NULL'
                || CASE WHEN p_max_depth IS NOT NULL
                        THEN ' AND t.depth < ' || p_max_depth::text
                        ELSE '' END
                || CASE WHEN NOT p_polymorphic THEN v_scheme_pred ELSE '' END
                || E'\n)');
            v_has_recursive := true;
            v_tree_filter := 'o._id IN (SELECT _id FROM _pvt_tree)';
        END IF;

        IF v_tree_filter <> '' THEN
            v_where := v_where || ' AND ' || v_tree_filter;
        END IF;
    END;

    -- ------- Base-field pushdown (Pro parity) -------------------------
    -- pvt_split_filter feeds us a ready-baked predicate fragment that
    -- references `o.*` directly. We AND it into the inner WHERE so the
    -- planner can apply system-column indexes BEFORE the JOIN with
    -- _values and BEFORE the GROUP BY. Cuts work from O(scheme_size)
    -- down to O(filtered_objects).
    IF p_extra_where IS NOT NULL AND p_extra_where <> '' THEN
        v_where := v_where || ' AND ' || p_extra_where;
    END IF;

    -- ------- Nested-dict side CTEs (Pro pattern) ----------------------
    -- For each group (parent_sid, dict_key) we emit ONE CTE that scans
    -- the parent dict-row from _values and projects each child field as
    -- a correlated subselect over child _values rows. Outer _pvt_cte
    -- then LEFT JOINs all groups by _id_object.
    --
    -- Nested-only detection: when v_pivot_cols is empty AND exactly
    -- one nested-dict group is present AND p_narrow / NOT p_force_outer,
    -- the outer pi wrapper is wasteful. Fold the object-set restriction
    -- (scheme + base pushdown + tree filter) into the nested CTE's WHERE
    -- and emit a flat _pvt_cte body below.
    v_pivot_cols_empty := (COALESCE(v_pivot_cols, '') = '');
    v_is_nested_only := p_narrow
                       AND NOT p_force_outer
                       AND v_pivot_cols_empty
                       AND (v_nested_groups <> '{}'::jsonb);
    -- Pro-parity: always fold object-set restriction (scheme + base
    -- pushdown + tree filter) into every nested_dict_N CTE so the
    -- planner can prune dp rows by _id_scheme BEFORE the LEFT JOIN nv
    -- expansion. Previously we set this only on the nested-only path;
    -- mixed scalar+nested queries were left scanning the full _values
    -- partition for the parent sid. Mirrors PG PRO 12.
    v_nested_obj_filter := format(
        ' AND dp._id_object IN (SELECT o._id FROM _objects o WHERE o._id_scheme = %s%s%s)',
        p_scheme_id::text,
        CASE WHEN p_extra_where IS NOT NULL AND p_extra_where <> ''
             THEN ' AND ' || p_extra_where
             ELSE '' END,
        CASE WHEN v_tree_filter <> ''
             THEN ' AND ' || v_tree_filter
             ELSE '' END);

    FOR v_group_key, v_group_entry IN
        SELECT key, value FROM jsonb_each(v_nested_groups)
    LOOP
        v_group_idx := v_group_idx + 1;
        v_group_cte_name := 'nested_dict_' || v_group_idx::text;
        v_group_fields   := v_group_entry->'fields';
        v_inner_subs := '';
        DECLARE
            v_inner_sids text := '';   -- comma-sep sid list for the LEFT JOIN predicate
            v_seen       jsonb := '{}'::jsonb;
        BEGIN
        FOR v_inner_field IN SELECT * FROM jsonb_array_elements(v_group_fields) LOOP
            v_inner_meta     := v_inner_field->'meta';
            v_inner_alias    := v_inner_field->>'name';
            v_inner_sid      := v_inner_meta->>'sid';
            v_inner_col      := v_inner_meta->>'db_column';
            v_inner_is_array := COALESCE((v_inner_meta->>'is_array')::boolean, false);
            IF (v_inner_meta->>'list_item_prop') IS NOT NULL THEN
                RAISE EXCEPTION
                    'pvt_build_cte_sql: nested-dict ListItem accessor not supported in v0.3.0 (field "%")',
                    v_inner_alias;
            END IF;
            -- Pro-parity aggregate: one LEFT JOIN over all sids in the
            -- group + per-field array_agg(...) FILTER (WHERE _id_structure
            -- = SID AND _array_index <kind>). Scalar takes [1]; array
            -- takes the array_agg as-is.
            IF v_inner_is_array THEN
                v_inner_sub := format(
                    'array_agg(nv.%s) FILTER (WHERE nv._id_structure = %s AND nv._array_index IS NOT NULL) AS %I',
                    v_inner_col, v_inner_sid, v_inner_alias);
            ELSE
                v_inner_sub := format(
                    '(array_agg(nv.%s) FILTER (WHERE nv._id_structure = %s AND nv._array_index IS NULL))[1] AS %I',
                    v_inner_col, v_inner_sid, v_inner_alias);
            END IF;
            IF v_inner_subs <> '' THEN
                v_inner_subs := v_inner_subs || ',' || E'\n            ';
            END IF;
            v_inner_subs := v_inner_subs || v_inner_sub;
            -- Collect distinct sids for the JOIN predicate.
            IF (v_seen ? v_inner_sid) = false THEN
                v_seen := v_seen || jsonb_build_object(v_inner_sid, true);
                IF v_inner_sids <> '' THEN
                    v_inner_sids := v_inner_sids || ', ';
                END IF;
                v_inner_sids := v_inner_sids || v_inner_sid;
            END IF;
            -- Track alias for the explicit-cols residual wrapper.
            v_pivot_aliases := v_pivot_aliases || quote_ident(v_inner_alias);
            -- Outer projection + JOIN parts
            v_nested_cols_sql := v_nested_cols_sql
                || ', ' || quote_ident(v_group_cte_name) || '.' || quote_ident(v_inner_alias)
                || ' AS ' || quote_ident(v_inner_alias);
            -- Flat projection (nested-only path): no `pi.` prefix; the
            -- outer SELECT JOINs _objects on _pvt_cte._id_object.
            IF v_is_nested_only THEN
                IF v_flat_cols_sql <> '' THEN
                    v_flat_cols_sql := v_flat_cols_sql || ', ';
                END IF;
                v_flat_cols_sql := v_flat_cols_sql
                    || quote_ident(v_group_cte_name) || '.' || quote_ident(v_inner_alias)
                    || ' AS ' || quote_ident(v_inner_alias);
            END IF;
            -- (joins built once per group below)
        END LOOP;
        v_cte_parts := v_cte_parts || format(
            '%I AS (' || E'\n        SELECT dp._id_object,\n            %s\n        FROM _values dp\n        LEFT JOIN _values nv ON nv._array_parent_id = dp._id AND nv._id_structure %s\n        WHERE dp._id_structure = %s AND dp._array_index = %L%s\n        GROUP BY dp._id_object\n    )',
            v_group_cte_name,
            v_inner_subs,
            CASE WHEN position(',' in v_inner_sids) > 0
                 THEN 'IN (' || v_inner_sids || ')'
                 ELSE '= ' || v_inner_sids END,
            v_group_entry->>'parent_sid',
            v_group_entry->>'dict_key',
            v_nested_obj_filter);
        v_nested_joins := v_nested_joins
            || E'\n        LEFT JOIN ' || quote_ident(v_group_cte_name)
            || ' ON ' || quote_ident(v_group_cte_name) || '._id_object = pi._id';
        END;
    END LOOP;

    -- ------- Inner pivot SELECT ---------------------------------------
    -- Narrow Pro-shape: scan _values directly, restrict by
    -- _id_structure ANY(<requested sids>), restrict object set via an
    -- IN-subquery over _objects (carrying scheme + base pushdown +
    -- tree restriction), and group by a single column (_id_object).
    -- The outer SELECT must JOIN _objects to expose base columns.
    -- Eligible iff: narrow flag set, no LEFT JOIN required (would
    -- lose missing _values rows), no nested-dict groups (they require
    -- pi.* projection over the wide source), and at least one pivot
    -- sid was collected. Tree mode IS supported: v_tree_filter is
    -- folded into the IN-subquery so the recursive `_pvt_tree` walk
    -- and base pushdown both run before the _values scan.
    IF p_narrow
       AND NOT p_force_outer
       AND v_group_idx = 0
       AND array_length(v_sids, 1) IS NOT NULL THEN

        SELECT string_agg(DISTINCT s::text, ', ')
          INTO v_sid_text
          FROM unnest(v_sids) AS s;

        v_objects_subq := format(
            '(SELECT o._id FROM _objects o WHERE o._id_scheme = %s%s%s)',
            p_scheme_id::text,
            CASE WHEN p_extra_where IS NOT NULL AND p_extra_where <> ''
                 THEN ' AND ' || p_extra_where
                 ELSE '' END,
            CASE WHEN v_tree_filter <> ''
                 THEN ' AND ' || v_tree_filter
                 ELSE '' END);

        v_inner_pivot :=
            E'SELECT\n            v._id_object' || v_pivot_cols ||
            E'\n        FROM _values v' ||
            CASE WHEN v_has_listitem_join
                 THEN E'\n        LEFT JOIN _list_items li ON li._id = v._ListItem'
                 ELSE '' END ||
            E'\n        WHERE v._id_structure = ANY(ARRAY[' || v_sid_text || ']::bigint[])' ||
            E'\n          AND v._id_object IN ' || v_objects_subq ||
            E'\n        GROUP BY v._id_object';
    ELSE
        v_inner_pivot :=
            E'SELECT\n            ' || v_base_cols || v_pivot_cols ||
            E'\n        FROM _objects o\n        ' || v_join_kind ||
            E' _values v ON v._id_object = o._id' ||
            CASE WHEN v_has_listitem_join
                 THEN E'\n        LEFT JOIN _list_items li ON li._id = v._ListItem'
                 ELSE '' END ||
            E'\n        WHERE ' || v_where ||
            E'\n        GROUP BY ' || v_base_cols;
    END IF;

    -- ------- Final _pvt_cte body --------------------------------------
    IF v_is_nested_only THEN
        -- Nested-only: flatten across all nested_dict_N groups joined
        -- on _id_object. First group is the seed, additional groups
        -- LEFT JOIN onto it (objects missing entries in later groups
        -- keep NULL for those cols, matching wide-path LEFT JOIN).
        DECLARE
            v_nested_from text := '';
            v_idx         integer;
        BEGIN
            FOR v_idx IN 1..v_group_idx LOOP
                IF v_idx = 1 THEN
                    v_nested_from := 'nested_dict_1';
                ELSE
                    v_nested_from := v_nested_from
                        || E'\n        LEFT JOIN ' || 'nested_dict_' || v_idx::text
                        || ' ON ' || 'nested_dict_' || v_idx::text
                        || '._id_object = nested_dict_1._id_object';
                END IF;
            END LOOP;
            v_pvt_body :=
                'SELECT nested_dict_1._id_object, ' || v_flat_cols_sql
                || E'\n        FROM ' || v_nested_from;
        END;
    ELSIF v_group_idx = 0 THEN
        v_pvt_body := v_inner_pivot;
    ELSE
        v_pvt_body :=
            E'SELECT pi.*' || v_nested_cols_sql ||
            E'\n        FROM (' || v_inner_pivot || E') pi'
            || v_nested_joins;
    END IF;

    -- ------- Residual WHERE pushdown (Pro parity) ---------------------
    -- When pvt_build_query_sql determines the outer filter touches only
    -- pivoted columns, it passes the pre-rendered SQL here. Wrapping it
    -- inside _pvt_cte lets the planner prune rows BEFORE the outer
    -- JOIN _objects, mirroring Pro's `WHERE pvt.<col> = $N` shape.
    -- Explicit column list (not SELECT *): matches Pro 1:1 and keeps
    -- the projection contract stable for downstream consumers.
    IF p_residual_where IS NOT NULL
       AND p_residual_where <> ''
       AND p_residual_where <> 'TRUE' THEN
        DECLARE
            v_wrap_cols text;
        BEGIN
            v_wrap_cols := 'pvt._id_object';
            IF array_length(v_pivot_aliases, 1) IS NOT NULL THEN
                v_wrap_cols := v_wrap_cols
                            || ', pvt.'
                            || array_to_string(v_pivot_aliases, ', pvt.');
            END IF;
            v_pvt_body := 'SELECT ' || v_wrap_cols
                       || E'\n        FROM ('
                       || E'\n        ' || v_pvt_body
                       || E'\n        ) pvt WHERE ' || p_residual_where;
        END;
    END IF;

    v_cte_parts := v_cte_parts ||
        ('_pvt_cte AS (' || E'\n        ' || v_pvt_body || E'\n    )');

    -- ------- Assemble final CTE SQL -----------------------------------
    RETURN (CASE WHEN v_has_recursive THEN 'WITH RECURSIVE ' ELSE 'WITH ' END)
           || array_to_string(v_cte_parts, E',\n');
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_cte_sql(bigint, jsonb, text, bigint[], integer, boolean, text, boolean, boolean, boolean, text) IS
    'Builds the WITH _pvt_cte AS (...) clause. Two pivot shapes: wide legacy (`FROM _objects o JOIN _values v` GROUP BY 21 base cols) used for null-checks, tree mode, and nested-dict groups; narrow Pro-shape (`FROM _values v WHERE _id_structure=ANY(...) AND _id_object IN (SELECT _id FROM _objects WHERE ...)` GROUP BY 1) selected via p_narrow=true. In narrow mode the outer SELECT must JOIN _objects to expose base columns. Source modes: flat | tree (alias for tree_descendants) | tree_descendants | tree_children (single level, non-recursive) | tree_roots (`_id_parent IS NULL`) | tree_leaves (no children, non-recursive) | tree_ancestors (recursive walk UP via _id_parent). p_include_seed=false strips the seed from tree_descendants results; p_polymorphic=false ANDs scheme_id into recursive walk.';


-- ===== 13_pvt_condition.sql =====
-- =====================================================================
-- 13_pvt_condition.sql
-- ---------------------------------------------------------------------
-- Build a SQL predicate fragment for a single leaf node in the filter
-- JSON, referencing a CTE column (NOT the raw _values row). All
-- supported operators in v0.1.0 are emitted here; unsupported ones
-- raise with a clear message.
--
-- Function:
--   pvt_build_field_condition(p_field_name, p_field_meta, p_op_json) RETURNS text
--
-- Input shapes:
--   p_op_json = scalar  -> shorthand for {"$eq": <scalar>}
--   p_op_json = {"$op": <value>, ...}
--
-- Supported operators in v0.1.0:
--   $eq, $ne, $gt, $gte, $lt, $lte,
--   $in, $nin,
--   $like, $ilike,
--   $startsWith, $endsWith, $contains            (case-sensitive, LIKE),
--   $startsWithIgnoreCase, $endsWithIgnoreCase,
--   $containsIgnoreCase                          (case-insensitive, ILIKE),
--   $null, $exists, $isNull, $notNull
-- =====================================================================

-- Signature evolved in v0.4.0 (added p_base_prefix). CREATE OR REPLACE
-- cannot change a function's argument list, so drop the legacy form
-- first if present.
DROP FUNCTION IF EXISTS pvt_build_field_condition(text, jsonb, jsonb);

CREATE OR REPLACE FUNCTION pvt_build_field_condition(
    p_field_name   text,
    p_field_meta   jsonb,
    p_op_json      jsonb,
    p_base_prefix  text DEFAULT ''   -- '' → reference CTE column; 'o.' → reference _objects directly (pushdown)
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_col      text;
    v_kind     text;
    v_db_type  text;
    v_cast     text;
    v_elem_cast text;
    v_li_prop  text;
    v_is_array boolean;
    v_length_mod boolean;
    v_arr_col  text;
    v_op_key   text;
    v_op_val   jsonb;
    v_parts    text[] := ARRAY[]::text[];
    v_op_count integer := 0;
    v_op_norm  text;
    v_lit      text;
    v_arr_lit  text;
BEGIN
    IF p_field_meta IS NULL THEN
        RAISE EXCEPTION 'pvt_build_field_condition: p_field_meta is NULL for field "%"', p_field_name;
    END IF;

    v_kind     := p_field_meta->>'kind';
    v_li_prop  := p_field_meta->>'list_item_prop';
    v_is_array := COALESCE((p_field_meta->>'is_array')::boolean, false);
    v_db_type  := p_field_meta->>'db_type';
    v_length_mod := COALESCE((p_field_meta->>'length_modifier')::boolean, false);
    v_arr_col  := quote_ident(COALESCE(p_field_meta->>'base_name', p_field_name));

    -- ---- Element-type cast (used by $arrayXxx scalar operands) ------
    v_elem_cast := CASE pvt_db_type_to_value_column(v_db_type)
        WHEN '_String'         THEN '::text'
        WHEN '_Long'           THEN '::bigint'
        WHEN '_Double'         THEN '::double precision'
        WHEN '_Numeric'        THEN '::numeric'
        WHEN '_Boolean'        THEN '::boolean'
        WHEN '_Guid'           THEN '::uuid'
        WHEN '_DateTimeOffset' THEN '::timestamptz'
        WHEN '_ByteArray'      THEN '::bytea'
        WHEN '_ListItem'       THEN '::bigint'
        WHEN '_Object'         THEN '::bigint'
        ELSE ''
    END;

    -- ---- LHS column reference ---------------------------------------
    -- length/count modifier rewrites the LHS to a scalar-int expression:
    --   * is_array=true  -> array_length(<arr>, 1)
    --   * is_array=false -> LENGTH(<text>) (string length, e.g. p.Name.Length)
    -- Base fields project from _objects under their system column name
    -- (e.g. `_id`, `_name`) and never under the user-facing path (e.g.
    -- `0$:Id`); ordinary cases reference the CTE alias which matches the
    -- original field path.
    DECLARE
        v_length_target text;
    BEGIN
        v_length_target := CASE
            WHEN v_kind = 'base'
                THEN p_base_prefix || quote_ident(p_field_meta->>'column')
            ELSE v_arr_col
        END;
        v_col := CASE
            WHEN v_length_mod AND v_is_array
                THEN 'COALESCE(array_length(' || v_length_target || ', 1), 0)'
            WHEN v_length_mod
                THEN 'COALESCE(LENGTH(' || v_length_target || '), 0)'
            WHEN v_kind = 'base'
                THEN p_base_prefix || quote_ident(p_field_meta->>'column')
            ELSE quote_ident(p_field_name)
        END;
        -- Scalar ListItem.Value / .Alias: pivot column holds the raw
        -- _ListItem id (parity with Pro). WHERE compares by id; the caller
        -- (LINQ translator) is responsible for resolving a string like
        -- 'Active' to its _list_items._id before passing it down. Only
        -- ORDER BY dereferences to _value / _alias for textual sort.
        -- Array path is unchanged: pivot emits text[] of values.
    END;

    -- ---- Pick a SQL cast suitable for the resolved CTE column type --
    -- For base fields the cast must reflect the underlying _objects
    -- column type, otherwise comparisons such as `o._id IN (text...)`
    -- raise `bigint = text`.
    v_cast := CASE
        WHEN v_length_mod                  THEN ''
        WHEN v_kind = 'base'               THEN
            CASE p_field_meta->>'column'
                WHEN '_id'              THEN '::bigint'
                WHEN '_id_parent'       THEN '::bigint'
                WHEN '_id_scheme'       THEN '::bigint'
                WHEN '_id_owner'        THEN '::bigint'
                WHEN '_id_who_change'   THEN '::bigint'
                WHEN '_value_long'      THEN '::bigint'
                WHEN '_key'             THEN '::bigint'
                WHEN '_value_double'    THEN '::double precision'
                WHEN '_value_numeric'   THEN '::numeric'
                WHEN '_value_bool'      THEN '::boolean'
                WHEN '_value_guid'      THEN '::uuid'
                WHEN '_value_datetime'  THEN '::timestamptz'
                WHEN '_date_create'     THEN '::timestamptz'
                WHEN '_date_modify'     THEN '::timestamptz'
                WHEN '_date_begin'      THEN '::timestamptz'
                WHEN '_date_complete'   THEN '::timestamptz'
                ELSE '' -- _name / _note / _value_string / _hash / _value_bytes -> text/bytea, no cast needed
            END
        -- Scalar ListItem.Value/.Alias pivot column holds the resolved
        -- _list_items._value / _alias string (pvt_build_cte_sql adds a
        -- LEFT JOIN _list_items li when needed; pvt_build_column_expr
        -- aggregates li._value / li._alias). Free LINQ passes string
        -- literals; comparison is text. Array path also dereferences
        -- to text[]. ListItem.Id keeps bigint.
        WHEN v_li_prop IN ('Value', 'Alias')                    THEN '::text'
        WHEN v_li_prop = 'Id'              THEN '::bigint'
        ELSE v_elem_cast
    END;

    -- ---- Shorthand: scalar literal == {"$eq": <literal>} ----
    IF jsonb_typeof(p_op_json) <> 'object' THEN
        IF jsonb_typeof(p_op_json) = 'null' THEN
            RETURN format('%s IS NULL', v_col);
        END IF;
        RETURN format('%s = %L%s', v_col, pvt_jsonb_to_sql_literal(p_op_json), v_cast);
    END IF;

    -- ---- Iterate operator object (AND of every op present) ----
    FOR v_op_key, v_op_val IN SELECT key, value FROM jsonb_each(p_op_json) LOOP
        v_op_count := v_op_count + 1;
        v_op_norm  := lower(v_op_key);

        IF v_op_norm IN ('$eq', '$ne', '$gt', '$gte', '$lt', '$lte') THEN
            IF jsonb_typeof(v_op_val) = 'null' THEN
                v_parts := v_parts || (CASE v_op_norm
                    WHEN '$eq' THEN format('%s IS NULL', v_col)
                    WHEN '$ne' THEN format('%s IS NOT NULL', v_col)
                    ELSE format('%s %s NULL', v_col,
                        CASE v_op_norm WHEN '$gt' THEN '>' WHEN '$gte' THEN '>='
                                       WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END)
                END);
            ELSIF v_is_array AND NOT v_length_mod AND v_op_norm IN ('$eq', '$ne') THEN
                -- Array LHS: scalar = ANY / NOT (= ANY) semantics, e.g.
                -- Roles[].Any(r => r.Value == "X") -> 'X' = ANY(<text[]>)
                v_parts := v_parts || (CASE v_op_norm
                    WHEN '$eq'
                        THEN format('(%L%s = ANY(%s))',
                                pvt_jsonb_to_sql_literal(v_op_val), v_cast, v_col)
                    WHEN '$ne'
                        THEN format('(%s IS NULL OR NOT (%L%s = ANY(%s)))',
                                v_col, pvt_jsonb_to_sql_literal(v_op_val), v_cast, v_col)
                END);
            ELSE
                v_parts := v_parts || format('%s %s %L%s',
                    v_col,
                    CASE v_op_norm WHEN '$eq' THEN '=' WHEN '$ne' THEN '<>'
                                   WHEN '$gt' THEN '>' WHEN '$gte' THEN '>='
                                   WHEN '$lt' THEN '<' WHEN '$lte' THEN '<=' END,
                    pvt_jsonb_to_sql_literal(v_op_val),
                    v_cast);
            END IF;

        ELSIF v_op_norm IN ('$in', '$nin') THEN
            IF jsonb_typeof(v_op_val) <> 'array' THEN
                RAISE EXCEPTION 'pvt_build_field_condition: % expects array (field "%")', v_op_key, p_field_name;
            END IF;
            IF v_is_array AND NOT v_length_mod THEN
                -- Array LHS: use overlap (&&) instead of IN, since
                -- `text[] IN (text,...)` is not a valid SQL expression.
                IF v_op_norm = '$in' THEN
                    v_parts := v_parts || format(
                        '%s && (SELECT array_agg(x%s) FROM jsonb_array_elements_text(%L::jsonb) AS x)',
                        v_col, v_cast, v_op_val::text);
                ELSE
                    v_parts := v_parts || format(
                        '(%s IS NULL OR NOT (%s && (SELECT array_agg(x%s) FROM jsonb_array_elements_text(%L::jsonb) AS x)))',
                        v_col, v_col, v_cast, v_op_val::text);
                END IF;
            ELSE
                v_parts := v_parts || format('%s %s (SELECT (jsonb_array_elements_text(%L::jsonb))%s)',
                    v_col,
                    CASE v_op_norm WHEN '$in' THEN 'IN' ELSE 'NOT IN' END,
                    v_op_val::text,
                    v_cast);
            END IF;

        ELSIF v_op_norm IN ('$like', '$ilike') THEN
            v_parts := v_parts || format('%s %s %L',
                v_col,
                CASE v_op_norm WHEN '$like' THEN 'LIKE' ELSE 'ILIKE' END,
                v_op_val #>> '{}');

        ELSIF v_op_norm = '$startswith' THEN
            v_parts := v_parts || format('%s LIKE %L', v_col, (v_op_val #>> '{}') || '%');
        ELSIF v_op_norm = '$endswith' THEN
            v_parts := v_parts || format('%s LIKE %L', v_col, '%' || (v_op_val #>> '{}'));
        ELSIF v_op_norm = '$contains' THEN
            v_parts := v_parts || format('%s LIKE %L', v_col, '%' || (v_op_val #>> '{}') || '%');

        ELSIF v_op_norm = '$startswithignorecase' THEN
            v_parts := v_parts || format('%s ILIKE %L', v_col, (v_op_val #>> '{}') || '%');
        ELSIF v_op_norm = '$endswithignorecase' THEN
            v_parts := v_parts || format('%s ILIKE %L', v_col, '%' || (v_op_val #>> '{}'));
        ELSIF v_op_norm = '$containsignorecase' THEN
            v_parts := v_parts || format('%s ILIKE %L', v_col, '%' || (v_op_val #>> '{}') || '%');

        ELSIF v_op_norm IN ('$null', '$isnull') THEN
            v_parts := v_parts || (CASE
                WHEN (v_op_val::text)::boolean
                    THEN format('%s IS NULL', v_col)
                    ELSE format('%s IS NOT NULL', v_col)
                END);
        ELSIF v_op_norm IN ('$notnull', '$exists') THEN
            v_parts := v_parts || (CASE
                WHEN (v_op_val::text)::boolean
                    THEN format('%s IS NOT NULL', v_col)
                    ELSE format('%s IS NULL', v_col)
                END);

        -- ============ POSIX regex shorthand =========================
        --   {"Field": {"$regex": "pattern"}}      -> Field ~  'pattern'
        --   {"Field": {"$iregex": "pattern"}}     -> Field ~* 'pattern'
        --   {"Field": {"$notregex": "pattern"}}   -> Field !~  'pattern'
        --   {"Field": {"$inotregex": "pattern"}}  -> Field !~* 'pattern'
        ELSIF v_op_norm IN ('$regex', '$iregex', '$notregex', '$inotregex') THEN
            v_parts := v_parts || format('%s %s %L',
                v_col,
                CASE v_op_norm
                    WHEN '$regex'     THEN '~'
                    WHEN '$iregex'    THEN '~*'
                    WHEN '$notregex'  THEN '!~'
                    WHEN '$inotregex' THEN '!~*'
                END,
                v_op_val #>> '{}');

        -- ============ Full-text-search shorthand ====================
        --   {"Field": {"$fts": "query"}}                -- lang='simple'
        --   {"Field": {"$fts": {"query":"...",
        --                       "language":"english"}}} -- explicit lang
        ELSIF v_op_norm = '$fts' THEN
            DECLARE
                v_fts_lang  text := 'simple';
                v_fts_query text;
            BEGIN
                IF jsonb_typeof(v_op_val) = 'string' THEN
                    v_fts_query := v_op_val #>> '{}';
                ELSIF jsonb_typeof(v_op_val) = 'object' AND v_op_val ? 'query' THEN
                    v_fts_query := v_op_val ->> 'query';
                    IF v_op_val ? 'language' THEN
                        v_fts_lang := lower(v_op_val ->> 'language');
                    END IF;
                ELSE
                    RAISE EXCEPTION 'pvt_build_field_condition: $fts expects string or {"query":..., "language"?}';
                END IF;
                v_parts := v_parts || format(
                    '(to_tsvector(%L, COALESCE(%s::text, '''')) @@ websearch_to_tsquery(%L, %L))',
                    v_fts_lang, v_col, v_fts_lang, v_fts_query);
            END;

        -- ============ Array operators (Pro parity, v0.3.0) ===========
        -- All $arrayXxx require an array-shaped CTE column (typed array
        -- or ListItem-array projection: bigint[] for .Id / text[] for
        -- .Value / .Alias). The length/count modifier is incompatible
        -- because it collapses the array to a scalar.
        ELSIF v_op_norm LIKE '$array%' THEN
            IF v_length_mod THEN
                RAISE EXCEPTION
                    'pvt_build_field_condition: % cannot be combined with .$length / .$count (field "%")',
                    v_op_key, p_field_name;
            END IF;
            IF NOT v_is_array THEN
                RAISE EXCEPTION
                    'pvt_build_field_condition: % requires an array field (field "%")',
                    v_op_key, p_field_name;
            END IF;

            IF v_op_norm = '$arraycontains' THEN
                v_parts := v_parts || format('%L%s = ANY(%s)',
                    pvt_jsonb_to_sql_literal(v_op_val), v_elem_cast, v_col);
            ELSIF v_op_norm = '$arrayany' THEN
                -- Boolean-shorthand: true => not empty, false => empty.
                v_parts := v_parts || (CASE
                    WHEN (v_op_val::text)::boolean
                        THEN format('COALESCE(array_length(%s, 1), 0) > 0', v_col)
                        ELSE format('COALESCE(array_length(%s, 1), 0) = 0', v_col)
                    END);
            ELSIF v_op_norm = '$arrayempty' THEN
                v_parts := v_parts || (CASE
                    WHEN (v_op_val::text)::boolean
                        THEN format('COALESCE(array_length(%s, 1), 0) = 0', v_col)
                        ELSE format('COALESCE(array_length(%s, 1), 0) > 0', v_col)
                    END);
            ELSIF v_op_norm IN ('$arraycount', '$arraycountgt', '$arraycountgte',
                                '$arraycountlt', '$arraycountlte') THEN
                v_parts := v_parts || format('COALESCE(array_length(%s, 1), 0) %s %L::bigint',
                    v_col,
                    CASE v_op_norm
                        WHEN '$arraycount'    THEN '='
                        WHEN '$arraycountgt'  THEN '>'
                        WHEN '$arraycountgte' THEN '>='
                        WHEN '$arraycountlt'  THEN '<'
                        WHEN '$arraycountlte' THEN '<='
                    END,
                    pvt_jsonb_to_sql_literal(v_op_val));
            ELSIF v_op_norm = '$arrayat' THEN
                -- Operand: {"index": N, "value": V} (PG arrays are 1-based,
                -- but our convention is 0-based at the API; add 1).
                IF jsonb_typeof(v_op_val) <> 'object'
                   OR NOT (v_op_val ? 'index') OR NOT (v_op_val ? 'value') THEN
                    RAISE EXCEPTION 'pvt_build_field_condition: $arrayAt expects {"index":N,"value":V} (field "%")', p_field_name;
                END IF;
                v_parts := v_parts || format('%s[(%L::int) + 1] = %L%s',
                    v_col,
                    (v_op_val->>'index'),
                    pvt_jsonb_to_sql_literal(v_op_val->'value'),
                    v_elem_cast);
            ELSIF v_op_norm = '$arrayfirst' THEN
                v_parts := v_parts || format('%s[1] = %L%s',
                    v_col, pvt_jsonb_to_sql_literal(v_op_val), v_elem_cast);
            ELSIF v_op_norm = '$arraylast' THEN
                v_parts := v_parts || format('%s[array_length(%s, 1)] = %L%s',
                    v_col, v_col, pvt_jsonb_to_sql_literal(v_op_val), v_elem_cast);
            ELSIF v_op_norm = '$arraystartswith' THEN
                v_parts := v_parts || format('%s[1] LIKE %L', v_col, (v_op_val #>> '{}') || '%');
            ELSIF v_op_norm = '$arrayendswith' THEN
                v_parts := v_parts || format('%s[array_length(%s, 1)] LIKE %L',
                    v_col, v_col, '%' || (v_op_val #>> '{}'));
            ELSIF v_op_norm = '$arraymatches' THEN
                v_parts := v_parts || format(
                    'EXISTS (SELECT 1 FROM unnest(%s) AS _x WHERE _x LIKE %L)',
                    v_col, v_op_val #>> '{}');
            ELSIF v_op_norm IN ('$arraysum', '$arrayavg', '$arraymin', '$arraymax') THEN
                v_parts := v_parts || format(
                    '(SELECT %s(_x) FROM unnest(%s) AS _x) = %L%s',
                    CASE v_op_norm
                        WHEN '$arraysum' THEN 'SUM'
                        WHEN '$arrayavg' THEN 'AVG'
                        WHEN '$arraymin' THEN 'MIN'
                        WHEN '$arraymax' THEN 'MAX'
                    END,
                    v_col,
                    pvt_jsonb_to_sql_literal(v_op_val),
                    v_elem_cast);
            ELSE
                RAISE EXCEPTION
                    'pvt_build_field_condition: array operator "%" is not supported in v0.3.0 (field "%")',
                    v_op_key, p_field_name;
            END IF;

        ELSE
            RAISE EXCEPTION
                'pvt_build_field_condition: operator "%" is not supported in v0.3.0 (field "%")',
                v_op_key, p_field_name;
        END IF;
    END LOOP;

    IF v_op_count = 0 THEN
        RAISE EXCEPTION 'pvt_build_field_condition: empty operator object for field "%"', p_field_name;
    END IF;
    IF v_op_count = 1 THEN
        RETURN v_parts[1];
    END IF;
    RETURN '(' || array_to_string(v_parts, ' AND ') || ')';
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_field_condition(text, jsonb, jsonb, text) IS
    'Builds an AND-joined predicate fragment for one leaf field. v0.3.0 supports $eq/$ne/$gt/$gte/$lt/$lte/$in/$nin/$like/$ilike/$startsWith/$endsWith/$contains/$startsWithIgnoreCase/$endsWithIgnoreCase/$containsIgnoreCase/$null/$exists/$isNull/$notNull plus array ops $arrayContains/$arrayAny/$arrayEmpty/$arrayCount/$arrayCountGt|Gte|Lt|Lte/$arrayAt/$arrayFirst/$arrayLast/$arrayStartsWith|EndsWith|Matches/$arraySum|Avg|Min|Max and .$length/.$count modifiers. References CTE alias columns, not raw _values.';


-- ---------- pvt_jsonb_to_sql_literal -----------------------------------
-- Convert a JSON scalar into a string suitable for the %L format spec
-- (which adds the surrounding single quotes). Objects/arrays are not
-- valid here and cause an error.
CREATE OR REPLACE FUNCTION pvt_jsonb_to_sql_literal(p_val jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
BEGIN
    IF p_val IS NULL OR jsonb_typeof(p_val) = 'null' THEN
        RETURN NULL;
    END IF;
    IF jsonb_typeof(p_val) IN ('object', 'array') THEN
        RAISE EXCEPTION 'pvt_jsonb_to_sql_literal: cannot coerce % to scalar literal', jsonb_typeof(p_val);
    END IF;
    -- jsonb_typeof in ('string','number','boolean'): #>> '{}' strips
    -- the JSON quoting for strings and gives a plain text rendering
    -- for numbers/booleans that PostgreSQL accepts in casts.
    RETURN p_val #>> '{}';
END;
$BODY$;

COMMENT ON FUNCTION pvt_jsonb_to_sql_literal(jsonb) IS
    'Unwraps a JSON scalar to the text form expected by format() %L. Throws on objects/arrays.';


-- ===== 14_pvt_where.sql =====
-- =====================================================================
-- 14_pvt_where.sql
-- ---------------------------------------------------------------------
-- Recursive walker: turns the full filter JSON into a single WHERE
-- expression. Column references default to bare names (matching the
-- legacy wide _pvt_cte shape), but when p_base_prefix='o.' is passed
-- the walker rewrites base-field references to `o.<system_col>` so the
-- predicate composes against `FROM _objects o` directly (used by the
-- narrow Pro-shape outer SELECT that JOINs _objects on the pivot CTE).
--
-- Function:
--   pvt_build_where_from_json(p_filter jsonb, p_fields jsonb,
--                             p_base_prefix text DEFAULT '') RETURNS text
--
-- Grammar:
--   { "$and": [ ... ] }              ->  ( ... AND ... )
--   { "$or":  [ ... ] }              ->  ( ... OR  ... )
--   { "$not": { ... }   }            ->  NOT ( ... )
--   { "<field>": <op_json>, ... }    ->  AND of per-field conditions
--
-- An empty filter (NULL or {}) returns the SQL literal 'TRUE'.
-- =====================================================================

-- Signature evolved in v0.5.0 (added p_base_prefix for narrow Pro-shape
-- outer SELECT). CREATE OR REPLACE cannot change argument lists.
DROP FUNCTION IF EXISTS pvt_build_where_from_json(jsonb, jsonb);

CREATE OR REPLACE FUNCTION pvt_build_where_from_json(
    p_filter      jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k        text;
    v        jsonb;
    elem     jsonb;
    parts    text[] := ARRAY[]::text[];
    children text[] := ARRAY[]::text[];
    meta     jsonb;
    v_normalized text;
    v_peek   text;
BEGIN
    IF p_filter IS NULL OR p_filter = '{}'::jsonb THEN
        RETURN 'TRUE';
    END IF;
    IF jsonb_typeof(p_filter) <> 'object' THEN
        RAISE EXCEPTION 'pvt_build_where_from_json: filter must be a JSON object (got %)', jsonb_typeof(p_filter);
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP

        -- ---------- logical $and ----------
        IF lower(k) = '$and' THEN
            IF jsonb_typeof(v) <> 'array' THEN
                RAISE EXCEPTION '$and expects an array';
            END IF;
            children := ARRAY[]::text[];
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                children := children || pvt_build_where_from_json(elem, p_fields, p_base_prefix);
            END LOOP;
            IF array_length(children, 1) IS NULL THEN
                parts := parts || 'TRUE';
            ELSE
                parts := parts || ('(' || array_to_string(children, ' AND ') || ')');
            END IF;

        -- ---------- logical $or ----------
        ELSIF lower(k) = '$or' THEN
            IF jsonb_typeof(v) <> 'array' THEN
                RAISE EXCEPTION '$or expects an array';
            END IF;
            children := ARRAY[]::text[];
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                children := children || pvt_build_where_from_json(elem, p_fields, p_base_prefix);
            END LOOP;
            IF array_length(children, 1) IS NULL THEN
                parts := parts || 'FALSE';
            ELSE
                parts := parts || ('(' || array_to_string(children, ' OR ') || ')');
            END IF;

        -- ---------- logical $not ----------
        ELSIF lower(k) = '$not' THEN
            parts := parts || ('NOT (' || pvt_build_where_from_json(v, p_fields, p_base_prefix) || ')');

        -- ---------- hierarchical (tree) operators ----------
        ELSIF lower(k) IN ('$hasancestor', '$hasdescendant', '$level', '$isroot', '$isleaf') THEN
            -- Delegate to forked legacy helper. It operates on _objects
            -- columns which are all present inside the PVT CTE under
            -- their original names, so the predicates compose cleanly.
            parts := parts || pvt_build_hierarchical_conditions(
                jsonb_build_object(k, v),
                'o'  -- legacy expects an alias prefix; CTE column names match _objects column names
            );

        -- ---------- expression-form predicate ----------
        --   { "$gt": [exprL, exprR] }, { "$between": [L, lo, hi] }, etc.
        -- Disambiguated from `{ "$gt": value }` shorthand by the fact
        -- that the shorthand never appears at filter-level: it always
        -- lives inside `{ "<field>": { ... } }`.
        ELSIF lower(k) IN (
            '$eq', '$ne', '$lt', '$lte', '$gt', '$gte',
            '$like', '$ilike',
            '$in', '$nin', '$between',
            '$null', '$notnull', '$isnull', '$exists',
            '$contains', '$startswith', '$endswith',
            '$containsignorecase', '$startswithignorecase', '$endswithignorecase',
            '$regex', '$iregex', '$notregex', '$inotregex',
            '$fts'
        ) THEN
            parts := parts || pvt_build_expr_predicate(k, v, p_fields, p_base_prefix);

        -- ---------- $expr: arbitrary boolean expression --------------
        --   { "$expr": <bool-expr-node> } — full pvt_build_bool_expr,
        --   including $and/$or/$not, and any predicate compiled by
        --   pvt_build_expr_predicate. Useful for cross-field arithmetic
        --   filters and for $regex / $fts on multiple fields.
        ELSIF lower(k) = '$expr' THEN
            parts := parts || pvt_build_bool_expr(v, p_fields, p_base_prefix);

        -- ---------- field leaf ----------
        ELSE
            -- Pro-style ContainsKey normalization: rewrite `Dict.ContainsKey`
            -- to `Dict[<key>]` and look up the resolved metadata under the
            -- rewritten name. When the rewrite fired, the predicate
            -- collapses to `<col> IS NOT NULL` on the dict pivot column.
            v_peek       := pvt_peek_contains_key_value(v);
            v_normalized := pvt_normalize_field_name(k, v_peek);
            meta := p_fields -> v_normalized;
            IF meta IS NULL THEN
                RAISE EXCEPTION
                    'pvt_build_where_from_json: field "%" (normalized "%") has no metadata. Did pvt_collect_fields miss it?',
                    k, v_normalized;
            END IF;
            IF v_normalized <> k
               AND COALESCE((meta->>'was_contains_key')::boolean, false) THEN
                parts := parts || format('%I IS NOT NULL', v_normalized);
            ELSE
                parts := parts || pvt_build_field_condition(v_normalized, meta, v, p_base_prefix);
            END IF;
        END IF;
    END LOOP;

    IF array_length(parts, 1) IS NULL THEN
        RETURN 'TRUE';
    END IF;
    IF array_length(parts, 1) = 1 THEN
        RETURN parts[1];
    END IF;
    RETURN '(' || array_to_string(parts, ' AND ') || ')';
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_where_from_json(jsonb, jsonb, text) IS
    'Recursive filter walker. Emits a SQL WHERE expression. With default p_base_prefix='''' base columns are referenced bare (matches the legacy wide CTE shape); with p_base_prefix=''o.'' base references are emitted as `o.<system_col>` so the predicate composes against `FROM _objects o` in the narrow Pro-shape outer SELECT.';


-- ===== 15_pvt_order.sql =====
-- =====================================================================
-- 15_pvt_order.sql
-- ---------------------------------------------------------------------
-- Build an ORDER BY clause for the outer SELECT, referencing CTE
-- columns. Returns the empty string when no ordering is requested.
--
-- Function:
--   pvt_build_order_conditions(p_order jsonb, p_fields jsonb,
--                              p_base_prefix text DEFAULT '') RETURNS text
--
-- Input shape (accepted variants):
--   [
--     { "field": "FullName", "dir": "asc"  },
--     { "field": "Age",      "dir": "desc", "nulls": "last" }
--   ]
--   [
--     { "field": "FullName",      "direction": "ASC"  },     -- legacy C# emits this
--     { "field_path": "Age",      "direction": "DESC" }      -- legacy SQL emits this
--   ]
--   [
--     { "$expr": {"$mul":[{"$field":"Age"},{"$const":2}]},  -- arithmetic
--       "dir": "desc" },
--     { "$expr": {"$upper":{"$field":"FirstName"}},          -- function
--       "dir": "asc", "nulls": "last" }
--   ]
--
-- ORDER BY always runs on the OUTER SELECT (over the materialized CTE
-- row + JOIN _objects o in narrow Pro shape). Expression compilation
-- is delegated to pvt_build_scalar_expr from 17_pvt_expr.sql so the
-- same $field/$const/$add/.../$upper/.../$cast grammar that WHERE
-- accepts works in ORDER BY too. Base fields are emitted as
-- p_base_prefix || quote_ident(<system_col>), pivot fields as
-- quote_ident(<name>) (= the CTE column).
-- =====================================================================

-- Signature evolved in v0.6.0 (expression-form $expr support) and
-- v0.6.1 (p_distinct_on prepend for SELECT DISTINCT ON parity).
-- CREATE OR REPLACE cannot change argument lists, so drop legacy forms.
DROP FUNCTION IF EXISTS pvt_build_order_conditions(jsonb, jsonb);
DROP FUNCTION IF EXISTS pvt_build_order_conditions(jsonb, jsonb, text);
DROP FUNCTION IF EXISTS pvt_build_order_conditions(jsonb, jsonb, text, jsonb);
DROP FUNCTION IF EXISTS _pvt_compile_order_col(jsonb, jsonb, text);


-- ---------- internal helper: compile one entry column expression ------
-- Shared by ORDER BY, DISTINCT ON, GROUP BY and window PARTITION BY
-- builders. Returns the column-side SQL fragment (no direction, no
-- NULLS, no DISTINCT ON wrapping).
--
-- p_listitem_as_id (v0.4.x): controls how ListItem.Value / .Alias are
-- emitted. Pivot columns store raw `_ListItem` ids (parity with Pro);
-- ORDER BY / DISTINCT ON need the dereferenced string so that sorting
-- has a meaningful order, while GROUP BY / SELECT projection keep the
-- id directly. Pass `true` from GROUP BY-style builders.
CREATE OR REPLACE FUNCTION _pvt_compile_order_col(
    p_elem            jsonb,
    p_fields          jsonb,
    p_base_prefix     text,
    p_listitem_as_id  boolean DEFAULT false
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_field   text;
    v_meta    jsonb;
    v_li_prop text;
    v_is_arr  boolean;
    v_col     text;
BEGIN
    IF p_elem IS NULL OR jsonb_typeof(p_elem) <> 'object' THEN
        RAISE EXCEPTION '_pvt_compile_order_col: each entry must be a JSON object (got %)', jsonb_typeof(p_elem);
    END IF;

    IF p_elem ? '$expr' THEN
        RETURN pvt_build_scalar_expr(p_elem->'$expr', p_fields, p_base_prefix);
    END IF;

    v_field := COALESCE(p_elem->>'field', p_elem->>'field_path');
    IF v_field IS NULL OR v_field = '' THEN
        RAISE EXCEPTION '_pvt_compile_order_col: entry must include "field", "field_path" or "$expr"';
    END IF;

    v_meta := p_fields -> v_field;
    IF v_meta IS NULL THEN
        RAISE EXCEPTION
            '_pvt_compile_order_col: field "%" has no metadata. Did pvt_collect_fields miss it?', v_field;
    END IF;

    IF v_meta->>'kind' = 'base' THEN
        RETURN p_base_prefix || quote_ident(v_meta->>'column');
    END IF;

    v_col     := quote_ident(v_field);
    v_li_prop := v_meta->>'list_item_prop';
    v_is_arr  := COALESCE((v_meta->>'is_array')::boolean, false);

    -- ListItem.Value / .Alias scalar: pivot column already holds the
    -- dereferenced _list_items._value / _alias string (see
    -- pvt_build_column_expr), so ORDER BY / GROUP BY just reference
    -- the column directly. No extra _list_items lookup needed.
    RETURN v_col;
END;
$BODY$;


-- ---------- pvt_build_distinct_on_select ------------------------------
-- Render a "DISTINCT ON (expr1, expr2) " SELECT-list prefix from a JSON
-- array of {field|$expr} entries. Returns the empty string when
-- p_distinct_on is NULL/empty. Used by the orchestrator inside the
-- three outer-SELECT shapes (Shape A, narrow, wide).
CREATE OR REPLACE FUNCTION pvt_build_distinct_on_select(
    p_distinct_on jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    elem  jsonb;
    parts text[] := ARRAY[]::text[];
BEGIN
    IF p_distinct_on IS NULL
       OR jsonb_typeof(p_distinct_on) <> 'array'
       OR jsonb_array_length(p_distinct_on) = 0 THEN
        RETURN '';
    END IF;
    FOR elem IN SELECT value FROM jsonb_array_elements(p_distinct_on) LOOP
        parts := parts || _pvt_compile_order_col(elem, p_fields, p_base_prefix);
    END LOOP;
    RETURN 'DISTINCT ON (' || array_to_string(parts, ', ') || ') ';
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_distinct_on_select(jsonb, jsonb, text) IS
    'Renders a "DISTINCT ON (expr1, expr2) " SELECT-list prefix from a JSON array of {field|$expr} entries. PostgreSQL requires these expressions to also appear at the head of ORDER BY -- the caller pairs this with pvt_build_order_conditions(..., p_distinct_on) which auto-prepends them.';


CREATE OR REPLACE FUNCTION pvt_build_order_conditions(
    p_order       jsonb,
    p_fields      jsonb,
    p_base_prefix text  DEFAULT '',
    p_distinct_on jsonb DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    elem  jsonb;
    dir   text;
    nulls text;
    parts text[] := ARRAY[]::text[];
    v_col text;
BEGIN
    -- ---------- DISTINCT ON prefix --------------------------------
    -- PostgreSQL requires every DISTINCT ON expression to appear at
    -- the head of ORDER BY (with matching direction). We always emit
    -- them ASC -- callers needing the opposite must include the same
    -- expression explicitly in p_order with their desired direction
    -- before any other ordering keys, in which case PG will use that
    -- direction and we MUST NOT double-add (deduplicate by textual
    -- equality of the compiled fragment).
    IF p_distinct_on IS NOT NULL
       AND jsonb_typeof(p_distinct_on) = 'array'
       AND jsonb_array_length(p_distinct_on) > 0 THEN
        FOR elem IN SELECT value FROM jsonb_array_elements(p_distinct_on) LOOP
            v_col := _pvt_compile_order_col(elem, p_fields, p_base_prefix);
            parts := parts || (v_col || ' ASC');
        END LOOP;
    END IF;

    IF p_order IS NULL OR jsonb_typeof(p_order) <> 'array'
       OR jsonb_array_length(p_order) = 0 THEN
        IF array_length(parts, 1) IS NULL THEN
            RETURN '';
        END IF;
        RETURN E'\nORDER BY ' || array_to_string(parts, ', ');
    END IF;

    FOR elem IN SELECT value FROM jsonb_array_elements(p_order) LOOP
        dir   := lower(COALESCE(elem->>'dir', elem->>'direction', 'asc'));
        nulls := lower(COALESCE(elem->>'nulls', ''));

        IF dir NOT IN ('asc', 'desc') THEN
            RAISE EXCEPTION 'pvt_build_order_conditions: invalid direction "%" (asc|desc only)', dir;
        END IF;

        v_col := _pvt_compile_order_col(elem, p_fields, p_base_prefix);

        parts := parts || (
            v_col
            || ' ' || upper(dir)
            || CASE
                WHEN nulls = 'first' THEN ' NULLS FIRST'
                WHEN nulls = 'last'  THEN ' NULLS LAST'
                ELSE ''
               END
        );
    END LOOP;

    RETURN E'\nORDER BY ' || array_to_string(parts, ', ');
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_order_conditions(jsonb, jsonb, text, jsonb) IS
    'Builds an ORDER BY clause for the outer SELECT. Accepts plain {"field"|"field_path","dir","nulls"} entries and Pro-parity {"$expr":<scalar-expr>,"dir","nulls"} entries; expressions are compiled via pvt_build_scalar_expr. Base fields use p_base_prefix (default '''', or ''o.'' for the narrow Pro-shape outer that JOINs _objects). When p_distinct_on is provided its expressions are auto-prepended ASC (PostgreSQL DISTINCT ON requirement). Returns empty string when both p_order and p_distinct_on are empty.';


-- ===== 16_pvt_split.sql =====
-- =====================================================================
-- 16_pvt_split.sql
-- ---------------------------------------------------------------------
-- Pro-parity "base pushdown" optimizer. Walks the filter JSON and
-- splits it into two parts:
--
--   * pushdown_sql       -- a SQL predicate over `_objects o.*` that is
--                           safe to AND with `o._id_scheme = X` inside
--                           the pivot CTE; lets PG use indexes on
--                           system columns BEFORE the JOIN with _values
--                           and the GROUP BY agg.
--   * residual_filter    -- the JSON subtree that still needs to be
--                           evaluated against pivot/props columns AFTER
--                           the CTE materializes; fed to the regular
--                           pvt_build_where_from_json walker.
--
-- Function:
--   pvt_split_filter(
--       p_filter jsonb,
--       p_fields jsonb,
--       OUT v_pushdown_sql    text,
--       OUT v_residual_filter jsonb
--   )
--
-- Splitting rules (conservative — never changes semantics):
--   leaf {f: ops}              base/hierarchical -> push,
--                              else                -> residual.
--   {$and: [c1, c2, ...]}      split each child; AND the push parts;
--                              residual = $and of non-null residuals
--                              (degraded to single child or NULL).
--   {$or:  [c1, c2, ...]}      pushable ONLY when every child is fully
--                              base (residual_i is NULL for all i).
--                              Mixed-leaf $or stays entirely in residual.
--   {$not: c}                  pushable ONLY when c is fully base.
--                              Otherwise stays entirely in residual.
--   top-level multi-key object treated as implicit $and of singletons.
--
-- Hierarchical operators ($hasAncestor / $hasDescendant / $level /
-- $isRoot / $isLeaf) are always pushable because the legacy helper
-- already emits SQL over the `o` alias of _objects.
-- =====================================================================

CREATE OR REPLACE FUNCTION pvt_split_filter(
    p_filter             jsonb,
    p_fields             jsonb,
    OUT v_pushdown_sql    text,
    OUT v_residual_filter jsonb
)
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k                 text;
    v                 jsonb;
    elem              jsonb;
    child_push        text;
    child_residual    jsonb;
    push_parts        text[] := ARRAY[]::text[];
    residual_children jsonb  := '[]'::jsonb;
    is_logical        boolean := false;
    is_field_object   boolean := false;
    key_count         integer;
    v_peek            text;
    v_normalized      text;
    v_meta            jsonb;
    v_kind            text;
    v_op_key          text;
    v_singleton       jsonb;
BEGIN
    v_pushdown_sql    := NULL;
    v_residual_filter := NULL;

    -- Empty filter: nothing to push, nothing to filter.
    IF p_filter IS NULL OR p_filter = '{}'::jsonb THEN
        RETURN;
    END IF;

    IF jsonb_typeof(p_filter) <> 'object' THEN
        RAISE EXCEPTION 'pvt_split_filter: filter must be a JSON object (got %)', jsonb_typeof(p_filter);
    END IF;

    SELECT count(*) INTO key_count FROM jsonb_object_keys(p_filter);

    -- Detect logical operator at this node.
    SELECT key INTO v_op_key
      FROM jsonb_object_keys(p_filter) AS k(key)
     WHERE lower(k.key) IN ('$and', '$or', '$not');

    -- A node is "logical" only when it is a singleton {$and|$or|$not: ...}.
    -- Mixing {$and: [...], "Age": 18} is legal in the outer parser
    -- (treated as implicit AND), so re-route those through the multi-key
    -- path below.
    IF v_op_key IS NOT NULL AND key_count = 1 THEN
        is_logical := true;
    END IF;

    -- ============================================================
    -- 1) Logical singleton: $and / $or / $not
    -- ============================================================
    IF is_logical THEN
        v := p_filter -> v_op_key;

        IF lower(v_op_key) = '$and' THEN
            IF jsonb_typeof(v) <> 'array' THEN
                RAISE EXCEPTION 'pvt_split_filter: $and expects an array';
            END IF;
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                SELECT s.v_pushdown_sql, s.v_residual_filter
                  INTO child_push, child_residual
                  FROM pvt_split_filter(elem, p_fields) s;
                IF child_push IS NOT NULL THEN
                    push_parts := push_parts || child_push;
                END IF;
                IF child_residual IS NOT NULL THEN
                    residual_children := residual_children || jsonb_build_array(child_residual);
                END IF;
            END LOOP;

            IF array_length(push_parts, 1) IS NOT NULL THEN
                IF array_length(push_parts, 1) = 1 THEN
                    v_pushdown_sql := push_parts[1];
                ELSE
                    v_pushdown_sql := '(' || array_to_string(push_parts, ' AND ') || ')';
                END IF;
            END IF;

            IF jsonb_array_length(residual_children) > 0 THEN
                IF jsonb_array_length(residual_children) = 1 THEN
                    v_residual_filter := residual_children -> 0;
                ELSE
                    v_residual_filter := jsonb_build_object('$and', residual_children);
                END IF;
            END IF;
            RETURN;
        END IF;

        IF lower(v_op_key) = '$or' THEN
            IF jsonb_typeof(v) <> 'array' THEN
                RAISE EXCEPTION 'pvt_split_filter: $or expects an array';
            END IF;
            -- Pushable only if EVERY child is fully base (residual=NULL).
            -- Otherwise the whole $or must stay in residual unchanged,
            -- else we would tighten the predicate inside the CTE.
            DECLARE
                all_pushable boolean := true;
                or_pushes    text[]  := ARRAY[]::text[];
            BEGIN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    SELECT s.v_pushdown_sql, s.v_residual_filter
                      INTO child_push, child_residual
                      FROM pvt_split_filter(elem, p_fields) s;
                    IF child_residual IS NOT NULL OR child_push IS NULL THEN
                        all_pushable := false;
                        EXIT;
                    END IF;
                    or_pushes := or_pushes || child_push;
                END LOOP;

                IF all_pushable AND array_length(or_pushes, 1) IS NOT NULL THEN
                    v_pushdown_sql := '(' || array_to_string(or_pushes, ' OR ') || ')';
                    v_residual_filter := NULL;
                ELSE
                    v_pushdown_sql := NULL;
                    v_residual_filter := p_filter;
                END IF;
            END;
            RETURN;
        END IF;

        IF lower(v_op_key) = '$not' THEN
            SELECT s.v_pushdown_sql, s.v_residual_filter
              INTO child_push, child_residual
              FROM pvt_split_filter(v, p_fields) s;
            IF child_residual IS NULL AND child_push IS NOT NULL THEN
                v_pushdown_sql    := 'NOT (' || child_push || ')';
                v_residual_filter := NULL;
            ELSE
                v_pushdown_sql    := NULL;
                v_residual_filter := p_filter;
            END IF;
            RETURN;
        END IF;
    END IF;

    -- ============================================================
    -- 2) Multi-key object: implicit $and. Split each {k: v} singleton
    --    and reassemble.
    -- ============================================================
    IF key_count > 1 THEN
        FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
            v_singleton := jsonb_build_object(k, v);
            SELECT s.v_pushdown_sql, s.v_residual_filter
              INTO child_push, child_residual
              FROM pvt_split_filter(v_singleton, p_fields) s;
            IF child_push IS NOT NULL THEN
                push_parts := push_parts || child_push;
            END IF;
            IF child_residual IS NOT NULL THEN
                residual_children := residual_children || jsonb_build_array(child_residual);
            END IF;
        END LOOP;

        IF array_length(push_parts, 1) IS NOT NULL THEN
            IF array_length(push_parts, 1) = 1 THEN
                v_pushdown_sql := push_parts[1];
            ELSE
                v_pushdown_sql := '(' || array_to_string(push_parts, ' AND ') || ')';
            END IF;
        END IF;

        IF jsonb_array_length(residual_children) > 0 THEN
            IF jsonb_array_length(residual_children) = 1 THEN
                v_residual_filter := residual_children -> 0;
            ELSE
                v_residual_filter := jsonb_build_object('$and', residual_children);
            END IF;
        END IF;
        RETURN;
    END IF;

    -- ============================================================
    -- 3) Single-key leaf: either a hierarchical op or a field leaf.
    -- ============================================================
    SELECT key, value INTO k, v FROM jsonb_each(p_filter);

    -- Hierarchical operators -> always pushable. Reuse legacy helper.
    -- The legacy helper returns conditions with a leading ` AND ` so
    -- they can be concatenated to a pre-existing WHERE; we strip it
    -- here so the caller controls AND-joining.
    IF lower(k) IN ('$hasancestor', '$hasdescendant', '$level', '$isroot', '$isleaf', '$childrenof') THEN
        v_pushdown_sql    := pvt_build_hierarchical_conditions(
            jsonb_build_object(k, v),
            'o');
        IF v_pushdown_sql IS NOT NULL THEN
            v_pushdown_sql := regexp_replace(v_pushdown_sql, '^\s*AND\s+', '');
            v_pushdown_sql := trim(v_pushdown_sql);
            IF v_pushdown_sql = '' THEN
                v_pushdown_sql := NULL;
            END IF;
        END IF;
        v_residual_filter := NULL;
        RETURN;
    END IF;

    -- Expression-form predicate ($eq/$ne/$lt/.../$between/$in/...).
    -- Pushable iff every $field referenced inside resolves to a base
    -- column; otherwise the whole node stays in residual unchanged.
    IF lower(k) IN (
        '$eq', '$ne', '$lt', '$lte', '$gt', '$gte',
        '$like', '$ilike',
        '$in', '$nin', '$between',
        '$null', '$notnull', '$isnull', '$exists',
        '$contains', '$startswith', '$endswith',
        '$containsignorecase', '$startswithignorecase', '$endswithignorecase'
    ) THEN
        IF pvt_expr_is_base_only(v, p_fields) THEN
            v_pushdown_sql    := pvt_build_expr_predicate(k, v, p_fields, 'o.');
            v_residual_filter := NULL;
        ELSE
            v_pushdown_sql    := NULL;
            v_residual_filter := p_filter;
        END IF;
        RETURN;
    END IF;

    -- Top-level $expr: arbitrary boolean expression. Pushable iff every
    -- $field reference inside resolves to a base column; otherwise the
    -- whole node stays in residual unchanged.
    IF lower(k) = '$expr' THEN
        IF pvt_expr_is_base_only(v, p_fields) THEN
            v_pushdown_sql    := pvt_build_bool_expr(v, p_fields, 'o.');
            v_residual_filter := NULL;
        ELSE
            v_pushdown_sql    := NULL;
            v_residual_filter := p_filter;
        END IF;
        RETURN;
    END IF;

    -- Field leaf -> look up metadata; push only if kind = 'base'.
    v_peek       := pvt_peek_contains_key_value(v);
    v_normalized := pvt_normalize_field_name(k, v_peek);
    v_meta       := p_fields -> v_normalized;
    IF v_meta IS NULL THEN
        -- Unknown field: leave to the outer walker so its RAISE fires
        -- with the existing, descriptive error message.
        v_pushdown_sql    := NULL;
        v_residual_filter := p_filter;
        RETURN;
    END IF;

    v_kind := v_meta->>'kind';
    IF v_kind = 'base' THEN
        -- ContainsKey rewrites are dict-pivot only, never base.
        v_pushdown_sql    := pvt_build_field_condition(
            v_normalized, v_meta, v, 'o.');
        v_residual_filter := NULL;
    ELSE
        v_pushdown_sql    := NULL;
        v_residual_filter := p_filter;
    END IF;
END;
$BODY$;

COMMENT ON FUNCTION pvt_split_filter(jsonb, jsonb) IS
    'Splits a filter tree into (pushdown_sql over _objects o, residual_filter for outer CTE WHERE). Conservative: $or/$not are pushed only when every leaf inside is a base/hierarchical predicate; mixed branches stay in residual unchanged so semantics never change.';

-- =====================================================================
-- pvt_filter_has_base_refs: scan a residual filter and report whether it
-- references any base/hierarchical column. Used to gate WHERE pushdown
-- into _pvt_cte (Pro-parity perf): when false, the residual is safe to
-- evaluate inside the CTE wrapper that exposes only pivoted columns.
-- =====================================================================
CREATE OR REPLACE FUNCTION pvt_filter_has_base_refs(p_filter jsonb, p_fields jsonb)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text; v jsonb; elem jsonb;
    v_norm text; v_meta jsonb; v_peek text;
BEGIN
    IF p_filter IS NULL OR p_filter = '{}'::jsonb THEN
        RETURN false;
    END IF;
    IF jsonb_typeof(p_filter) <> 'object' THEN
        RETURN true;  -- conservative
    END IF;
    FOR k, v IN SELECT key, value FROM jsonb_each(p_filter) LOOP
        IF lower(k) IN ('$hasancestor','$hasdescendant','$level','$isroot','$isleaf') THEN
            RETURN true;
        END IF;
        IF lower(k) IN ('$and','$or') THEN
            IF jsonb_typeof(v) = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                    IF pvt_filter_has_base_refs(elem, p_fields) THEN RETURN true; END IF;
                END LOOP;
            END IF;
            CONTINUE;
        END IF;
        IF lower(k) = '$not' THEN
            IF pvt_filter_has_base_refs(v, p_fields) THEN RETURN true; END IF;
            CONTINUE;
        END IF;
        IF left(k, 1) = '$' THEN
            -- $expr / expression-form ops: conservative -> true
            RETURN true;
        END IF;
        -- field leaf
        v_peek := pvt_peek_contains_key_value(v);
        v_norm := pvt_normalize_field_name(k, v_peek);
        v_meta := p_fields -> v_norm;
        IF v_meta IS NULL THEN
            RETURN true;  -- conservative
        END IF;
        IF (v_meta->>'kind') = 'base' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END;
$BODY$;

COMMENT ON FUNCTION pvt_filter_has_base_refs(jsonb, jsonb) IS
    'Returns true when the residual filter references any base column or hierarchical operator. Used to gate WHERE pushdown into the _pvt_cte wrapper (which projects only pivoted columns).';


-- ===== 17_pvt_expr.sql =====
-- =====================================================================
-- 17_pvt_expr.sql
-- ---------------------------------------------------------------------
-- Expression engine for the free PVT pipeline. Implements arithmetic,
-- string and Math.* style functions, parenthesized nesting and full
-- expression-form predicates --- matching Pro's `WhereRedb` /
-- `IRedbTreeQueryable` capability (Pro emits the same SQL by walking
-- the LINQ AST via ExpressionToSqlCompiler; here we accept a JSON AST
-- shaped 1:1 with that compiler's output).
--
-- ---------------------------------------------------------------------
-- Scalar expression grammar (returns a SQL fragment):
--
--   { "$field":  "<path>" }                    -> "PathColumn"
--                                                or 'o."_id"' under pushdown
--   { "$const":  <json scalar> }               -> quoted literal
--   { "$add":    [a, b, ...] }                 -> (a + b + ...)
--   { "$sub":    [a, b] }                      -> (a - b)
--   { "$mul":    [a, b, ...] }                 -> (a * b * ...)
--   { "$div":    [a, b] }                      -> (a / b)
--   { "$mod":    [a, b] }                      -> (a % b)
--   { "$neg":    a }                           -> (-a)
--   { "$abs":    a }                           -> ABS(a)
--   { "$round":  [a, digits] }                 -> ROUND(a, digits)
--   { "$floor":  a }                           -> FLOOR(a)
--   { "$ceil":   a }                           -> CEIL(a)
--   { "$min":    [a, b, ...] }                 -> LEAST(a, b, ...)
--   { "$max":    [a, b, ...] }                 -> GREATEST(a, b, ...)
--   { "$upper":  a }                           -> UPPER(a)
--   { "$lower":  a }                           -> LOWER(a)
--   { "$trim":   a }                           -> TRIM(a)
--   { "$length": a }                           -> LENGTH(a) (str) or
--                                                COALESCE(array_length(a,1),0)
--   { "$concat": [a, b, ...] }                 -> (a || b || ...)
--   { "$coalesce":[a, b, ...] }                -> COALESCE(a, b, ...)
--   { "$cast":   ["<sqltype>", a] }            -> (a)::<sqltype>
--
-- Expression-form predicate grammar (returns a SQL boolean fragment):
--
--   { "$eq" | "$ne" | "$lt" | "$lte" | "$gt" | "$gte":
--                [exprL, exprR] }              -> (L op R)
--   { "$like"  | "$ilike":
--                [exprL, exprR] }              -> (L LIKE / ILIKE R)
--   { "$contains" | "$startsWith" | "$endsWith":
--                [exprL, "literal"] }          -> sugar over LIKE
--   { "$containsIgnoreCase" | "$startsWithIgnoreCase" | "$endsWithIgnoreCase":
--                [exprL, "literal"] }          -> sugar over ILIKE
--   { "$in" | "$nin":
--                [exprL, [v1, v2, ...]] }      -> L [NOT] IN (...)
--   { "$between":
--                [exprL, low, high] }          -> L BETWEEN low AND high
--   { "$null"  | "$notNull"  | "$isNull":
--                expr | true }                 -> expr IS [NOT] NULL
--
-- Push-down classification: pvt_expr_is_base_only(node, fields) returns
-- true iff every `$field` reference inside resolves to kind='base'. SQL
-- functions and arithmetic are pushable by construction (they map 1:1
-- to PG operators).
-- =====================================================================


-- ---------- pvt_expr_field_names --------------------------------------
-- Yield every `$field` reference inside a scalar expression node.
-- Used by pvt_collect_fields so metadata is resolved for them.
CREATE OR REPLACE FUNCTION pvt_expr_field_names(p_node jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k    text;
    v    jsonb;
    elem jsonb;
BEGIN
    IF p_node IS NULL THEN
        RETURN;
    END IF;

    -- Array node (predicate args or n-ary operand list): walk each element.
    IF jsonb_typeof(p_node) = 'array' THEN
        FOR elem IN SELECT value FROM jsonb_array_elements(p_node) LOOP
            RETURN QUERY SELECT pvt_expr_field_names(elem);
        END LOOP;
        RETURN;
    END IF;

    -- Scalar literal: nothing to harvest.
    IF jsonb_typeof(p_node) <> 'object' THEN
        RETURN;
    END IF;

    IF p_node ? '$field' THEN
        RETURN NEXT p_node->>'$field';
        RETURN;
    END IF;
    IF p_node ? '$const' THEN
        RETURN;
    END IF;

    -- Generic recursion over every operand.
    FOR k, v IN SELECT key, value FROM jsonb_each(p_node) LOOP
        IF left(k, 1) <> '$' THEN
            CONTINUE;
        END IF;
        -- $case has named branches {when,then,else}: descend into values only.
        IF lower(k) = '$case' AND jsonb_typeof(v) = 'array' THEN
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                IF jsonb_typeof(elem) = 'object' THEN
                    IF elem ? 'when' THEN
                        RETURN QUERY SELECT pvt_expr_field_names(elem->'when');
                    END IF;
                    IF elem ? 'then' THEN
                        RETURN QUERY SELECT pvt_expr_field_names(elem->'then');
                    END IF;
                    IF elem ? 'else' THEN
                        RETURN QUERY SELECT pvt_expr_field_names(elem->'else');
                    END IF;
                END IF;
            END LOOP;
            CONTINUE;
        END IF;
        -- $fts object form has `query` (expression) + `fields` array;
        -- `language` is a literal config name and must not be walked.
        IF lower(k) = '$fts' AND jsonb_typeof(v) = 'object' THEN
            IF v ? 'query'  THEN RETURN QUERY SELECT pvt_expr_field_names(v->'query'); END IF;
            IF v ? 'fields' AND jsonb_typeof(v->'fields') = 'array' THEN
                FOR elem IN SELECT value FROM jsonb_array_elements(v->'fields') LOOP
                    RETURN QUERY SELECT pvt_expr_field_names(elem);
                END LOOP;
            END IF;
            CONTINUE;
        END IF;
        -- $cast: ["<sql-type>", expr] -- type literal must not be walked.
        IF lower(k) = '$cast' AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 2 THEN
            RETURN QUERY SELECT pvt_expr_field_names(v->1);
            CONTINUE;
        END IF;
        -- Date helpers whose first array slot is a unit string literal.
        IF lower(k) IN ('$dateadd', '$datesub', '$datediff', '$datetrunc')
           AND jsonb_typeof(v) = 'array' AND jsonb_array_length(v) >= 1 THEN
            FOR elem IN SELECT value FROM jsonb_array_elements(v) WITH ORDINALITY AS t(value, ord)
                          WHERE ord >= 2 LOOP
                RETURN QUERY SELECT pvt_expr_field_names(elem);
            END LOOP;
            CONTINUE;
        END IF;
        IF jsonb_typeof(v) = 'array' THEN
            FOR elem IN SELECT value FROM jsonb_array_elements(v) LOOP
                RETURN QUERY SELECT pvt_expr_field_names(elem);
            END LOOP;
        ELSIF jsonb_typeof(v) = 'object' THEN
            RETURN QUERY SELECT pvt_expr_field_names(v);
        END IF;
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_expr_field_names(jsonb) IS
    'Yields every "$field" path referenced inside a scalar expression node (recursive). Empty for $const / non-object nodes.';


-- ---------- pvt_expr_is_base_only -------------------------------------
-- Recursive check: every `$field` inside the node maps to a metadata
-- entry with kind='base'. Used by the pushdown splitter to decide
-- whether an expression predicate can move inside the pivot CTE.
CREATE OR REPLACE FUNCTION pvt_expr_is_base_only(
    p_node   jsonb,
    p_fields jsonb
)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_name text;
    v_meta jsonb;
BEGIN
    IF p_node IS NULL THEN
        RETURN true;
    END IF;
    FOR v_name IN SELECT pvt_expr_field_names(p_node) LOOP
        v_meta := p_fields -> v_name;
        IF v_meta IS NULL OR v_meta->>'kind' <> 'base' THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END;
$BODY$;

COMMENT ON FUNCTION pvt_expr_is_base_only(jsonb, jsonb) IS
    'True iff every $field inside the expression resolves (per pvt_collect_fields) to kind=base. Pushable predicates run inside the pivot CTE on _objects o.*.';


-- ---------- pvt_format_const ------------------------------------------
-- Convert a JSON scalar to a SQL literal. Strings are single-quoted
-- with `''` escaping; booleans/null/numbers map natively. No casts:
-- callers wrap with `$cast` when an explicit type is needed.
CREATE OR REPLACE FUNCTION pvt_format_const(p_value jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
DECLARE
    v_t text;
BEGIN
    IF p_value IS NULL OR jsonb_typeof(p_value) = 'null' THEN
        RETURN 'NULL';
    END IF;
    v_t := jsonb_typeof(p_value);
    IF v_t = 'string'  THEN RETURN quote_literal(p_value #>> '{}'); END IF;
    IF v_t = 'number'  THEN RETURN p_value::text;                    END IF;
    IF v_t = 'boolean' THEN RETURN p_value::text;                    END IF;
    -- Arrays and objects are not scalar literals.
    RAISE EXCEPTION
        'pvt_format_const: unsupported JSON type % for scalar literal (use $const for scalars only)', v_t;
END;
$BODY$;


-- ---------- pvt_build_scalar_expr -------------------------------------
-- Recursive scalar-expression compiler. Always wraps binary/unary
-- results in parentheses so operator precedence is preserved through
-- nesting.
CREATE OR REPLACE FUNCTION pvt_build_scalar_expr(
    p_node        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_name      text;
    v_meta      jsonb;
    v_kind      text;
    v_args      jsonb;
    v_parts     text[];
    v_elem      jsonb;
    v_op_key    text;
    v_n         integer;
    v_a         text;
    v_b         text;
    v_type      text;
BEGIN
    IF p_node IS NULL OR jsonb_typeof(p_node) = 'null' THEN
        RETURN 'NULL';
    END IF;

    -- Bare JSON scalar / array literal: tolerate it as shorthand for
    -- $const (saves callers from wrapping every literal).
    IF jsonb_typeof(p_node) <> 'object' THEN
        IF jsonb_typeof(p_node) = 'array' THEN
            RAISE EXCEPTION
                'pvt_build_scalar_expr: bare JSON array is not a scalar expression (wrap each element or use a list-aware operator like $in)';
        END IF;
        RETURN pvt_format_const(p_node);
    END IF;

    -- ---------------- $field ---------------------------------------
    IF p_node ? '$field' THEN
        v_name := p_node->>'$field';
        v_meta := p_fields -> v_name;
        IF v_meta IS NULL THEN
            RAISE EXCEPTION
                'pvt_build_scalar_expr: $field "%" has no metadata (did pvt_collect_fields miss it?)', v_name;
        END IF;
        v_kind := v_meta->>'kind';
        IF v_kind = 'base' THEN
            RETURN p_base_prefix || quote_ident(v_meta->>'column');
        END IF;
        RETURN quote_ident(v_name);
    END IF;

    -- ---------------- $const ---------------------------------------
    IF p_node ? '$const' THEN
        RETURN pvt_format_const(p_node->'$const');
    END IF;

    -- Dispatch on the single $-key.
    SELECT key INTO v_op_key FROM jsonb_object_keys(p_node) AS t(key) WHERE left(key,1) = '$' LIMIT 1;
    IF v_op_key IS NULL THEN
        RAISE EXCEPTION 'pvt_build_scalar_expr: no operator key in node: %', p_node::text;
    END IF;
    v_args := p_node -> v_op_key;

    -- ---------------- Aggregate passthrough ------------------------
    -- Aggregate expressions ($count/$sum/$avg/$min*/$max*/$string_agg/
    -- $bool_and/$bool_or) appearing inside a scalar context delegate to
    -- pvt_build_agg_expr. This makes HAVING / ORDER BY / window args
    -- agg-aware without a separate compiler stack. PostgreSQL itself
    -- rejects aggregates in WHERE / GROUP BY so this is not a footgun.
    -- $min and $max overlap with n-ary LEAST/GREATEST helpers; the
    -- aggregate form is recognised only when the operand is a single
    -- non-array scalar-expression node (or "*"). LEAST/GREATEST callers
    -- always pass an array of >=1 operands.
    IF lower(v_op_key) IN ('$count', '$sum', '$avg', '$string_agg', '$bool_and', '$bool_or')
       OR (lower(v_op_key) IN ('$min', '$max')
           AND jsonb_typeof(v_args) <> 'array') THEN
        RETURN pvt_build_agg_expr(p_node, p_fields, p_base_prefix);
    END IF;

    -- ---------------- Window passthrough ---------------------------
    -- {"$over": <window-node>} delegates to pvt_build_window_expr.
    -- PostgreSQL itself rejects window calls outside SELECT / ORDER BY.
    IF lower(v_op_key) = '$over' THEN
        RETURN pvt_build_window_expr(v_args, p_fields, p_base_prefix);
    END IF;

    -- N-ary helpers --------------------------------------------------
    IF lower(v_op_key) IN ('$add', '$sub', '$mul', '$div', '$mod', '$concat', '$min', '$max', '$coalesce') THEN
        IF jsonb_typeof(v_args) <> 'array' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects a JSON array of operands', v_op_key;
        END IF;
        v_n := jsonb_array_length(v_args);
        IF v_n < 1 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects at least 1 operand', v_op_key;
        END IF;
        IF lower(v_op_key) IN ('$sub', '$div', '$mod') AND v_n <> 2 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects exactly 2 operands (got %)', v_op_key, v_n;
        END IF;
        v_parts := ARRAY[]::text[];
        FOR v_elem IN SELECT value FROM jsonb_array_elements(v_args) LOOP
            v_parts := v_parts || pvt_build_scalar_expr(v_elem, p_fields, p_base_prefix);
        END LOOP;
        IF lower(v_op_key) = '$add'      THEN RETURN '(' || array_to_string(v_parts, ' + ')  || ')'; END IF;
        IF lower(v_op_key) = '$sub'      THEN RETURN '(' || v_parts[1] || ' - ' || v_parts[2] || ')'; END IF;
        IF lower(v_op_key) = '$mul'      THEN RETURN '(' || array_to_string(v_parts, ' * ')  || ')'; END IF;
        IF lower(v_op_key) = '$div'      THEN RETURN '(' || v_parts[1] || ' / ' || v_parts[2] || ')'; END IF;
        IF lower(v_op_key) = '$mod'      THEN RETURN '(' || v_parts[1] || ' % ' || v_parts[2] || ')'; END IF;
        IF lower(v_op_key) = '$concat'   THEN RETURN '(' || array_to_string(v_parts, ' || ') || ')'; END IF;
        IF lower(v_op_key) = '$min'      THEN RETURN 'LEAST('    || array_to_string(v_parts, ', ') || ')'; END IF;
        IF lower(v_op_key) = '$max'      THEN RETURN 'GREATEST(' || array_to_string(v_parts, ', ') || ')'; END IF;
        IF lower(v_op_key) = '$coalesce' THEN RETURN 'COALESCE(' || array_to_string(v_parts, ', ') || ')'; END IF;
    END IF;

    -- Unary -----------------------------------------------------------
    IF lower(v_op_key) IN ('$neg', '$abs', '$floor', '$ceil', '$upper', '$lower', '$trim', '$length') THEN
        -- Accept both { $op: expr } and { $op: [expr] } (array-wrapped
        -- form, which is how the C# compilers emit single-arg calls).
        IF jsonb_typeof(v_args) = 'array' THEN
            IF jsonb_array_length(v_args) <> 1 THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: % expects exactly 1 operand (got %)',
                    v_op_key, jsonb_array_length(v_args);
            END IF;
            v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        ELSE
            v_a := pvt_build_scalar_expr(v_args, p_fields, p_base_prefix);
        END IF;
        IF lower(v_op_key) = '$neg'    THEN RETURN '(-' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$abs'    THEN RETURN 'ABS('    || v_a || ')'; END IF;
        IF lower(v_op_key) = '$floor'  THEN RETURN 'FLOOR('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$ceil'   THEN RETURN 'CEIL('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$upper'  THEN RETURN 'UPPER('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$lower'  THEN RETURN 'LOWER('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$trim'   THEN RETURN 'TRIM('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$length' THEN
            -- Polymorphic: array → array_length, otherwise → text length.
            -- We can't always know the type statically, so prefer the
            -- string form; callers wanting array-length should use the
            -- `.$length` legacy modifier on a registered array field.
            RETURN 'LENGTH(' || v_a || ')';
        END IF;
    END IF;

    -- Round (2-arg) ---------------------------------------------------
    IF lower(v_op_key) = '$round' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) NOT IN (1, 2) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $round expects [value] or [value, digits]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        IF jsonb_array_length(v_args) = 1 THEN
            RETURN 'ROUND(' || v_a || ')';
        END IF;
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN 'ROUND(' || v_a || ', ' || v_b || ')';
    END IF;

    -- $cast -----------------------------------------------------------
    IF lower(v_op_key) = '$cast' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2
           OR jsonb_typeof(v_args->0) <> 'string' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $cast expects ["<sql-type>", expr]';
        END IF;
        v_type := v_args->>0;
        -- Whitelist SQL types to keep the surface area sane.
        IF v_type NOT IN (
            'text', 'varchar', 'bigint', 'integer', 'int', 'smallint',
            'numeric', 'double precision', 'real', 'boolean', 'uuid',
            'timestamptz', 'timestamp', 'date', 'bytea'
        ) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $cast unsupported type "%"', v_type;
        END IF;
        v_a := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN '(' || v_a || ')::' || v_type;
    END IF;

    -- ---------------- Extended unary (Pro-parity + free extras) -----
    -- Strings:  $trimStart -> LTRIM, $trimEnd -> RTRIM
    -- Math:     $sqrt, $sign, $exp, $ln (natural log)
    -- Dates:    $year/$month/$day/$hour/$minute/$second
    --           $dayOfWeek/$dayOfYear (free extras over Pro)
    IF lower(v_op_key) IN (
        '$trimstart', '$trimend',
        '$sqrt', '$sign', '$exp', '$ln',
        '$year', '$month', '$day', '$hour', '$minute', '$second',
        '$dayofweek', '$dayofyear'
    ) THEN
        IF jsonb_typeof(v_args) = 'array' THEN
            IF jsonb_array_length(v_args) <> 1 THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: % expects exactly 1 operand (got %)',
                    v_op_key, jsonb_array_length(v_args);
            END IF;
            v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        ELSE
            v_a := pvt_build_scalar_expr(v_args, p_fields, p_base_prefix);
        END IF;
        IF lower(v_op_key) = '$trimstart'  THEN RETURN 'LTRIM(' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$trimend'    THEN RETURN 'RTRIM(' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$sqrt'       THEN RETURN 'SQRT('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$sign'       THEN RETURN 'SIGN('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$exp'        THEN RETURN 'EXP('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$ln'         THEN RETURN 'LN('    || v_a || ')'; END IF;
        IF lower(v_op_key) = '$year'       THEN RETURN 'EXTRACT(YEAR FROM '   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$month'      THEN RETURN 'EXTRACT(MONTH FROM '  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$day'        THEN RETURN 'EXTRACT(DAY FROM '    || v_a || ')'; END IF;
        IF lower(v_op_key) = '$hour'       THEN RETURN 'EXTRACT(HOUR FROM '   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$minute'     THEN RETURN 'EXTRACT(MINUTE FROM ' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$second'     THEN RETURN 'EXTRACT(SECOND FROM ' || v_a || ')'; END IF;
        IF lower(v_op_key) = '$dayofweek'  THEN RETURN 'EXTRACT(DOW FROM '    || v_a || ')'; END IF;
        IF lower(v_op_key) = '$dayofyear'  THEN RETURN 'EXTRACT(DOY FROM '    || v_a || ')'; END IF;
    END IF;

    -- ---------------- $power (2-arg) -------------------------------
    IF lower(v_op_key) = '$power' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $power expects [base, exponent]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN 'POWER(' || v_a || ', ' || v_b || ')';
    END IF;

    -- ---------------- $log (2-arg: base, value) --------------------
    IF lower(v_op_key) = '$log' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $log expects [base, value]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN 'LOG(' || v_a || ', ' || v_b || ')';
    END IF;

    -- ---------------- $substring (2 or 3 args) ---------------------
    -- $substring: [str, start]  -> SUBSTRING(str FROM start)
    -- $substring: [str, start, length] -> SUBSTRING(str FROM start FOR length)
    -- Note: PostgreSQL SUBSTRING is 1-based; callers translating from
    -- C#'s 0-based String.Substring must add 1 in the AST.
    IF lower(v_op_key) = '$substring' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) NOT IN (2, 3) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $substring expects [str, start] or [str, start, length]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        IF jsonb_array_length(v_args) = 2 THEN
            RETURN 'SUBSTRING(' || v_a || ' FROM ' || v_b || ')';
        END IF;
        DECLARE
            v_c text := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
        BEGIN
            RETURN 'SUBSTRING(' || v_a || ' FROM ' || v_b || ' FOR ' || v_c || ')';
        END;
    END IF;

    -- ---------------- $replace (3-arg) -----------------------------
    IF lower(v_op_key) = '$replace' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 3 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $replace expects [str, find, replaceWith]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        DECLARE
            v_find    text := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_replace text := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
        BEGIN
            RETURN 'REPLACE(' || v_a || ', ' || v_find || ', ' || v_replace || ')';
        END;
    END IF;

    -- ---------------- $indexOf (2-arg) -----------------------------
    -- Returns 1-based position; 0 when not found (Postgres POSITION).
    -- For C#'s 0-based IndexOf, subtract 1 in the caller AST.
    IF lower(v_op_key) = '$indexof' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $indexOf expects [str, needle]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        RETURN 'POSITION(' || v_b || ' IN ' || v_a || ')';
    END IF;

    -- ---------------- $padLeft / $padRight (2 or 3-arg) ------------
    IF lower(v_op_key) IN ('$padleft', '$padright') THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) NOT IN (2, 3) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects [str, length] or [str, length, padChar]', v_op_key;
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        v_b := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
        DECLARE
            v_kw  text := CASE WHEN lower(v_op_key) = '$padleft' THEN 'LPAD' ELSE 'RPAD' END;
            -- PG LPAD/RPAD truncate when target length < input length, while
            -- C# PadLeft/PadRight are no-op in that case. Use GREATEST() so we
            -- never request a width smaller than the input string.
            v_len text := 'GREATEST(length(' || v_a || '), ' || v_b || ')';
        BEGIN
            IF jsonb_array_length(v_args) = 2 THEN
                RETURN v_kw || '(' || v_a || ', ' || v_len || ')';
            END IF;
            RETURN v_kw || '(' || v_a || ', ' || v_len || ', '
                       || pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix) || ')';
        END;
    END IF;

    -- ---------------- $dateAdd / $dateSub (3-arg) ------------------
    -- $dateAdd: ["day"|"month"|..., <date_expr>, <int_expr>]
    -- Emits:    <date_expr> + (<int_expr> * INTERVAL '1 day')
    IF lower(v_op_key) IN ('$dateadd', '$datesub') THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 3
           OR jsonb_typeof(v_args->0) <> 'string' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: % expects ["<unit>", date_expr, int_expr]', v_op_key;
        END IF;
        DECLARE
            v_unit text := lower(v_args->>0);
            v_op   text := CASE WHEN lower(v_op_key) = '$dateadd' THEN ' + ' ELSE ' - ' END;
        BEGIN
            IF v_unit NOT IN ('year','month','week','day','hour','minute','second','millisecond') THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: % unsupported unit "%"', v_op_key, v_unit;
            END IF;
            v_a := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_b := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
            RETURN '(' || v_a || v_op || '(' || v_b || ' * INTERVAL ''1 ' || v_unit || '''))';
        END;
    END IF;

    -- ---------------- $dateDiff (3-arg) ----------------------------
    -- $dateDiff: ["<unit>", a, b]  -> integer count of <unit> from b to a.
    -- Implemented via EXTRACT(EPOCH ...) for sub-day units and AGE for
    -- year/month so calendar arithmetic is preserved.
    IF lower(v_op_key) = '$datediff' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 3
           OR jsonb_typeof(v_args->0) <> 'string' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $dateDiff expects ["<unit>", a, b]';
        END IF;
        DECLARE
            v_unit text := lower(v_args->>0);
        BEGIN
            v_a := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_b := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
            IF v_unit IN ('second','millisecond','minute','hour','day','week') THEN
                DECLARE
                    v_divisor text := CASE v_unit
                        WHEN 'millisecond' THEN '0.001'
                        WHEN 'second'      THEN '1'
                        WHEN 'minute'      THEN '60'
                        WHEN 'hour'        THEN '3600'
                        WHEN 'day'         THEN '86400'
                        WHEN 'week'        THEN '604800'
                    END;
                BEGIN
                    RETURN '(EXTRACT(EPOCH FROM (' || v_a || ' - ' || v_b || ')) / ' || v_divisor || ')::bigint';
                END;
            ELSIF v_unit = 'month' THEN
                RETURN '((EXTRACT(YEAR FROM AGE(' || v_a || ', ' || v_b || ')) * 12'
                    || ' + EXTRACT(MONTH FROM AGE(' || v_a || ', ' || v_b || '))))';
            ELSIF v_unit = 'year' THEN
                RETURN 'EXTRACT(YEAR FROM AGE(' || v_a || ', ' || v_b || '))';
            END IF;
            RAISE EXCEPTION 'pvt_build_scalar_expr: $dateDiff unsupported unit "%"', v_unit;
        END;
    END IF;

    -- ---------------- $if (3-arg: cond, then, else) ----------------
    -- cond is a boolean expression node compiled via pvt_build_bool_expr.
    IF lower(v_op_key) = '$if' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 3 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $if expects [cond, then, else]';
        END IF;
        DECLARE
            v_cond text := pvt_build_bool_expr(v_args->0, p_fields, p_base_prefix);
            v_then text := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_else text := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
        BEGIN
            RETURN '(CASE WHEN ' || v_cond || ' THEN ' || v_then || ' ELSE ' || v_else || ' END)';
        END;
    END IF;

    -- ---------------- $case ----------------------------------------
    -- $case: [ {"when": <bool>, "then": <expr>}, ..., {"else": <expr>}? ]
    -- The trailing else entry is optional; when absent NULL is used.
    IF lower(v_op_key) = '$case' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) = 0 THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $case expects a non-empty array of {when,then} / {else} entries';
        END IF;
        DECLARE
            v_sb     text := '(CASE';
            v_entry  jsonb;
            v_else   text := 'NULL';
            v_seen_else boolean := false;
        BEGIN
            FOR v_entry IN SELECT value FROM jsonb_array_elements(v_args) LOOP
                IF v_entry ? 'else' THEN
                    IF v_seen_else THEN
                        RAISE EXCEPTION 'pvt_build_scalar_expr: $case has multiple else entries';
                    END IF;
                    v_else := pvt_build_scalar_expr(v_entry->'else', p_fields, p_base_prefix);
                    v_seen_else := true;
                ELSIF v_entry ? 'when' AND v_entry ? 'then' THEN
                    v_sb := v_sb
                        || ' WHEN ' || pvt_build_bool_expr(v_entry->'when', p_fields, p_base_prefix)
                        || ' THEN ' || pvt_build_scalar_expr(v_entry->'then', p_fields, p_base_prefix);
                ELSE
                    RAISE EXCEPTION 'pvt_build_scalar_expr: $case entry must be {when,then} or {else} (got %)', v_entry::text;
                END IF;
            END LOOP;
            RETURN v_sb || ' ELSE ' || v_else || ' END)';
        END;
    END IF;

    -- ---------------- Trigonometry / log10 (unary) -----------------
    IF lower(v_op_key) IN ('$sin', '$cos', '$tan', '$asin', '$acos', '$atan', '$log10') THEN
        IF jsonb_typeof(v_args) = 'array' THEN
            IF jsonb_array_length(v_args) <> 1 THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: % expects exactly 1 operand (got %)',
                    v_op_key, jsonb_array_length(v_args);
            END IF;
            v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        ELSE
            v_a := pvt_build_scalar_expr(v_args, p_fields, p_base_prefix);
        END IF;
        IF lower(v_op_key) = '$sin'   THEN RETURN 'SIN('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$cos'   THEN RETURN 'COS('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$tan'   THEN RETURN 'TAN('   || v_a || ')'; END IF;
        IF lower(v_op_key) = '$asin'  THEN RETURN 'ASIN('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$acos'  THEN RETURN 'ACOS('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$atan'  THEN RETURN 'ATAN('  || v_a || ')'; END IF;
        IF lower(v_op_key) = '$log10' THEN RETURN 'LOG('   || v_a || ')'; END IF;
    END IF;

    -- ---------------- $now / $today (zero-arg) ---------------------
    -- Shape: {"$now": null}, {"$now": []}, {"$today": []}.
    IF lower(v_op_key) IN ('$now', '$today', '$utcnow') THEN
        IF lower(v_op_key) = '$now'    THEN RETURN 'NOW()'; END IF;
        IF lower(v_op_key) = '$utcnow' THEN RETURN '(NOW() AT TIME ZONE ''UTC'')'; END IF;
        IF lower(v_op_key) = '$today'  THEN RETURN 'CURRENT_DATE'; END IF;
    END IF;

    -- ---------------- $dateTrunc(["<unit>", expr]) -----------------
    -- Emits DATE_TRUNC('<unit>', expr).
    IF lower(v_op_key) = '$datetrunc' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) <> 2
           OR jsonb_typeof(v_args->0) <> 'string' THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $dateTrunc expects ["<unit>", date_expr]';
        END IF;
        DECLARE
            v_unit text := lower(v_args->>0);
        BEGIN
            IF v_unit NOT IN (
                'microseconds','milliseconds','second','minute','hour',
                'day','week','month','quarter','year','decade','century','millennium'
            ) THEN
                RAISE EXCEPTION 'pvt_build_scalar_expr: $dateTrunc unsupported unit "%"', v_unit;
            END IF;
            v_a := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            RETURN 'DATE_TRUNC(' || quote_literal(v_unit) || ', ' || v_a || ')';
        END;
    END IF;

    -- ---------------- $regexReplace([str, pat, repl, flags?]) ------
    IF lower(v_op_key) = '$regexreplace' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) NOT IN (3, 4) THEN
            RAISE EXCEPTION 'pvt_build_scalar_expr: $regexReplace expects [str, pattern, replacement] or [str, pattern, replacement, flags]';
        END IF;
        v_a := pvt_build_scalar_expr(v_args->0, p_fields, p_base_prefix);
        DECLARE
            v_pat  text := pvt_build_scalar_expr(v_args->1, p_fields, p_base_prefix);
            v_repl text := pvt_build_scalar_expr(v_args->2, p_fields, p_base_prefix);
        BEGIN
            IF jsonb_array_length(v_args) = 3 THEN
                RETURN 'REGEXP_REPLACE(' || v_a || ', ' || v_pat || ', ' || v_repl || ')';
            END IF;
            RETURN 'REGEXP_REPLACE(' || v_a || ', ' || v_pat || ', ' || v_repl || ', '
                || pvt_build_scalar_expr(v_args->3, p_fields, p_base_prefix) || ')';
        END;
    END IF;

    RAISE EXCEPTION 'pvt_build_scalar_expr: unsupported operator "%"', v_op_key;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_scalar_expr(jsonb, jsonb, text) IS
    'Compiles a JSON scalar-expression AST into a SQL fragment. Supports field/const, full arithmetic (+ - * / %), math (abs/round/floor/ceil/min/max), string (upper/lower/trim/length/concat), coalesce and cast. Recursive: every binary node wraps in parens so precedence is preserved.';


-- ---------- pvt_build_expr_predicate ----------------------------------
-- Compiles an expression-form predicate (key = $op, value = arguments)
-- into a SQL boolean fragment.
CREATE OR REPLACE FUNCTION pvt_build_expr_predicate(
    p_op          text,
    p_args        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_op    text := lower(p_op);
    v_n     integer;
    v_l     text;
    v_r     text;
    v_lo    text;
    v_hi    text;
    v_parts text[];
    v_elem  jsonb;
    v_pat   text;
BEGIN
    -- Unary: $null / $notNull / $isNull / $exists
    IF v_op IN ('$null', '$notnull', '$isnull', '$exists') THEN
        -- Value may be the expression itself, or a boolean toggle
        -- ({"$null": true} is only legal in shorthand form, not here).
        v_l := pvt_build_scalar_expr(p_args, p_fields, p_base_prefix);
        IF v_op IN ('$null', '$isnull') THEN RETURN v_l || ' IS NULL';     END IF;
        IF v_op = '$notnull'             THEN RETURN v_l || ' IS NOT NULL'; END IF;
        IF v_op = '$exists'              THEN RETURN v_l || ' IS NOT NULL'; END IF;
    END IF;

    -- $between [L, lo, hi]
    IF v_op = '$between' THEN
        IF jsonb_typeof(p_args) <> 'array' OR jsonb_array_length(p_args) <> 3 THEN
            RAISE EXCEPTION 'pvt_build_expr_predicate: $between expects [expr, low, high]';
        END IF;
        v_l  := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
        v_lo := pvt_build_scalar_expr(p_args->1, p_fields, p_base_prefix);
        v_hi := pvt_build_scalar_expr(p_args->2, p_fields, p_base_prefix);
        RETURN '(' || v_l || ' BETWEEN ' || v_lo || ' AND ' || v_hi || ')';
    END IF;

    -- $in / $nin [L, [v1, v2, ...]]
    IF v_op IN ('$in', '$nin') THEN
        IF jsonb_typeof(p_args) <> 'array' OR jsonb_array_length(p_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_expr_predicate: % expects [expr, [v1, v2, ...]]', p_op;
        END IF;
        v_l := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
        IF jsonb_typeof(p_args->1) <> 'array' THEN
            RAISE EXCEPTION 'pvt_build_expr_predicate: % RHS must be a JSON array of literals', p_op;
        END IF;
        IF jsonb_array_length(p_args->1) = 0 THEN
            RETURN CASE WHEN v_op = '$in' THEN 'FALSE' ELSE 'TRUE' END;
        END IF;
        v_parts := ARRAY[]::text[];
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_args->1) LOOP
            v_parts := v_parts || pvt_format_const(v_elem);
        END LOOP;
        IF v_op = '$in'  THEN RETURN '(' || v_l || ' IN ('     || array_to_string(v_parts, ', ') || '))'; END IF;
        IF v_op = '$nin' THEN RETURN '(' || v_l || ' NOT IN (' || array_to_string(v_parts, ', ') || '))'; END IF;
    END IF;

    -- ---------------- $fts (full-text search) ----------------------
    -- Shape A: {"$fts": [<query-expr>, <fieldExpr1>, <fieldExpr2>, ...]}
    --          (lang defaults to 'simple')
    -- Shape B: {"$fts": {"query": <expr>, "fields": [<fieldExpr>, ...],
    --                    "language": "english"|"simple"|"russian"|...}}
    -- Emits: ((to_tsvector(L, F1) || to_tsvector(L, F2) || ...)
    --         @@ websearch_to_tsquery(L, Q))
    IF v_op = '$fts' THEN
        DECLARE
            v_lang   text := 'simple';
            v_query  text;
            v_fields jsonb;
            v_vec    text;
        BEGIN
            IF jsonb_typeof(p_args) = 'array' THEN
                IF jsonb_array_length(p_args) < 2 THEN
                    RAISE EXCEPTION 'pvt_build_expr_predicate: $fts array form expects [query, field, ...] (>=2 elements)';
                END IF;
                v_query  := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
                v_fields := jsonb_path_query_array(p_args, '$[1 to last]');
            ELSIF jsonb_typeof(p_args) = 'object' THEN
                IF NOT (p_args ? 'query') OR NOT (p_args ? 'fields') THEN
                    RAISE EXCEPTION 'pvt_build_expr_predicate: $fts object form expects {"query":..., "fields":[...], "language"?}';
                END IF;
                v_query := pvt_build_scalar_expr(p_args->'query', p_fields, p_base_prefix);
                IF jsonb_typeof(p_args->'fields') <> 'array' OR jsonb_array_length(p_args->'fields') = 0 THEN
                    RAISE EXCEPTION 'pvt_build_expr_predicate: $fts "fields" must be a non-empty array';
                END IF;
                v_fields := p_args->'fields';
                IF p_args ? 'language' THEN
                    IF jsonb_typeof(p_args->'language') <> 'string' THEN
                        RAISE EXCEPTION 'pvt_build_expr_predicate: $fts "language" must be a string';
                    END IF;
                    v_lang := lower(p_args->>'language');
                END IF;
            ELSE
                RAISE EXCEPTION 'pvt_build_expr_predicate: $fts expects array or object form';
            END IF;

            v_parts := ARRAY[]::text[];
            FOR v_elem IN SELECT value FROM jsonb_array_elements(v_fields) LOOP
                v_parts := v_parts ||
                    ('to_tsvector(' || quote_literal(v_lang) || ', COALESCE('
                        || pvt_build_scalar_expr(v_elem, p_fields, p_base_prefix)
                        || '::text, ''''))');
            END LOOP;
            IF array_length(v_parts, 1) = 1 THEN
                v_vec := v_parts[1];
            ELSE
                v_vec := '(' || array_to_string(v_parts, ' || ') || ')';
            END IF;
            RETURN '(' || v_vec || ' @@ websearch_to_tsquery('
                || quote_literal(v_lang) || ', ' || v_query || '))';
        END;
    END IF;

    -- Binary infix / LIKE family -------------------------------------
    IF jsonb_typeof(p_args) <> 'array' OR jsonb_array_length(p_args) <> 2 THEN
        RAISE EXCEPTION 'pvt_build_expr_predicate: % expects [exprL, exprR]', p_op;
    END IF;
    v_l := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
    v_r := pvt_build_scalar_expr(p_args->1, p_fields, p_base_prefix);

    IF v_op = '$eq'  THEN RETURN '(' || v_l || ' = '  || v_r || ')'; END IF;
    IF v_op = '$ne'  THEN RETURN '(' || v_l || ' <> ' || v_r || ')'; END IF;
    IF v_op = '$lt'  THEN RETURN '(' || v_l || ' < '  || v_r || ')'; END IF;
    IF v_op = '$lte' THEN RETURN '(' || v_l || ' <= ' || v_r || ')'; END IF;
    IF v_op = '$gt'  THEN RETURN '(' || v_l || ' > '  || v_r || ')'; END IF;
    IF v_op = '$gte' THEN RETURN '(' || v_l || ' >= ' || v_r || ')'; END IF;

    IF v_op IN ('$like', '$ilike') THEN
        RETURN '(' || v_l || CASE WHEN v_op = '$like' THEN ' LIKE ' ELSE ' ILIKE ' END || v_r || ')';
    END IF;

    -- Sugar over LIKE/ILIKE: RHS must be a string literal so we can
    -- splice the wildcards. Accept $const-wrapped or bare-string form.
    IF v_op IN ('$contains', '$startswith', '$endswith',
                '$containsignorecase', '$startswithignorecase', '$endswithignorecase') THEN
        IF jsonb_typeof(p_args->1) = 'object' AND (p_args->1) ? '$const' THEN
            v_pat := (p_args->1)->>'$const';
        ELSIF jsonb_typeof(p_args->1) = 'string' THEN
            v_pat := p_args->1 #>> '{}';
        ELSE
            RAISE EXCEPTION
                'pvt_build_expr_predicate: % RHS must be a string literal (got %)',
                p_op, jsonb_typeof(p_args->1);
        END IF;
        DECLARE
            v_lit text;
            v_kw  text;
        BEGIN
            v_kw  := CASE WHEN right(v_op, length('ignorecase')) = 'ignorecase' THEN ' ILIKE ' ELSE ' LIKE ' END;
            v_lit := CASE
                WHEN v_op IN ('$contains', '$containsignorecase')   THEN quote_literal('%' || v_pat || '%')
                WHEN v_op IN ('$startswith', '$startswithignorecase') THEN quote_literal(v_pat || '%')
                WHEN v_op IN ('$endswith', '$endswithignorecase')     THEN quote_literal('%' || v_pat)
            END;
            RETURN '(' || v_l || v_kw || v_lit || ')';
        END;
    END IF;

    -- ---------------- $regex / $iregex / $notregex / $inotregex ----
    -- Shape: {"$regex": [strExpr, patternStringOrConst]} (PG POSIX regex).
    IF v_op IN ('$regex', '$iregex', '$notregex', '$inotregex') THEN
        IF jsonb_typeof(p_args) <> 'array' OR jsonb_array_length(p_args) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_expr_predicate: % expects [expr, pattern]', p_op;
        END IF;
        v_l := pvt_build_scalar_expr(p_args->0, p_fields, p_base_prefix);
        v_r := pvt_build_scalar_expr(p_args->1, p_fields, p_base_prefix);
        DECLARE
            v_op_sql text := CASE v_op
                WHEN '$regex'     THEN ' ~ '
                WHEN '$iregex'    THEN ' ~* '
                WHEN '$notregex'  THEN ' !~ '
                WHEN '$inotregex' THEN ' !~* '
            END;
        BEGIN
            RETURN '(' || v_l || v_op_sql || v_r || ')';
        END;
    END IF;

    RAISE EXCEPTION 'pvt_build_expr_predicate: unsupported predicate "%"', p_op;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_expr_predicate(text, jsonb, jsonb, text) IS
    'Compiles an expression-form predicate (key = $eq/$ne/$lt/$gt/$like/$in/$between/...) into a SQL boolean fragment. LHS and RHS are full pvt_build_scalar_expr expressions; LIKE-family operators expect a string literal on RHS so wildcards can be spliced.';


-- ---------- pvt_build_bool_expr ---------------------------------------
-- Boolean-expression compiler used by $if / $case scalar nodes (and by
-- HAVING/window-FILTER builders). The node is either:
--   * { "$and": [<bool>, ...] } / { "$or": [<bool>, ...] }
--   * { "$not": <bool> }
--   * { "<predicate-op>": <args> }   (delegates to pvt_build_expr_predicate)
-- Bare booleans `true`/`false` map to SQL TRUE/FALSE literals.
CREATE OR REPLACE FUNCTION pvt_build_bool_expr(
    p_node        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_op    text;
    v_args  jsonb;
    v_elem  jsonb;
    v_parts text[];
BEGIN
    IF p_node IS NULL OR jsonb_typeof(p_node) = 'null' THEN
        RETURN 'TRUE';
    END IF;
    IF jsonb_typeof(p_node) = 'boolean' THEN
        RETURN CASE WHEN (p_node)::text = 'true' THEN 'TRUE' ELSE 'FALSE' END;
    END IF;
    IF jsonb_typeof(p_node) <> 'object' THEN
        RAISE EXCEPTION 'pvt_build_bool_expr: bool node must be object/boolean (got %)', jsonb_typeof(p_node);
    END IF;

    SELECT key INTO v_op
      FROM jsonb_object_keys(p_node) AS t(key)
     WHERE left(key, 1) = '$'
     LIMIT 1;
    IF v_op IS NULL THEN
        RAISE EXCEPTION 'pvt_build_bool_expr: bool node has no operator key: %', p_node::text;
    END IF;
    v_args := p_node -> v_op;

    IF lower(v_op) = '$and' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) = 0 THEN
            RAISE EXCEPTION '$and expects a non-empty array';
        END IF;
        v_parts := ARRAY[]::text[];
        FOR v_elem IN SELECT value FROM jsonb_array_elements(v_args) LOOP
            v_parts := v_parts || pvt_build_bool_expr(v_elem, p_fields, p_base_prefix);
        END LOOP;
        RETURN '(' || array_to_string(v_parts, ' AND ') || ')';
    END IF;
    IF lower(v_op) = '$or' THEN
        IF jsonb_typeof(v_args) <> 'array' OR jsonb_array_length(v_args) = 0 THEN
            RAISE EXCEPTION '$or expects a non-empty array';
        END IF;
        v_parts := ARRAY[]::text[];
        FOR v_elem IN SELECT value FROM jsonb_array_elements(v_args) LOOP
            v_parts := v_parts || pvt_build_bool_expr(v_elem, p_fields, p_base_prefix);
        END LOOP;
        RETURN '(' || array_to_string(v_parts, ' OR ') || ')';
    END IF;
    IF lower(v_op) = '$not' THEN
        RETURN 'NOT (' || pvt_build_bool_expr(v_args, p_fields, p_base_prefix) || ')';
    END IF;

    RETURN pvt_build_expr_predicate(v_op, v_args, p_fields, p_base_prefix);
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_bool_expr(jsonb, jsonb, text) IS
    'Compiles a boolean expression node (with $and/$or/$not support) into a SQL boolean fragment. Leaf operators delegate to pvt_build_expr_predicate. Used by $if/$case scalar forms and by HAVING/window-FILTER builders.';


-- ===== 19_pvt_agg_expr.sql =====
-- =====================================================================
-- 19_pvt_agg_expr.sql
-- ---------------------------------------------------------------------
-- Aggregate-expression compiler used by terminal aggregations (21),
-- GROUP BY projections / HAVING (22) and window aggregate flavours (23).
-- A single aggregate entry has the shape:
--
--   {
--     "alias":   "<column alias, optional>",
--     "$<func>": "*" | <scalar-expr-node>,
--     "distinct": true | false,   -- optional, default false
--     "filter":   <bool-expr-node> -- optional, emits FILTER (WHERE ...)
--   }
--
-- Where `<func>` is one of: count / sum / avg / min / max / string_agg /
-- bool_and / bool_or.
--
-- `string_agg` requires a 2-element scalar-expr array `[value, separator]`
-- because Postgres demands a separator argument.
--
-- Free extras over Pro (which only knows COUNT/SUM/AVG/MIN/MAX without
-- DISTINCT, without FILTER, and without string_agg):
--   * `distinct: true` -> COUNT(DISTINCT expr) etc.
--   * `filter:   <bool>` -> COUNT(...) FILTER (WHERE ...) (per-aggregate
--     conditional aggregation; HAVING-of-one-aggregate)
--   * string_agg / bool_and / bool_or
--
-- All entries return text fragments without the `AS "<alias>"` suffix;
-- callers append aliasing themselves so the same compiler is reusable
-- inside HAVING / OVER / nested expressions.
-- =====================================================================


-- ---------- pvt_agg_entry_field_names ---------------------------------
-- Yield every field path referenced inside an aggregate entry. Used by
-- pvt_collect_fields callers (orchestrators of 21/22/23 wrap each entry
-- as `{"$expr": <entry-as-bag-of-nodes>}` and rely on this helper to
-- enumerate field refs in $<func> operand + filter).
CREATE OR REPLACE FUNCTION pvt_agg_entry_field_names(p_entry jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    k text;
    v jsonb;
BEGIN
    IF p_entry IS NULL OR jsonb_typeof(p_entry) <> 'object' THEN
        RETURN;
    END IF;

    FOR k, v IN SELECT key, value FROM jsonb_each(p_entry) LOOP
        IF k IN ('alias', 'distinct') THEN
            CONTINUE;
        END IF;
        IF k = 'filter' THEN
            RETURN QUERY SELECT pvt_expr_field_names(v);
            CONTINUE;
        END IF;
        IF left(k, 1) <> '$' THEN
            CONTINUE;
        END IF;
        -- $<func> operand: bare "*" yields nothing; otherwise full expr walk.
        IF jsonb_typeof(v) = 'string' AND (v #>> '{}') = '*' THEN
            CONTINUE;
        END IF;
        RETURN QUERY SELECT pvt_expr_field_names(v);
    END LOOP;
END;
$BODY$;


-- ---------- pvt_build_agg_expr ----------------------------------------
-- Compile a single aggregate entry into a SQL fragment (without alias).
-- Validates exactly one $<func> key per entry and dispatches by name.
CREATE OR REPLACE FUNCTION pvt_build_agg_expr(
    p_entry       jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_op        text;
    v_arg       jsonb;
    v_distinct  boolean := false;
    v_filter    jsonb;
    v_inner     text;
    v_func      text;
    v_filt_sql  text := '';
    v_keys      int;
    v_args      jsonb;
    v_elem      jsonb;
    v_sep       text;
    v_fld_name  text;
    v_fld_meta  jsonb;
    v_is_array  boolean := false;
    v_col_expr  text;
BEGIN
    IF p_entry IS NULL OR jsonb_typeof(p_entry) <> 'object' THEN
        RAISE EXCEPTION 'pvt_build_agg_expr: entry must be a JSON object';
    END IF;

    -- Pick the single $-key inside the entry (alias/distinct/filter
    -- are sidecars, not operators).
    SELECT key INTO v_op
      FROM jsonb_object_keys(p_entry) AS t(key)
     WHERE left(key, 1) = '$'
     LIMIT 1;
    IF v_op IS NULL THEN
        RAISE EXCEPTION 'pvt_build_agg_expr: entry has no $<func> key: %', p_entry::text;
    END IF;

    -- Sanity: no more than one $-key per entry.
    SELECT count(*) INTO v_keys
      FROM jsonb_object_keys(p_entry) AS t(key)
     WHERE left(key, 1) = '$';
    IF v_keys > 1 THEN
        RAISE EXCEPTION 'pvt_build_agg_expr: entry has multiple $<func> keys (%): %',
            v_keys, p_entry::text;
    END IF;

    v_arg := p_entry -> v_op;
    IF p_entry ? 'distinct' AND jsonb_typeof(p_entry->'distinct') = 'boolean' THEN
        v_distinct := (p_entry->>'distinct')::boolean;
    END IF;
    IF p_entry ? 'filter' THEN
        v_filter := p_entry->'filter';
        v_filt_sql := ' FILTER (WHERE ' || pvt_build_bool_expr(v_filter, p_fields, p_base_prefix) || ')';
    END IF;

    -- ---------------- COUNT ----------------------------------------
    IF lower(v_op) = '$count' THEN
        -- "*" shorthand for COUNT(*) - DISTINCT meaningless here.
        IF jsonb_typeof(v_arg) = 'string' AND (v_arg #>> '{}') = '*' THEN
            IF v_distinct THEN
                RAISE EXCEPTION 'pvt_build_agg_expr: $count "*" with distinct=true is not allowed';
            END IF;
            RETURN 'COUNT(*)' || v_filt_sql;
        END IF;

        -- Detect array-typed $field operand (e.g. {"$count":{"$field":"SkillLevels[]"}})
        -- and route through per-row unnest so $count returns total element count
        -- across all rows (rows × array_length), matching Pro semantics.
        v_is_array := false;
        IF jsonb_typeof(v_arg) = 'object' AND v_arg ? '$field' THEN
            v_fld_name := v_arg->>'$field';
            v_fld_meta := p_fields -> v_fld_name;
            IF v_fld_meta IS NOT NULL
               AND COALESCE((v_fld_meta->>'is_array')::boolean, false)
               AND COALESCE(v_fld_meta->>'kind','') <> 'base' THEN
                v_is_array := true;
            END IF;
        END IF;

        IF v_is_array THEN
            IF v_distinct THEN
                RAISE EXCEPTION
                    'pvt_build_agg_expr: distinct=true is not supported for $count over array field "%"', v_fld_name;
            END IF;
            v_col_expr := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
            -- COUNT over unnested elements = SUM(per-row element count).
            -- COALESCE handles empty/NULL array (array_length returns NULL).
            RETURN 'SUM(COALESCE(array_length(' || v_col_expr || ', 1), 0))::bigint' || v_filt_sql;
        END IF;

        v_inner := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
        RETURN 'COUNT(' || CASE WHEN v_distinct THEN 'DISTINCT ' ELSE '' END || v_inner || ')' || v_filt_sql;
    END IF;

    -- ---------------- SUM / AVG / MIN / MAX ------------------------
    IF lower(v_op) IN ('$sum', '$avg', '$min', '$max') THEN
        v_func := CASE lower(v_op)
            WHEN '$sum' THEN 'SUM'
            WHEN '$avg' THEN 'AVG'
            WHEN '$min' THEN 'MIN'
            WHEN '$max' THEN 'MAX'
        END;

        -- Detect array-typed $field operand (e.g. {"$sum":{"$field":"SkillLevels[]"}})
        -- and route through per-row unnest so PG can aggregate element values
        -- instead of failing with "function sum(<type>[]) does not exist".
        v_is_array := false;
        IF jsonb_typeof(v_arg) = 'object' AND v_arg ? '$field' THEN
            v_fld_name := v_arg->>'$field';
            v_fld_meta := p_fields -> v_fld_name;
            IF v_fld_meta IS NOT NULL
               AND COALESCE((v_fld_meta->>'is_array')::boolean, false)
               AND COALESCE(v_fld_meta->>'kind','') <> 'base' THEN
                v_is_array := true;
            END IF;
        END IF;

        IF v_is_array THEN
            IF v_distinct THEN
                RAISE EXCEPTION
                    'pvt_build_agg_expr: distinct=true is not supported for aggregate over array field "%"', v_fld_name;
            END IF;
            v_col_expr := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
            IF lower(v_op) = '$avg' THEN
                -- AVG over all elements across all rows = SUM(elements) / COUNT(elements).
                -- Per-row sum and per-row count are computed in subqueries; outer SUMs
                -- combine them. NULLIF guards an all-empty-array result.
                RETURN '(SUM((SELECT SUM(_x) FROM unnest(' || v_col_expr || ') AS _x))::numeric'
                    || ' / NULLIF(SUM(COALESCE(array_length(' || v_col_expr || ', 1), 0)), 0))'
                    || v_filt_sql;
            ELSE
                -- SUM/MIN/MAX of aggregates is associative -> outer FUNC over per-row FUNC.
                RETURN v_func || '((SELECT ' || v_func || '(_x) FROM unnest('
                    || v_col_expr || ') AS _x))' || v_filt_sql;
            END IF;
        END IF;

        v_inner := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
        RETURN v_func || '(' || CASE WHEN v_distinct THEN 'DISTINCT ' ELSE '' END || v_inner || ')' || v_filt_sql;
    END IF;

    -- ---------------- STRING_AGG -----------------------------------
    -- { "$string_agg": [value, separator] }
    IF lower(v_op) = '$string_agg' THEN
        IF jsonb_typeof(v_arg) <> 'array' OR jsonb_array_length(v_arg) <> 2 THEN
            RAISE EXCEPTION 'pvt_build_agg_expr: $string_agg expects [value, separator]';
        END IF;
        v_inner := pvt_build_scalar_expr(v_arg->0, p_fields, p_base_prefix);
        v_sep   := pvt_build_scalar_expr(v_arg->1, p_fields, p_base_prefix);
        RETURN 'STRING_AGG(' || CASE WHEN v_distinct THEN 'DISTINCT ' ELSE '' END
            || v_inner || ', ' || v_sep || ')' || v_filt_sql;
    END IF;

    -- ---------------- BOOL_AND / BOOL_OR ---------------------------
    IF lower(v_op) IN ('$bool_and', '$bool_or') THEN
        v_func := CASE lower(v_op)
            WHEN '$bool_and' THEN 'BOOL_AND'
            WHEN '$bool_or'  THEN 'BOOL_OR'
        END;
        v_inner := pvt_build_scalar_expr(v_arg, p_fields, p_base_prefix);
        RETURN v_func || '(' || v_inner || ')' || v_filt_sql;
    END IF;

    RAISE EXCEPTION 'pvt_build_agg_expr: unsupported aggregate "%"', v_op;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_agg_expr(jsonb, jsonb, text) IS
    'Compiles a single aggregate entry into a SQL fragment. Supports $count|$sum|$avg|$min|$max|$string_agg|$bool_and|$bool_or with optional distinct=true and per-aggregate filter={...}. Free extras over Pro: DISTINCT modifier and FILTER clause.';


-- ---------- pvt_build_agg_projection ----------------------------------
-- Compile an array of aggregate entries into a SQL SELECT-list fragment
-- like `agg1 AS "alias1", agg2 AS "alias2", ...`. Aliases default to
-- positional `_agg_<i>` when omitted; duplicates raise.
CREATE OR REPLACE FUNCTION pvt_build_agg_projection(
    p_aggs        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_entry  jsonb;
    v_alias  text;
    v_sql    text;
    v_seen   text[] := ARRAY[]::text[];
    v_parts  text[] := ARRAY[]::text[];
    v_idx    integer := 0;
BEGIN
    IF p_aggs IS NULL OR jsonb_typeof(p_aggs) <> 'array' OR jsonb_array_length(p_aggs) = 0 THEN
        RAISE EXCEPTION 'pvt_build_agg_projection: p_aggs must be a non-empty JSON array';
    END IF;

    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_aggs) LOOP
        v_idx := v_idx + 1;
        v_alias := CASE WHEN v_entry ? 'alias' AND jsonb_typeof(v_entry->'alias') = 'string'
                        THEN v_entry->>'alias'
                        ELSE '_agg_' || v_idx::text
                   END;
        IF v_alias = ANY(v_seen) THEN
            RAISE EXCEPTION 'pvt_build_agg_projection: duplicate alias "%"', v_alias;
        END IF;
        v_seen := v_seen || v_alias;
        v_sql := pvt_build_agg_expr(v_entry, p_fields, p_base_prefix);
        v_parts := v_parts || (v_sql || ' AS ' || quote_ident(v_alias));
    END LOOP;
    RETURN array_to_string(v_parts, ', ');
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_agg_projection(jsonb, jsonb, text) IS
    'Compiles an array of aggregate entries into a `<sql> AS "<alias>", ...` projection fragment. Aliases default to positional `_agg_<i>`. Used by terminal aggregations and GROUP BY projections.';


-- ===== 20_pvt_build_query_sql.sql =====
-- =====================================================================
-- 20_pvt_build_query_sql.sql
-- ---------------------------------------------------------------------
-- Factor-1 orchestrator: produce the full SQL string for a base-fields
-- search query. Pure SQL generation, no execution. The result is used
-- both by pvt_search_objects_base (via EXECUTE) and by the SQL preview
-- entry point.
--
-- Function:
--   pvt_build_query_sql(
--       p_scheme_id    bigint,
--       p_filter       jsonb,
--       p_limit        integer,
--       p_offset       integer,
--       p_order        jsonb,
--       p_max_depth    integer,
--       p_distinct     boolean,
--       p_source_mode  text,        -- flat | tree | tree_descendants | tree_children | tree_roots | tree_leaves | tree_ancestors
--       p_tree_ids     bigint[],
--       p_include_seed boolean,     -- false strips seed from tree_descendants
--       p_polymorphic  boolean      -- false ANDs scheme_id into recursive walks
--   ) RETURNS text
--
-- Generated shape (depending on filter content):
--
--   Shape A (pure base, no props referenced) -- emitted directly:
--      SELECT [DISTINCT] _id FROM _objects o
--       WHERE o._id_scheme = X [AND <pushed_base_predicate>]
--       [ORDER BY ...] [LIMIT ... OFFSET ...]
--
--   Shape B narrow (props referenced, no null-check, no tree,
--                   no nested-dict groups) -- Pro-parity outer:
--      WITH _pvt_cte AS (
--          SELECT v._id_object, <pivot cols>
--          FROM _values v
--          WHERE v._id_structure = ANY(ARRAY[<sids>]::bigint[])
--            AND v._id_object IN (SELECT _id FROM _objects
--                                  WHERE _id_scheme=X [AND <push>])
--          GROUP BY v._id_object
--      )
--      SELECT [DISTINCT] o._id
--        FROM _pvt_cte
--        JOIN _objects o ON o._id = _pvt_cte._id_object
--       WHERE <residual with base refs rewritten to o.*>
--       [ORDER BY ...] [LIMIT ... OFFSET ...]
--
--   Shape C wide (null-check, tree mode, or nested-dict groups) --
--                                             legacy wide CTE outer:
--      <pvt_build_cte_sql wide output>
--      SELECT [DISTINCT] _id FROM _pvt_cte
--       WHERE <pvt_build_where_from_json (bare base refs)>
--       [ORDER BY ...] [LIMIT ... OFFSET ...]
-- =====================================================================

-- Signature evolved in v0.6.0 (added p_include_seed + p_polymorphic) and
-- v0.6.1 (added p_distinct_on for SELECT DISTINCT ON parity).
-- CREATE OR REPLACE cannot change argument lists, so drop legacy forms.
DROP FUNCTION IF EXISTS pvt_build_query_sql(bigint, jsonb, integer, integer, jsonb, integer, boolean);
DROP FUNCTION IF EXISTS pvt_build_query_sql(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[]);
DROP FUNCTION IF EXISTS pvt_build_query_sql(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[], boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_query_sql(
    p_scheme_id    bigint,
    p_filter       jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0,
    p_order        jsonb    DEFAULT NULL,
    p_max_depth    integer  DEFAULT NULL,
    p_distinct     boolean  DEFAULT false,
    p_source_mode  text     DEFAULT 'flat',
    p_tree_ids     bigint[] DEFAULT NULL,
    p_include_seed boolean  DEFAULT true,
    p_polymorphic  boolean  DEFAULT true,
    p_distinct_on  jsonb    DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_fields    jsonb;
    v_cte_sql   text;
    v_where_sql text;
    v_order_sql text;
    v_paging    text := '';
    v_force_outer boolean;
    v_narrow      boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_residual_sql  text;
    v_can_push      boolean;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_query_sql: p_scheme_id is required';
    END IF;

    -- DISTINCT and DISTINCT ON are mutually exclusive; PostgreSQL does
    -- not allow combining them in a single SELECT.
    IF p_distinct AND p_distinct_on IS NOT NULL
       AND jsonb_typeof(p_distinct_on) = 'array'
       AND jsonb_array_length(p_distinct_on) > 0 THEN
        RAISE EXCEPTION 'pvt_build_query_sql: p_distinct and p_distinct_on are mutually exclusive';
    END IF;

    -- 1. Resolve metadata for every referenced field. DISTINCT ON
    --    entries share shape with ORDER BY entries (field|field_path|$expr),
    --    so we concat them into the order channel for collection only;
    --    actual rendering happens via separate builders.
    v_fields := pvt_collect_fields(
        p_scheme_id,
        p_filter,
        CASE
            WHEN p_distinct_on IS NULL THEN p_order
            WHEN p_order       IS NULL THEN p_distinct_on
            ELSE p_order || p_distinct_on
        END,
        false);

    -- 2. Pro-style base pushdown: peel off base/hierarchical predicates
    --    so they can be AND-ed inside the pivot CTE (before JOIN/GROUP
    --    BY) instead of after it. Indexes on _objects(_id_scheme,
    --    _date_*, _id_parent, _id_owner, ...) become usable.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;

    -- The outer WHERE walker only sees the residual; if pushdown
    -- swallowed the entire filter the outer becomes 'TRUE'.
    v_outer_filter := v_residual;

    -- 2b. Shape A shortcut: pure-base flat query. When the filter
    --     references zero PROPS fields (only base / hierarchical),
    --     split swallowed everything (residual = NULL), and we are
    --     not in tree mode, the whole query collapses to a plain
    --     SELECT over _objects -- no CTE, no JOIN with _values, no
    --     GROUP BY. This is the same shape Pro emits for queries
    --     like `WhereRedb(o => o._id > 0)`.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN

        v_order_sql := pvt_build_order_conditions(p_order, v_fields, '', p_distinct_on);

        IF p_limit IS NOT NULL AND p_limit >= 0 THEN
            v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
        END IF;
        IF COALESCE(p_offset, 0) > 0 THEN
            v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
        END IF;

        RETURN 'SELECT '
               || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
               || pvt_build_distinct_on_select(p_distinct_on, v_fields, 'o.')
               || '_id FROM _objects o'
               || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
               || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                       THEN ' AND ' || v_push_sql
                       ELSE '' END
               || v_order_sql
               || v_paging;
    END IF;

    -- 3. Decide LEFT vs INNER JOIN on _values: use LEFT only when the
    --    RESIDUAL filter explicitly tests for ABSENCE (i.e. needs to
    --    surface rows that have no matching _values record). Base-only
    --    null tests are already pushed; $notNull/{$ne:null} are handled
    --    correctly by INNER JOIN (it drops absent rows, which is the
    --    desired outcome). See pvt_has_absence_check for the exact set.
    --    Also force outer when v_fields contains zero PROPS entries
    --    (only base/hierarchical refs from order/distinct_on): the
    --    narrow shape requires a non-empty pivot SID array, otherwise
    --    pvt_build_cte_sql silently downgrades the inner CTE to wide
    --    and the outer 'JOIN _objects o ON o._id = _pvt_cte._id_object'
    --    references a column the wide CTE does not project.
    v_force_outer := pvt_has_absence_check(v_outer_filter)
                     OR (v_fields = '{}'::jsonb)
                     OR NOT EXISTS (
                         SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
                          WHERE COALESCE(val->>'kind', '') <> 'base'
                     );

    -- 3b. Decide narrow vs wide CTE shape. Narrow Pro-parity shape
    --     applies when (a) we don't need LEFT JOIN (no absence check)
    --     and (b) the field set is NOT mixed (scalar pivot + nested-dict
    --     in the same query). Pure scalar pivot -> narrow _values scan.
    --     Pure nested-dict -> nested-only optimization (pvt_build_cte_sql
    --     skips the heavy pi wrapper and folds the object-set restriction
    --     into the nested CTE). Tree mode IS compatible in both. In
    --     narrow mode the outer SELECT JOINs _objects to expose base
    --     columns for residual / order refs.
    DECLARE
        v_has_nested boolean;
        v_has_scalar boolean;
    BEGIN
        v_has_nested := EXISTS (
            SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
             WHERE (val->>'dict_key') IS NOT NULL
               AND (val->>'parent_sid') IS NOT NULL);
        v_has_scalar := EXISTS (
            SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
               AND NOT (COALESCE((val->>'length_modifier')::boolean, false))
               AND NOT ((val->>'dict_key') IS NOT NULL AND (val->>'parent_sid') IS NOT NULL));
        v_narrow := NOT v_force_outer
                    AND NOT (v_has_nested AND v_has_scalar);
    END;

    -- 4. Pro-parity residual WHERE pushdown: when the outer filter
    --    references only pivoted columns (no base/hierarchical refs),
    --    render it now and feed to pvt_build_cte_sql so it wraps
    --    _pvt_cte body as `SELECT * FROM (<pivot>) pvt WHERE <residual>`.
    --    The planner then prunes rows BEFORE the outer JOIN _objects.
    --    Mixed filters keep the legacy outer WHERE (base refs can only
    --    be resolved via the `o` alias the JOIN exposes).
    v_can_push := v_narrow
                  AND v_outer_filter IS NOT NULL
                  AND v_outer_filter <> '{}'::jsonb
                  AND NOT pvt_filter_has_base_refs(v_outer_filter, v_fields);
    IF v_can_push THEN
        v_residual_sql := pvt_build_where_from_json(v_outer_filter, v_fields, '');
    ELSE
        v_residual_sql := NULL;
    END IF;

    -- 5. Build the CTE (with optional base + residual pushdown).
    v_cte_sql := pvt_build_cte_sql(
        p_scheme_id      => p_scheme_id,
        p_fields         => v_fields,
        p_source_mode    => p_source_mode,
        p_tree_ids       => p_tree_ids,
        p_max_depth      => p_max_depth,
        p_force_outer    => v_force_outer,
        p_extra_where    => v_push_sql,
        p_narrow         => v_narrow,
        p_include_seed   => p_include_seed,
        p_polymorphic    => p_polymorphic,
        p_residual_where => v_residual_sql
    );

    -- 6. Build outer predicates and ordering. In narrow shape base
    --    columns live on _objects (alias `o`), so base references in
    --    the residual / order BY must be emitted as `o.<col>`. In wide
    --    shape base columns are projected into _pvt_cte unaliased and
    --    references stay bare (legacy behavior). When residual was
    --    pushed inside _pvt_cte we skip the outer WHERE.
    IF v_can_push THEN
        v_where_sql := 'TRUE';
    ELSE
        v_where_sql := pvt_build_where_from_json(
            v_outer_filter, v_fields,
            CASE WHEN v_narrow THEN 'o.' ELSE '' END);
    END IF;
    v_order_sql := pvt_build_order_conditions(
        p_order, v_fields,
        CASE WHEN v_narrow THEN 'o.' ELSE '' END,
        p_distinct_on);

    -- 6. Paging.
    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- 7. Assemble outer SELECT. Narrow shape JOINs _objects so the
    --    residual WHERE / ORDER BY can address base columns via the
    --    `o` alias; wide shape selects directly from _pvt_cte.
    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT '
            || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
            || pvt_build_distinct_on_select(p_distinct_on, v_fields, 'o.')
            || 'o._id FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || CASE WHEN v_where_sql = 'TRUE' THEN '' ELSE E'\nWHERE ' || v_where_sql END
            || v_order_sql
            || v_paging;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT '
        || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
        || pvt_build_distinct_on_select(p_distinct_on, v_fields, '')
        || '_id FROM _pvt_cte'
        || CASE WHEN v_where_sql = 'TRUE' THEN '' ELSE E'\nWHERE ' || v_where_sql END
        || v_order_sql
        || v_paging;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_query_sql(bigint, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[], boolean, boolean, jsonb) IS
    'Factor-1 of the PVT engine: pure SQL generation. Composes pvt_collect_fields + pvt_build_cte_sql + pvt_build_where_from_json + pvt_build_order_conditions into a complete SELECT statement that yields a single _id column. p_source_mode: flat | tree | tree_descendants | tree_children | tree_roots | tree_leaves | tree_ancestors. p_include_seed=false strips seed objects from tree_descendants. p_polymorphic=false restricts recursive walks to p_scheme_id (default polymorphic matches Pro). p_distinct_on accepts the same {field|field_path|$expr} entries as p_order; its expressions are rendered as SELECT DISTINCT ON (...) and auto-prepended ASC to ORDER BY (PostgreSQL requirement). Mutually exclusive with p_distinct.';


-- ===== 21_pvt_aggregate.sql =====
-- =====================================================================
-- 21_pvt_aggregate.sql
-- ---------------------------------------------------------------------
-- Terminal aggregations: produce a single-row result of N aggregates
-- over a (filtered) PVT source. Sister of pvt_build_query_sql (file 20)
-- - same source-mode / tree / push-down plumbing, different projection
-- and no LIMIT/OFFSET/ORDER BY/DISTINCT (a single row has no order).
--
--   pvt_build_aggregate_sql(
--       p_scheme_id    bigint,
--       p_filter       jsonb,
--       p_aggregations jsonb,         -- non-empty array of agg entries
--       p_source_mode  text,
--       p_tree_ids     bigint[],
--       p_max_depth    integer,
--       p_include_seed boolean,
--       p_polymorphic  boolean
--   ) RETURNS text
--
-- Generated shape (Shape A vs Shape B/C selection mirrors file 20):
--
--   Shape A (pure-base filter, no PROPS refs, flat mode):
--      SELECT <agg1> AS "<a1>", ... FROM _objects o
--       WHERE o._id_scheme = X [AND <push>]
--
--   Shape B narrow:
--      <pvt_build_cte_sql narrow output>
--      SELECT <agg1> AS "<a1>", ...
--        FROM _pvt_cte JOIN _objects o ON o._id = _pvt_cte._id_object
--       WHERE <residual with base refs as o.*>
--
--   Shape C wide:
--      <pvt_build_cte_sql wide output>
--      SELECT <agg1> AS "<a1>", ... FROM _pvt_cte
--       WHERE <residual>
-- =====================================================================


-- ---------- pvt_collect_extra_fields ----------------------------------
-- Resolve metadata for a flat array of field paths. Used by aggregation
-- / window orchestrators to register fields referenced inside aggregate
-- operands / window arguments (which don't fit the field|field_path|
-- $expr shape consumed by pvt_collect_fields).
CREATE OR REPLACE FUNCTION pvt_collect_extra_fields(
    p_scheme_id bigint,
    p_paths     text[]
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_path   text;
    v_meta   jsonb;
    v_result jsonb := '{}'::jsonb;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_collect_extra_fields: p_scheme_id is required';
    END IF;
    IF p_paths IS NULL OR array_length(p_paths, 1) IS NULL THEN
        RETURN v_result;
    END IF;

    FOREACH v_path IN ARRAY p_paths LOOP
        IF v_path IS NULL OR v_path = '' THEN
            CONTINUE;
        END IF;
        v_meta := pvt_resolve_field_path(p_scheme_id, v_path);
        v_result := v_result || jsonb_build_object(v_path, v_meta);
    END LOOP;
    RETURN v_result;
END;
$BODY$;

COMMENT ON FUNCTION pvt_collect_extra_fields(bigint, text[]) IS
    'Resolve metadata for a flat array of field paths and merge into a JSONB fields map. Used to register fields referenced inside aggregate operands / window arguments that do not fit the order/filter shapes consumed by pvt_collect_fields.';


-- ---------- pvt_aggregations_field_names ------------------------------
-- Yield every field path referenced inside an aggregation array (operands
-- + per-aggregate filters). Used by orchestrators to extend the fields
-- map fed into pvt_build_cte_sql.
CREATE OR REPLACE FUNCTION pvt_aggregations_field_names(p_aggs jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_entry jsonb;
BEGIN
    IF p_aggs IS NULL OR jsonb_typeof(p_aggs) <> 'array' THEN
        RETURN;
    END IF;
    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_aggs) LOOP
        RETURN QUERY SELECT pvt_agg_entry_field_names(v_entry);
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_aggregations_field_names(jsonb) IS
    'Yields every field path referenced inside an array of aggregate entries (operands + filters). Empty when p_aggs is null or non-array.';


-- ---------------------------------------------------------------------
-- pvt_build_aggregate_sql
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS pvt_build_aggregate_sql(bigint, jsonb, jsonb, text, bigint[], integer, boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_aggregate_sql(
    p_scheme_id    bigint,
    p_filter       jsonb,
    p_aggregations jsonb,
    p_source_mode  text     DEFAULT 'flat',
    p_tree_ids     bigint[] DEFAULT NULL,
    p_max_depth    integer  DEFAULT NULL,
    p_include_seed boolean  DEFAULT true,
    p_polymorphic  boolean  DEFAULT true
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_fields      jsonb;
    v_extra       jsonb;
    v_extra_names text[];
    v_cte_sql     text;
    v_where_sql   text;
    v_force_outer boolean;
    v_narrow      boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_proj          text;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_aggregate_sql: p_scheme_id is required';
    END IF;
    IF p_aggregations IS NULL
       OR jsonb_typeof(p_aggregations) <> 'array'
       OR jsonb_array_length(p_aggregations) = 0 THEN
        RAISE EXCEPTION 'pvt_build_aggregate_sql: p_aggregations must be a non-empty JSON array';
    END IF;

    -- 1. Collect fields from filter (no order entries for aggregations).
    v_fields := pvt_collect_fields(p_scheme_id, p_filter, NULL, false);

    -- 2. Extend with fields referenced inside aggregate operands /
    --    per-aggregate filters.
    v_extra_names := ARRAY(SELECT DISTINCT n
                             FROM pvt_aggregations_field_names(p_aggregations) AS n
                            WHERE n IS NOT NULL);
    v_extra := pvt_collect_extra_fields(p_scheme_id, v_extra_names);
    v_fields := v_fields || v_extra;

    -- 3. Base pushdown.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;
    v_outer_filter := v_residual;

    -- 4. Shape A shortcut: pure-base flat aggregation.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN
        v_proj := pvt_build_agg_projection(p_aggregations, v_fields, 'o.');
        RETURN 'SELECT ' || v_proj
            || ' FROM _objects o'
            || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
            || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                    THEN ' AND ' || v_push_sql ELSE '' END;
    END IF;

    -- 5. Narrow vs wide decision (mirrors file 20). Tree modes always go
    -- wide because pvt_build_cte_sql ignores narrow=true in tree mode
    -- and emits the base-cols CTE keyed on _id (not _id_object).
    v_force_outer := pvt_has_absence_check(v_outer_filter) OR (v_fields = '{}'::jsonb);
    v_narrow := p_source_mode = 'flat'
                AND NOT v_force_outer
                AND NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(v_fields) AS e(k, val)
                     WHERE (val->>'dict_key') IS NOT NULL
                       AND (val->>'parent_sid') IS NOT NULL);

    -- 6. Build CTE.
    v_cte_sql := pvt_build_cte_sql(
        p_scheme_id    => p_scheme_id,
        p_fields       => v_fields,
        p_source_mode  => p_source_mode,
        p_tree_ids     => p_tree_ids,
        p_max_depth    => p_max_depth,
        p_force_outer  => v_force_outer,
        p_extra_where  => v_push_sql,
        p_narrow       => v_narrow,
        p_include_seed => p_include_seed,
        p_polymorphic  => p_polymorphic
    );

    -- 7. Residual WHERE + projection (prefix depends on shape).
    v_where_sql := pvt_build_where_from_json(
        v_outer_filter, v_fields,
        CASE WHEN v_narrow THEN 'o.' ELSE '' END);
    v_proj := pvt_build_agg_projection(
        p_aggregations, v_fields,
        CASE WHEN v_narrow THEN 'o.' ELSE '' END);

    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT ' || v_proj
            || ' FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || E'\nWHERE ' || v_where_sql;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT ' || v_proj
        || ' FROM _pvt_cte'
        || E'\nWHERE ' || v_where_sql;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_aggregate_sql(bigint, jsonb, jsonb, text, bigint[], integer, boolean, boolean) IS
    'Terminal aggregation orchestrator: emits a single-row SELECT projecting N aggregate columns over the PVT source. p_aggregations is a non-empty JSON array of {alias, $<func>: <expr>|"*", distinct?, filter?} entries. Tree variants reuse the same p_source_mode / p_tree_ids / p_max_depth / p_include_seed / p_polymorphic plumbing as pvt_build_query_sql.';


-- ===== 22_pvt_groupby.sql =====
-- =====================================================================
-- 22_pvt_groupby.sql
-- ---------------------------------------------------------------------
-- GROUP BY orchestrator: project group keys + aggregates, with optional
-- HAVING / ORDER BY / LIMIT / OFFSET clauses. Sister of files 20 and 21
-- - reuses the same field-collection, push-down and CTE-shape decisions.
--
--   pvt_build_groupby_sql(
--       p_scheme_id    bigint,
--       p_filter       jsonb,
--       p_group_by     jsonb,    -- non-empty array of {field|$expr,alias?}
--       p_aggregations jsonb,    -- optional array of agg entries (see 19)
--       p_having       jsonb,    -- optional boolean-expression node
--       p_order        jsonb,    -- optional ORDER BY entries
--       p_limit        integer,
--       p_offset       integer,
--       p_source_mode  text,
--       p_tree_ids     bigint[],
--       p_max_depth    integer,
--       p_include_seed boolean,
--       p_polymorphic  boolean
--   ) RETURNS text
--
-- Generated shape (narrow / wide selection mirrors files 20-21):
--
--   SELECT <grp1_sql> AS "<a1>", ..., <agg1_sql> AS "<aa1>", ...
--     FROM (CTE or _objects)
--    WHERE <residual>
--    GROUP BY <grp1_sql>, <grp2_sql>, ...
--   [HAVING <having_sql>]
--   [ORDER BY ...] [LIMIT ...] [OFFSET ...]
--
-- HAVING is a free feature over Pro: Pro currently emits zero HAVING
-- support, so any predicate over an aggregate had to be done client-side.
-- In HAVING, ORDER BY (and select-side projections), aggregates may be
-- written inline as {"$count":"*"} / {"$sum":{"$field":"X"}} thanks to
-- pvt_build_scalar_expr's aggregate-passthrough extension; alias refs
-- are not supported -- repeat the expression (PostgreSQL is fine with
-- HAVING on raw aggregate expressions).
-- =====================================================================

DROP FUNCTION IF EXISTS pvt_build_groupby_sql(bigint, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer, text, bigint[], integer, boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_groupby_sql(
    p_scheme_id    bigint,
    p_filter       jsonb,
    p_group_by     jsonb,
    p_aggregations jsonb    DEFAULT NULL,
    p_having       jsonb    DEFAULT NULL,
    p_order        jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0,
    p_source_mode  text     DEFAULT 'flat',
    p_tree_ids     bigint[] DEFAULT NULL,
    p_max_depth    integer  DEFAULT NULL,
    p_include_seed boolean  DEFAULT true,
    p_polymorphic  boolean  DEFAULT true
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_fields        jsonb;
    v_extra         jsonb;
    v_extra_names   text[];
    v_cte_sql       text;
    v_where_sql     text;
    v_having_sql    text := '';
    v_order_sql     text;
    v_paging        text := '';
    v_force_outer   boolean;
    v_narrow        boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_prefix        text;
    v_grp_entry     jsonb;
    v_grp_idx       integer := 0;
    v_grp_col       text;
    v_grp_alias     text;
    v_grp_seen      text[] := ARRAY[]::text[];
    v_select_parts  text[] := ARRAY[]::text[];
    v_groupby_parts text[] := ARRAY[]::text[];
    v_select_sql    text;
    v_groupby_sql   text;
    -- Pro-parity inline GROUP BY: when the field set is pure scalar
    -- pivot (no base/nested-dict/simple-dict/array refs) AND filter is
    -- narrow-eligible, skip the CTE wrapper and emit
    -- `SELECT pvt.<grp>, agg(...) FROM (<inline pivot>) pvt GROUP BY pvt.<grp>`.
    -- The inline pivot lifts `v._array_index IS NULL` out of every
    -- column FILTER into a single outer WHERE -- index-friendly at scale.
    v_inline        boolean := false;
    v_inline_sql    text;
    v_inline_cols   text := '';
    v_inline_sids   text := '';
    v_inline_li_join boolean := false;
    v_inline_first  boolean := true;
    v_fname         text;
    v_fmeta         jsonb;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_groupby_sql: p_scheme_id is required';
    END IF;
    IF p_group_by IS NULL
       OR jsonb_typeof(p_group_by) <> 'array'
       OR jsonb_array_length(p_group_by) = 0 THEN
        RAISE EXCEPTION 'pvt_build_groupby_sql: p_group_by must be a non-empty JSON array';
    END IF;

    -- 1. Collect fields from filter + group_by (piped via order channel)
    --    + ORDER BY entries.
    v_fields := pvt_collect_fields(
        p_scheme_id,
        p_filter,
        CASE
            WHEN p_order IS NULL THEN p_group_by
            ELSE p_group_by || p_order
        END,
        false);

    -- 2. Extend with fields from aggregations + HAVING.
    v_extra_names := ARRAY(
        SELECT DISTINCT n FROM (
            SELECT pvt_aggregations_field_names(p_aggregations) AS n
            UNION ALL
            SELECT pvt_expr_field_names(p_having) AS n
        ) z WHERE n IS NOT NULL);
    v_extra := pvt_collect_extra_fields(p_scheme_id, v_extra_names);
    v_fields := v_fields || v_extra;

    -- 3. Base pushdown.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;
    v_outer_filter := v_residual;

    -- 4. Narrow vs wide. Tree modes always go wide (pvt_build_cte_sql
    -- emits base-cols CTE keyed on _id in tree mode, not _id_object).
    v_force_outer := pvt_has_absence_check(v_outer_filter) OR (v_fields = '{}'::jsonb);
    v_narrow := p_source_mode = 'flat'
                AND NOT v_force_outer
                AND NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(v_fields) AS e(k, val)
                     WHERE (val->>'dict_key') IS NOT NULL
                       AND (val->>'parent_sid') IS NOT NULL);
    v_prefix := CASE WHEN v_narrow THEN 'o.' ELSE '' END;

    -- 5. Shape A shortcut: pure-base flat group-by, no aggregates touch
    --    PROPS. Skip CTE entirely.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN
        v_prefix := 'o.';
        v_narrow := false;   -- mark so assembly below picks the "no CTE" branch
    ELSE
        -- 5b. Pro-parity inline GROUP BY eligibility. Pure scalar pivot,
        --     narrow shape, no tree, no base refs anywhere (filter,
        --     group_by, having, order, aggregations -- all live in v_fields).
        v_inline := v_narrow
                    AND p_source_mode = 'flat'
                    AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
                    AND NOT EXISTS (
                        SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
                         WHERE COALESCE(val->>'kind', '') = 'base'
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM jsonb_each(v_fields) AS e(k, val)
                         WHERE COALESCE(val->>'kind', '') <> 'base'
                           AND (
                                COALESCE((val->>'is_array')::boolean, false)
                             OR (val->>'dict_key') IS NOT NULL
                             OR (val->>'parent_sid') IS NOT NULL
                             OR COALESCE((val->>'length_modifier')::boolean, false)
                           )
                    );

        IF v_inline THEN
            -- Build inline pivot subquery: project _id + every scalar
            -- pivot column WITHOUT `_array_index IS NULL` per-FILTER
            -- (hoisted to outer WHERE). Cuts FILTER overhead and lets
            -- the planner use a single ANY()-index scan.
            FOR v_fname, v_fmeta IN SELECT key, value FROM jsonb_each(v_fields) LOOP
                IF COALESCE(v_fmeta->>'kind', '') = 'base' THEN CONTINUE; END IF;
                IF NOT v_inline_first THEN
                    v_inline_cols := v_inline_cols || ',' || E'\n        ';
                ELSE
                    v_inline_cols := E',\n        ';
                    v_inline_first := false;
                END IF;
                v_inline_cols := v_inline_cols
                              || pvt_build_column_expr(v_fname, v_fmeta, true);
                IF v_inline_sids <> '' THEN
                    v_inline_sids := v_inline_sids || ', ';
                END IF;
                v_inline_sids := v_inline_sids || (v_fmeta->>'sid');
                IF (v_fmeta->>'list_item_prop') IN ('Value', 'Alias') THEN
                    v_inline_li_join := true;
                END IF;
            END LOOP;
            v_inline_sql :=
                E'SELECT\n        o._id' || v_inline_cols ||
                E'\n    FROM _objects o' ||
                E'\n    INNER JOIN _values v ON v._id_object = o._id' ||
                CASE WHEN v_inline_li_join
                     THEN E'\n    LEFT JOIN _list_items li ON li._id = v._ListItem'
                     ELSE '' END ||
                E'\n    WHERE o._id_scheme = ' || p_scheme_id::text ||
                CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                     THEN ' AND ' || v_push_sql ELSE '' END ||
                E'\n      AND v._id_structure = ANY(ARRAY[' || v_inline_sids || ']::bigint[])' ||
                E'\n      AND v._array_index IS NULL' ||
                E'\n    GROUP BY o._id';
            v_prefix := 'pvt.';
        ELSE
            -- 6. Build CTE (legacy path: mixed types / tree / base refs).
            v_cte_sql := pvt_build_cte_sql(
                p_scheme_id    => p_scheme_id,
                p_fields       => v_fields,
                p_source_mode  => p_source_mode,
                p_tree_ids     => p_tree_ids,
                p_max_depth    => p_max_depth,
                p_force_outer  => v_force_outer,
                p_extra_where  => v_push_sql,
                p_narrow       => v_narrow,
                p_include_seed => p_include_seed,
                p_polymorphic  => p_polymorphic
            );
        END IF;
    END IF;

    -- 7. Build group-key projection + GROUP BY clause.
    FOR v_grp_entry IN SELECT value FROM jsonb_array_elements(p_group_by) LOOP
        v_grp_idx := v_grp_idx + 1;
        v_grp_col := _pvt_compile_order_col(v_grp_entry, v_fields, v_prefix, true);
        v_grp_alias := CASE
            WHEN v_grp_entry ? 'alias' AND jsonb_typeof(v_grp_entry->'alias') = 'string'
                THEN v_grp_entry->>'alias'
            WHEN v_grp_entry ? 'field'
                THEN v_grp_entry->>'field'
            WHEN v_grp_entry ? 'field_path'
                THEN v_grp_entry->>'field_path'
            ELSE '_grp_' || v_grp_idx::text
        END;
        IF v_grp_alias = ANY(v_grp_seen) THEN
            RAISE EXCEPTION 'pvt_build_groupby_sql: duplicate group-by alias "%"', v_grp_alias;
        END IF;
        v_grp_seen := v_grp_seen || v_grp_alias;
        v_select_parts  := v_select_parts  || (v_grp_col || ' AS ' || quote_ident(v_grp_alias));
        v_groupby_parts := v_groupby_parts || v_grp_col;
    END LOOP;

    -- 8. Append aggregate projection.
    IF p_aggregations IS NOT NULL
       AND jsonb_typeof(p_aggregations) = 'array'
       AND jsonb_array_length(p_aggregations) > 0 THEN
        v_select_parts := v_select_parts || pvt_build_agg_projection(p_aggregations, v_fields, v_prefix);
    END IF;

    v_select_sql  := array_to_string(v_select_parts,  ', ');
    v_groupby_sql := array_to_string(v_groupby_parts, ', ');

    -- 9. WHERE / HAVING / ORDER BY / paging.
    v_where_sql := pvt_build_where_from_json(v_outer_filter, v_fields, v_prefix);
    IF p_having IS NOT NULL AND p_having <> '{}'::jsonb THEN
        v_having_sql := E'\nHAVING ' || pvt_build_bool_expr(p_having, v_fields, v_prefix);
    END IF;
    v_order_sql := pvt_build_order_conditions(p_order, v_fields, v_prefix, NULL);

    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- 10. Assemble. Shape A: SELECT ... FROM _objects o. Inline: SELECT
    --     ... FROM (<inline pivot>) pvt (Pro-parity). Narrow: from
    --     _pvt_cte JOIN _objects o. Wide: from _pvt_cte.
    IF v_cte_sql IS NULL AND NOT v_inline THEN
        -- Shape A: pure-base flat.
        RETURN 'SELECT ' || v_select_sql
            || ' FROM _objects o'
            || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
            || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                    THEN ' AND ' || v_push_sql ELSE '' END
            || E'\nGROUP BY ' || v_groupby_sql
            || v_having_sql
            || v_order_sql
            || v_paging;
    END IF;

    IF v_inline THEN
        -- Pro-parity inline GROUP BY: no CTE, pivot is a derived table.
        RETURN 'SELECT ' || v_select_sql
            || E'\n  FROM (' || v_inline_sql || E'\n  ) pvt'
            || CASE WHEN v_where_sql = 'TRUE' THEN '' ELSE E'\n WHERE ' || v_where_sql END
            || E'\n GROUP BY ' || v_groupby_sql
            || v_having_sql
            || v_order_sql
            || v_paging;
    END IF;

    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT ' || v_select_sql
            || ' FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || E'\nWHERE ' || v_where_sql
            || E'\nGROUP BY ' || v_groupby_sql
            || v_having_sql
            || v_order_sql
            || v_paging;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT ' || v_select_sql
        || ' FROM _pvt_cte'
        || E'\nWHERE ' || v_where_sql
        || E'\nGROUP BY ' || v_groupby_sql
        || v_having_sql
        || v_order_sql
        || v_paging;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_groupby_sql(bigint, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer, text, bigint[], integer, boolean, boolean) IS
    'GROUP BY orchestrator. p_group_by is a non-empty array of {field|field_path|$expr, alias?} entries (ORDER-BY-shape minus direction). p_aggregations is an optional array of {alias, $<func>: <expr>|"*", distinct?, filter?} entries. p_having is an optional boolean-expression node where aggregates can be referenced inline via {"$count":"*"} / {"$sum":{"$field":"X"}} (alias refs not supported). HAVING support is a free feature over Pro.';


-- ===== 23_pvt_window.sql =====
-- =====================================================================
-- 23_pvt_window.sql
-- ---------------------------------------------------------------------
-- Window functions over PVT source. Window calls are scalar expressions
-- carrying an OVER (...) clause; the compiler is exposed both as an
-- explicit pvt_build_window_expr() entry point and as a `$over` scalar-
-- expression key so callers can drop windows anywhere a scalar fits
-- (PostgreSQL itself enforces SELECT/ORDER-BY only at execution time).
--
-- Window node shape:
--   {
--     "func":          "row_number" | "rank" | "dense_rank"
--                    | "percent_rank" | "cume_dist" | "ntile"
--                    | "lag" | "lead" | "first_value" | "last_value"
--                    | "nth_value"
--                    | "sum" | "avg" | "min" | "max" | "count",
--     "args":          [<scalar-expr>, ...]?,  -- function arguments
--     "partition_by":  [<order-shape-entry>, ...]?,
--     "order_by":      [<order-shape-entry-with-direction>, ...]?,
--     "frame": {
--         "type": "rows" | "range" | "groups",
--         "start": <bound>,
--         "end":   <bound>?,
--         "exclude": "current_row" | "group" | "ties" | "no_others"?
--     }?
--   }
--
-- Frame bound forms:
--   "unbounded_preceding"
--   "current_row"
--   "unbounded_following"
--   { "preceding": <int> }
--   { "following": <int> }
--
--   pvt_build_window_sql(
--       p_scheme_id    bigint,
--       p_filter       jsonb,
--       p_select       jsonb,    -- non-empty array of select entries
--       p_order        jsonb,
--       p_limit        integer,
--       p_offset       integer,
--       p_source_mode  text,
--       p_tree_ids     bigint[],
--       p_max_depth    integer,
--       p_include_seed boolean,
--       p_polymorphic  boolean
--   ) RETURNS text
--
-- Select entries follow the ORDER-BY shape: {field|field_path|$expr,
-- alias?}. Aggregates and window expressions are emitted via $expr.
-- =====================================================================


-- ---------- pvt_window_field_names ------------------------------------
-- Recursively yield every field path referenced inside a window node
-- (args + partition_by + order_by). Used by orchestrators to extend the
-- fields map fed into pvt_build_cte_sql.
CREATE OR REPLACE FUNCTION pvt_window_field_names(p_node jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_elem jsonb;
BEGIN
    IF p_node IS NULL OR jsonb_typeof(p_node) <> 'object' THEN
        RETURN;
    END IF;

    IF p_node ? 'args' AND jsonb_typeof(p_node->'args') = 'array' THEN
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_node->'args') LOOP
            RETURN QUERY SELECT pvt_expr_field_names(v_elem);
        END LOOP;
    END IF;

    IF p_node ? 'partition_by' AND jsonb_typeof(p_node->'partition_by') = 'array' THEN
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_node->'partition_by') LOOP
            IF v_elem ? '$expr' THEN
                RETURN QUERY SELECT pvt_expr_field_names(v_elem->'$expr');
            ELSIF v_elem ? 'field' OR v_elem ? 'field_path' THEN
                RETURN NEXT COALESCE(v_elem->>'field', v_elem->>'field_path');
            END IF;
        END LOOP;
    END IF;

    IF p_node ? 'order_by' AND jsonb_typeof(p_node->'order_by') = 'array' THEN
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_node->'order_by') LOOP
            IF v_elem ? '$expr' THEN
                RETURN QUERY SELECT pvt_expr_field_names(v_elem->'$expr');
            ELSIF v_elem ? 'field' OR v_elem ? 'field_path' THEN
                RETURN NEXT COALESCE(v_elem->>'field', v_elem->>'field_path');
            END IF;
        END LOOP;
    END IF;
END;
$BODY$;

COMMENT ON FUNCTION pvt_window_field_names(jsonb) IS
    'Recursively yields every field path referenced inside a window node (args + partition_by + order_by). Used by pvt_build_window_sql to extend the fields map.';


-- ---------- _pvt_compile_frame_bound (internal) -----------------------
CREATE OR REPLACE FUNCTION _pvt_compile_frame_bound(p_bound jsonb, p_label text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $BODY$
DECLARE
    v_t text;
    v_n bigint;
BEGIN
    IF p_bound IS NULL OR jsonb_typeof(p_bound) = 'null' THEN
        RAISE EXCEPTION '_pvt_compile_frame_bound: frame.% is required', p_label;
    END IF;
    v_t := jsonb_typeof(p_bound);
    IF v_t = 'string' THEN
        DECLARE v_s text := lower(p_bound #>> '{}'); BEGIN
            IF v_s = 'unbounded_preceding' THEN RETURN 'UNBOUNDED PRECEDING'; END IF;
            IF v_s = 'current_row'         THEN RETURN 'CURRENT ROW';         END IF;
            IF v_s = 'unbounded_following' THEN RETURN 'UNBOUNDED FOLLOWING'; END IF;
            RAISE EXCEPTION '_pvt_compile_frame_bound: invalid string bound "%"', v_s;
        END;
    END IF;
    IF v_t = 'object' THEN
        IF p_bound ? 'preceding' THEN
            v_n := (p_bound->>'preceding')::bigint;
            RETURN v_n::text || ' PRECEDING';
        END IF;
        IF p_bound ? 'following' THEN
            v_n := (p_bound->>'following')::bigint;
            RETURN v_n::text || ' FOLLOWING';
        END IF;
    END IF;
    RAISE EXCEPTION '_pvt_compile_frame_bound: bound must be "unbounded_preceding"/"current_row"/"unbounded_following" or {preceding|following: N}, got %', p_bound::text;
END;
$BODY$;


-- ---------- _pvt_compile_window_over (internal) -----------------------
-- Emit `OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ... AND ...)`.
-- Returns the empty string when no window keys are present (caller emits
-- `OVER ()` for those, e.g. unframed ROW_NUMBER).
CREATE OR REPLACE FUNCTION _pvt_compile_window_over(
    p_node        jsonb,
    p_fields      jsonb,
    p_base_prefix text
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_parts        text[] := ARRAY[]::text[];
    v_pb_parts     text[] := ARRAY[]::text[];
    v_elem         jsonb;
    v_order_sql    text;
    v_frame        jsonb;
    v_frame_kw     text;
    v_frame_start  text;
    v_frame_end    text;
    v_frame_excl   text;
    v_frame_sql    text;
    v_synth_order  jsonb;
BEGIN
    -- PARTITION BY -------------------------------------------------
    IF p_node ? 'partition_by'
       AND jsonb_typeof(p_node->'partition_by') = 'array'
       AND jsonb_array_length(p_node->'partition_by') > 0 THEN
        FOR v_elem IN SELECT value FROM jsonb_array_elements(p_node->'partition_by') LOOP
            v_pb_parts := v_pb_parts || _pvt_compile_order_col(v_elem, p_fields, p_base_prefix, true);
        END LOOP;
        v_parts := v_parts || ('PARTITION BY ' || array_to_string(v_pb_parts, ', '));
    END IF;

    -- ORDER BY (reuse pvt_build_order_conditions to keep direction/
    -- NULLS handling consistent; strip the leading "ORDER BY " prefix
    -- because we are inside OVER (...)).
    IF p_node ? 'order_by'
       AND jsonb_typeof(p_node->'order_by') = 'array'
       AND jsonb_array_length(p_node->'order_by') > 0 THEN
        v_synth_order := p_node->'order_by';
        v_order_sql := pvt_build_order_conditions(v_synth_order, p_fields, p_base_prefix, NULL);
        -- pvt_build_order_conditions returns either '' or E'\nORDER BY ...'.
        IF v_order_sql IS NOT NULL AND v_order_sql <> '' THEN
            v_parts := v_parts || ltrim(v_order_sql, E'\n');
        END IF;
    END IF;

    -- FRAME ---------------------------------------------------------
    IF p_node ? 'frame' THEN
        v_frame := p_node->'frame';
        IF jsonb_typeof(v_frame) <> 'object' THEN
            RAISE EXCEPTION '_pvt_compile_window_over: frame must be an object';
        END IF;
        v_frame_kw := upper(COALESCE(v_frame->>'type', 'rows'));
        IF v_frame_kw NOT IN ('ROWS', 'RANGE', 'GROUPS') THEN
            RAISE EXCEPTION '_pvt_compile_window_over: frame.type must be rows|range|groups (got %)', v_frame_kw;
        END IF;
        v_frame_start := _pvt_compile_frame_bound(v_frame->'start', 'start');
        IF v_frame ? 'end' AND jsonb_typeof(v_frame->'end') <> 'null' THEN
            v_frame_end := _pvt_compile_frame_bound(v_frame->'end', 'end');
            v_frame_sql := v_frame_kw || ' BETWEEN ' || v_frame_start || ' AND ' || v_frame_end;
        ELSE
            v_frame_sql := v_frame_kw || ' ' || v_frame_start;
        END IF;
        IF v_frame ? 'exclude' AND jsonb_typeof(v_frame->'exclude') = 'string' THEN
            v_frame_excl := lower(v_frame->>'exclude');
            v_frame_sql := v_frame_sql || ' EXCLUDE ' || CASE v_frame_excl
                WHEN 'current_row' THEN 'CURRENT ROW'
                WHEN 'group'       THEN 'GROUP'
                WHEN 'ties'        THEN 'TIES'
                WHEN 'no_others'   THEN 'NO OTHERS'
                ELSE NULL
            END;
            IF v_frame_excl NOT IN ('current_row', 'group', 'ties', 'no_others') THEN
                RAISE EXCEPTION '_pvt_compile_window_over: frame.exclude must be current_row|group|ties|no_others (got %)', v_frame_excl;
            END IF;
        END IF;
        v_parts := v_parts || v_frame_sql;
    END IF;

    IF array_length(v_parts, 1) IS NULL THEN
        RETURN 'OVER ()';
    END IF;
    RETURN 'OVER (' || array_to_string(v_parts, ' ') || ')';
END;
$BODY$;


-- ---------- pvt_build_window_expr -------------------------------------
CREATE OR REPLACE FUNCTION pvt_build_window_expr(
    p_node        jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_func     text;
    v_args     jsonb;
    v_args_sql text[] := ARRAY[]::text[];
    v_elem     jsonb;
    v_arg_sql  text;
    v_call     text;
    v_over     text;
    v_distinct boolean := false;
    v_filter   jsonb;
    v_filt_sql text := '';
BEGIN
    IF p_node IS NULL OR jsonb_typeof(p_node) <> 'object' THEN
        RAISE EXCEPTION 'pvt_build_window_expr: window node must be an object';
    END IF;
    IF NOT (p_node ? 'func') OR jsonb_typeof(p_node->'func') <> 'string' THEN
        RAISE EXCEPTION 'pvt_build_window_expr: window node must include "func" (string)';
    END IF;

    v_func := lower(p_node->>'func');
    IF v_func NOT IN (
        'row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist',
        'ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value',
        'sum', 'avg', 'min', 'max', 'count'
    ) THEN
        RAISE EXCEPTION 'pvt_build_window_expr: unsupported window func "%"', v_func;
    END IF;

    -- Build the argument list. count(*) is a special shorthand.
    IF p_node ? 'args' THEN
        v_args := p_node->'args';
        IF v_func = 'count'
           AND jsonb_typeof(v_args) = 'array'
           AND jsonb_array_length(v_args) = 1
           AND jsonb_typeof(v_args->0) = 'string'
           AND (v_args->0 #>> '{}') = '*' THEN
            v_call := 'COUNT(*)';
        ELSE
            IF jsonb_typeof(v_args) <> 'array' THEN
                RAISE EXCEPTION 'pvt_build_window_expr: args must be an array (got %)', jsonb_typeof(v_args);
            END IF;
            FOR v_elem IN SELECT value FROM jsonb_array_elements(v_args) LOOP
                v_arg_sql := pvt_build_scalar_expr(v_elem, p_fields, p_base_prefix);
                v_args_sql := v_args_sql || v_arg_sql;
            END LOOP;
            v_call := upper(v_func) || '(' || array_to_string(v_args_sql, ', ') || ')';
        END IF;
    ELSE
        IF v_func IN ('ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value') THEN
            RAISE EXCEPTION 'pvt_build_window_expr: % requires "args"', v_func;
        END IF;
        IF v_func IN ('sum', 'avg', 'min', 'max', 'count') THEN
            RAISE EXCEPTION 'pvt_build_window_expr: aggregate window % requires "args"', v_func;
        END IF;
        v_call := upper(v_func) || '()';
    END IF;

    -- Optional FILTER (WHERE ...) for aggregate windows.
    IF p_node ? 'filter'
       AND v_func IN ('sum', 'avg', 'min', 'max', 'count') THEN
        v_filter := p_node->'filter';
        v_filt_sql := ' FILTER (WHERE ' || pvt_build_bool_expr(v_filter, p_fields, p_base_prefix) || ')';
    END IF;

    v_over := _pvt_compile_window_over(p_node, p_fields, p_base_prefix);
    RETURN v_call || v_filt_sql || ' ' || v_over;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_window_expr(jsonb, jsonb, text) IS
    'Compiles a window-function node into a SQL fragment `FUNC(args) [FILTER (WHERE ...)] OVER (...)`. Supports ranking, navigation (lag/lead/first_value/...), and aggregate windows with optional FILTER. Frame spec: rows|range|groups + start/end bounds + optional exclude.';


-- ---------- Hook $over into pvt_build_scalar_expr ---------------------
-- We can't extend scalar_expr from this file, but $over is recognised by
-- the existing dispatcher's "no matching operator" branch via a tiny
-- shim wrapper: callers may use either {"$over": <window_node>} or a
-- bare window node passed directly to pvt_build_window_expr.
-- The shim is registered as a scalar-expr operator by replacing the
-- final RAISE in pvt_build_scalar_expr -- see 17_pvt_expr.sql tail.


-- ---------------------------------------------------------------------
-- pvt_build_window_sql
-- ---------------------------------------------------------------------
DROP FUNCTION IF EXISTS pvt_build_window_sql(bigint, jsonb, jsonb, jsonb, integer, integer, text, bigint[], integer, boolean, boolean);

CREATE OR REPLACE FUNCTION pvt_build_window_sql(
    p_scheme_id    bigint,
    p_filter       jsonb,
    p_select       jsonb,
    p_order        jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0,
    p_source_mode  text     DEFAULT 'flat',
    p_tree_ids     bigint[] DEFAULT NULL,
    p_max_depth    integer  DEFAULT NULL,
    p_include_seed boolean  DEFAULT true,
    p_polymorphic  boolean  DEFAULT true
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_fields        jsonb;
    v_extra         jsonb;
    v_extra_names   text[];
    v_cte_sql       text;
    v_where_sql     text;
    v_order_sql     text;
    v_paging        text := '';
    v_force_outer   boolean;
    v_narrow        boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_prefix        text;
    v_sel_entry     jsonb;
    v_sel_idx       integer := 0;
    v_sel_col       text;
    v_sel_alias     text;
    v_sel_seen      text[] := ARRAY[]::text[];
    v_select_parts  text[] := ARRAY[]::text[];
    v_select_sql    text;
    v_win_node      jsonb;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_window_sql: p_scheme_id is required';
    END IF;
    IF p_select IS NULL
       OR jsonb_typeof(p_select) <> 'array'
       OR jsonb_array_length(p_select) = 0 THEN
        RAISE EXCEPTION 'pvt_build_window_sql: p_select must be a non-empty JSON array';
    END IF;

    -- 1. Collect fields from filter + select + order.
    v_fields := pvt_collect_fields(
        p_scheme_id,
        p_filter,
        CASE WHEN p_order IS NULL THEN p_select ELSE p_select || p_order END,
        false);

    -- 2. Extend with fields referenced inside window args / partition /
    --    order. Walk every $expr in the select array and harvest from
    --    nested $over nodes via pvt_window_field_names. (Regular $field
    --    refs inside $expr are already covered by pvt_collect_fields.)
    v_extra_names := ARRAY(
        SELECT DISTINCT n
          FROM (
            SELECT pvt_window_field_names(e->'$expr'->'$over') AS n
              FROM jsonb_array_elements(p_select) e
             WHERE e ? '$expr' AND (e->'$expr') ? '$over'
          ) z WHERE n IS NOT NULL);
    v_extra := pvt_collect_extra_fields(p_scheme_id, v_extra_names);
    v_fields := v_fields || v_extra;

    -- 3. Base pushdown.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;
    v_outer_filter := v_residual;

    -- 4. Narrow vs wide. Tree modes always go wide (CTE keyed on _id).
    v_force_outer := pvt_has_absence_check(v_outer_filter) OR (v_fields = '{}'::jsonb);
    v_narrow := p_source_mode = 'flat'
                AND NOT v_force_outer
                AND NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(v_fields) AS e(k, val)
                     WHERE (val->>'dict_key') IS NOT NULL
                       AND (val->>'parent_sid') IS NOT NULL);
    v_prefix := CASE WHEN v_narrow THEN 'o.' ELSE '' END;

    -- 5. Shape A shortcut.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN
        v_prefix := 'o.';
        v_narrow := false;
    ELSE
        v_cte_sql := pvt_build_cte_sql(
            p_scheme_id    => p_scheme_id,
            p_fields       => v_fields,
            p_source_mode  => p_source_mode,
            p_tree_ids     => p_tree_ids,
            p_max_depth    => p_max_depth,
            p_force_outer  => v_force_outer,
            p_extra_where  => v_push_sql,
            p_narrow       => v_narrow,
            p_include_seed => p_include_seed,
            p_polymorphic  => p_polymorphic
        );
    END IF;

    -- 6. Build select projection.
    FOR v_sel_entry IN SELECT value FROM jsonb_array_elements(p_select) LOOP
        v_sel_idx := v_sel_idx + 1;
        -- $over shortcut: {"$over": <win>} as the entry's expression.
        IF v_sel_entry ? '$expr' AND jsonb_typeof(v_sel_entry->'$expr') = 'object'
           AND (v_sel_entry->'$expr') ? '$over' THEN
            v_win_node := (v_sel_entry->'$expr')->'$over';
            v_sel_col := pvt_build_window_expr(v_win_node, v_fields, v_prefix);
        ELSE
            v_sel_col := _pvt_compile_order_col(v_sel_entry, v_fields, v_prefix, true);
        END IF;
        v_sel_alias := CASE
            WHEN v_sel_entry ? 'alias' AND jsonb_typeof(v_sel_entry->'alias') = 'string'
                THEN v_sel_entry->>'alias'
            WHEN v_sel_entry ? 'field'
                THEN v_sel_entry->>'field'
            WHEN v_sel_entry ? 'field_path'
                THEN v_sel_entry->>'field_path'
            ELSE '_sel_' || v_sel_idx::text
        END;
        IF v_sel_alias = ANY(v_sel_seen) THEN
            RAISE EXCEPTION 'pvt_build_window_sql: duplicate select alias "%"', v_sel_alias;
        END IF;
        v_sel_seen := v_sel_seen || v_sel_alias;
        v_select_parts := v_select_parts || (v_sel_col || ' AS ' || quote_ident(v_sel_alias));
    END LOOP;
    v_select_sql := array_to_string(v_select_parts, ', ');

    -- 7. WHERE / ORDER BY / paging.
    v_where_sql := pvt_build_where_from_json(v_outer_filter, v_fields, v_prefix);
    v_order_sql := pvt_build_order_conditions(p_order, v_fields, v_prefix, NULL);

    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- 8. Assemble.
    IF v_cte_sql IS NULL THEN
        RETURN 'SELECT ' || v_select_sql
            || ' FROM _objects o'
            || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
            || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                    THEN ' AND ' || v_push_sql ELSE '' END
            || v_order_sql
            || v_paging;
    END IF;

    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT ' || v_select_sql
            || ' FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || E'\nWHERE ' || v_where_sql
            || v_order_sql
            || v_paging;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT ' || v_select_sql
        || ' FROM _pvt_cte'
        || E'\nWHERE ' || v_where_sql
        || v_order_sql
        || v_paging;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_window_sql(bigint, jsonb, jsonb, jsonb, integer, integer, text, bigint[], integer, boolean, boolean) IS
    'Window-function orchestrator. p_select is a non-empty array of {field|field_path|$expr, alias?} entries; window expressions are written as {alias, $expr: {$over: {func, args, partition_by, order_by, frame}}}. Tree variants reuse the same p_source_mode / p_tree_ids / p_max_depth / p_include_seed / p_polymorphic plumbing as pvt_build_query_sql.';


-- ===== 24_pvt_projection.sql =====
-- =====================================================================
-- 24_pvt_projection.sql
-- ---------------------------------------------------------------------
-- Generic column projection ("Select" shaping). Sister of
-- pvt_build_query_sql (file 20): same source-mode / tree / push-down
-- plumbing, but instead of returning a single `_id` column the outer
-- SELECT yields an arbitrary list of typed columns built from the
-- entries in p_projection.
--
-- A projection entry has one of the following shapes:
--
--   { "field": "Name" }                                 -- bare field
--   { "field_path": "Address.City", "alias": "city" }   -- nested path
--   { "alias": "yr",
--     "$expr": { "$year": { "$field": "HireDate" } } }  -- expression
--   { "alias": "kind",
--     "$expr": { "$case": [
--         { "when": {"$gt":[{"$field":"Age"},{"$const":18}]},
--           "then": {"$const":"adult"} },
--         { "else": {"$const":"kid"} } ] } }
--
-- Functions:
--   pvt_projection_field_names(p_projection)
--       -> SETOF text  (every field path referenced by projection
--                       entries, for pvt_collect_extra_fields)
--   pvt_build_projection(p_projection, p_fields, p_base_prefix)
--       -> text         (a `<expr> AS "<alias>", ...` fragment, no
--                        leading SELECT)
--   pvt_build_projection_sql(...) -> text  (full SELECT orchestrator,
--                                           Shape A / B narrow / C wide)
--
-- Free-over-Pro angle: projections can carry arbitrary scalar-expr
-- nodes ($case / $coalesce / $cast / $year / $power / $concat / ...);
-- Pro's projection path is limited to bare field references plus a
-- handful of arithmetic / string ops on primitives.
-- =====================================================================


-- ---------- pvt_projection_field_names --------------------------------
-- Yield every field path referenced inside a projection array. Used by
-- the orchestrator to extend the fields map fed into pvt_build_cte_sql
-- via pvt_collect_extra_fields.
CREATE OR REPLACE FUNCTION pvt_projection_field_names(p_projection jsonb)
RETURNS SETOF text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_entry jsonb;
    v_path  text;
BEGIN
    IF p_projection IS NULL OR jsonb_typeof(p_projection) <> 'array' THEN
        RETURN;
    END IF;
    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_projection) LOOP
        IF v_entry IS NULL OR jsonb_typeof(v_entry) <> 'object' THEN
            CONTINUE;
        END IF;
        IF v_entry ? 'field' AND jsonb_typeof(v_entry->'field') = 'string' THEN
            v_path := v_entry->>'field';
            IF v_path IS NOT NULL AND v_path <> '' THEN
                RETURN NEXT v_path;
            END IF;
        END IF;
        IF v_entry ? 'field_path' AND jsonb_typeof(v_entry->'field_path') = 'string' THEN
            v_path := v_entry->>'field_path';
            IF v_path IS NOT NULL AND v_path <> '' THEN
                RETURN NEXT v_path;
            END IF;
        END IF;
        IF v_entry ? '$expr' THEN
            RETURN QUERY SELECT pvt_expr_field_names(v_entry->'$expr');
        END IF;
    END LOOP;
END;
$BODY$;

COMMENT ON FUNCTION pvt_projection_field_names(jsonb) IS
    'Yields every field path referenced inside a projection array (bare field / field_path entries plus $expr operands). Empty when p_projection is null or non-array.';


-- ---------- pvt_build_projection --------------------------------------
-- Compile a projection array into a SELECT-list fragment.
-- Aliases default to: the field/field_path value (when present) or
-- `_proj_<i>` (positional). Duplicate aliases raise.
CREATE OR REPLACE FUNCTION pvt_build_projection(
    p_projection  jsonb,
    p_fields      jsonb,
    p_base_prefix text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_entry  jsonb;
    v_alias  text;
    v_expr   jsonb;
    v_sql    text;
    v_seen   text[] := ARRAY[]::text[];
    v_parts  text[] := ARRAY[]::text[];
    v_idx    integer := 0;
    v_keys   integer;
BEGIN
    IF p_projection IS NULL
       OR jsonb_typeof(p_projection) <> 'array'
       OR jsonb_array_length(p_projection) = 0 THEN
        RAISE EXCEPTION 'pvt_build_projection: p_projection must be a non-empty JSON array';
    END IF;

    FOR v_entry IN SELECT value FROM jsonb_array_elements(p_projection) LOOP
        v_idx := v_idx + 1;
        IF v_entry IS NULL OR jsonb_typeof(v_entry) <> 'object' THEN
            RAISE EXCEPTION 'pvt_build_projection: entry #% is not a JSON object', v_idx;
        END IF;

        -- Exactly one source key per entry: field | field_path | $expr.
        v_keys := (CASE WHEN v_entry ? 'field'      THEN 1 ELSE 0 END)
                + (CASE WHEN v_entry ? 'field_path' THEN 1 ELSE 0 END)
                + (CASE WHEN v_entry ? '$expr'      THEN 1 ELSE 0 END);
        IF v_keys = 0 THEN
            RAISE EXCEPTION 'pvt_build_projection: entry #% has no source key (field|field_path|$expr): %', v_idx, v_entry::text;
        END IF;
        IF v_keys > 1 THEN
            RAISE EXCEPTION 'pvt_build_projection: entry #% has more than one source key (field|field_path|$expr): %', v_idx, v_entry::text;
        END IF;

        -- Determine alias.
        IF v_entry ? 'alias' AND jsonb_typeof(v_entry->'alias') = 'string' THEN
            v_alias := v_entry->>'alias';
        ELSIF v_entry ? 'field' THEN
            v_alias := v_entry->>'field';
        ELSIF v_entry ? 'field_path' THEN
            v_alias := v_entry->>'field_path';
        ELSE
            v_alias := '_proj_' || v_idx::text;
        END IF;
        IF v_alias IS NULL OR v_alias = '' THEN
            RAISE EXCEPTION 'pvt_build_projection: entry #% has empty alias', v_idx;
        END IF;
        IF v_alias = ANY(v_seen) THEN
            RAISE EXCEPTION 'pvt_build_projection: duplicate alias "%"', v_alias;
        END IF;
        v_seen := v_seen || v_alias;

        -- Normalize source to a scalar-expr node.
        IF v_entry ? '$expr' THEN
            v_expr := v_entry->'$expr';
        ELSIF v_entry ? 'field' THEN
            v_expr := jsonb_build_object('$field', v_entry->>'field');
        ELSE
            v_expr := jsonb_build_object('$field', v_entry->>'field_path');
        END IF;

        v_sql := pvt_build_scalar_expr(v_expr, p_fields, p_base_prefix);
        v_parts := v_parts || (v_sql || ' AS ' || quote_ident(v_alias));
    END LOOP;

    RETURN array_to_string(v_parts, ', ');
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_projection(jsonb, jsonb, text) IS
    'Compiles a projection array into a SELECT-list fragment `<expr> AS "<alias>", ...`. Supports {field} | {field_path} | {$expr:<scalar-expr-node>} entries. Aliases default to the field name or `_proj_<i>` (positional).';


-- =====================================================================
-- pvt_build_projection_sql
-- ---------------------------------------------------------------------
-- Full orchestrator: produce a complete SELECT statement that yields
-- the requested projection. Mirrors pvt_build_query_sql for filter /
-- order / distinct / tree / push-down handling; the only difference is
-- the SELECT list comes from p_projection instead of being a fixed
-- `_id`.
-- =====================================================================
DROP FUNCTION IF EXISTS pvt_build_projection_sql(bigint, jsonb, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[], boolean, boolean, jsonb);

CREATE OR REPLACE FUNCTION pvt_build_projection_sql(
    p_scheme_id    bigint,
    p_projection   jsonb,
    p_filter       jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0,
    p_order        jsonb    DEFAULT NULL,
    p_max_depth    integer  DEFAULT NULL,
    p_distinct     boolean  DEFAULT false,
    p_source_mode  text     DEFAULT 'flat',
    p_tree_ids     bigint[] DEFAULT NULL,
    p_include_seed boolean  DEFAULT true,
    p_polymorphic  boolean  DEFAULT true,
    p_distinct_on  jsonb    DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_fields        jsonb;
    v_extra         jsonb;
    v_extra_names   text[];
    v_cte_sql       text;
    v_where_sql     text;
    v_order_sql     text;
    v_paging        text := '';
    v_force_outer   boolean;
    v_narrow        boolean;
    v_push_sql      text;
    v_residual      jsonb;
    v_outer_filter  jsonb;
    v_proj          text;
    v_prefix        text;
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_projection_sql: p_scheme_id is required';
    END IF;
    IF p_projection IS NULL
       OR jsonb_typeof(p_projection) <> 'array'
       OR jsonb_array_length(p_projection) = 0 THEN
        RAISE EXCEPTION 'pvt_build_projection_sql: p_projection must be a non-empty JSON array';
    END IF;

    -- DISTINCT and DISTINCT ON are mutually exclusive.
    IF p_distinct AND p_distinct_on IS NOT NULL
       AND jsonb_typeof(p_distinct_on) = 'array'
       AND jsonb_array_length(p_distinct_on) > 0 THEN
        RAISE EXCEPTION 'pvt_build_projection_sql: p_distinct and p_distinct_on are mutually exclusive';
    END IF;

    -- 1. Collect fields from filter + order + distinct_on (same channel
    --    as pvt_build_query_sql), then extend with projection refs.
    v_fields := pvt_collect_fields(
        p_scheme_id,
        p_filter,
        CASE
            WHEN p_distinct_on IS NULL THEN p_order
            WHEN p_order       IS NULL THEN p_distinct_on
            ELSE p_order || p_distinct_on
        END,
        false);

    v_extra_names := ARRAY(SELECT DISTINCT n
                             FROM pvt_projection_field_names(p_projection) AS n
                            WHERE n IS NOT NULL);
    v_extra := pvt_collect_extra_fields(p_scheme_id, v_extra_names);
    v_fields := v_fields || v_extra;

    -- 2. Base pushdown.
    SELECT s.v_pushdown_sql, s.v_residual_filter
      INTO v_push_sql, v_residual
      FROM pvt_split_filter(p_filter, v_fields) s;
    v_outer_filter := v_residual;

    -- 3. Shape A shortcut: pure-base flat projection.
    IF v_outer_filter IS NULL
       AND p_source_mode = 'flat'
       AND (p_tree_ids IS NULL OR array_length(p_tree_ids, 1) IS NULL)
       AND NOT EXISTS (
            SELECT 1
              FROM jsonb_each(v_fields) AS e(k, val)
             WHERE COALESCE(val->>'kind', '') <> 'base'
       ) THEN

        v_order_sql := pvt_build_order_conditions(p_order, v_fields, 'o.', p_distinct_on);
        v_proj := pvt_build_projection(p_projection, v_fields, 'o.');

        IF p_limit IS NOT NULL AND p_limit >= 0 THEN
            v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
        END IF;
        IF COALESCE(p_offset, 0) > 0 THEN
            v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
        END IF;

        RETURN 'SELECT '
               || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
               || pvt_build_distinct_on_select(p_distinct_on, v_fields, 'o.')
               || v_proj
               || ' FROM _objects o'
               || E'\nWHERE o._id_scheme = ' || p_scheme_id::text
               || CASE WHEN v_push_sql IS NOT NULL AND v_push_sql <> ''
                       THEN ' AND ' || v_push_sql ELSE '' END
               || v_order_sql
               || v_paging;
    END IF;

    -- 4. Narrow vs wide. Tree modes always go wide (pvt_build_cte_sql
    -- emits base-cols CTE keyed on _id in tree mode, not _id_object).
    v_force_outer := pvt_has_absence_check(v_outer_filter) OR (v_fields = '{}'::jsonb);
    v_narrow := p_source_mode = 'flat'
                AND NOT v_force_outer
                AND NOT EXISTS (
                    SELECT 1
                      FROM jsonb_each(v_fields) AS e(k, val)
                     WHERE (val->>'dict_key') IS NOT NULL
                       AND (val->>'parent_sid') IS NOT NULL);
    v_prefix := CASE WHEN v_narrow THEN 'o.' ELSE '' END;

    -- 5. Build CTE.
    v_cte_sql := pvt_build_cte_sql(
        p_scheme_id    => p_scheme_id,
        p_fields       => v_fields,
        p_source_mode  => p_source_mode,
        p_tree_ids     => p_tree_ids,
        p_max_depth    => p_max_depth,
        p_force_outer  => v_force_outer,
        p_extra_where  => v_push_sql,
        p_narrow       => v_narrow,
        p_include_seed => p_include_seed,
        p_polymorphic  => p_polymorphic
    );

    -- 6. WHERE / ORDER / projection (prefix depends on shape).
    v_where_sql := pvt_build_where_from_json(v_outer_filter, v_fields, v_prefix);
    v_order_sql := pvt_build_order_conditions(p_order, v_fields, v_prefix, p_distinct_on);
    v_proj      := pvt_build_projection(p_projection, v_fields, v_prefix);

    -- 7. Paging.
    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- 8. Assemble outer SELECT.
    IF v_narrow THEN
        RETURN v_cte_sql
            || E'\nSELECT '
            || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
            || pvt_build_distinct_on_select(p_distinct_on, v_fields, v_prefix)
            || v_proj
            || ' FROM _pvt_cte'
            || E'\nJOIN _objects o ON o._id = _pvt_cte._id_object'
            || E'\nWHERE ' || v_where_sql
            || v_order_sql
            || v_paging;
    END IF;

    RETURN v_cte_sql
        || E'\nSELECT '
        || CASE WHEN p_distinct THEN 'DISTINCT ' ELSE '' END
        || pvt_build_distinct_on_select(p_distinct_on, v_fields, v_prefix)
        || v_proj
        || ' FROM _pvt_cte'
        || E'\nWHERE ' || v_where_sql
        || v_order_sql
        || v_paging;
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_projection_sql(bigint, jsonb, jsonb, integer, integer, jsonb, integer, boolean, text, bigint[], boolean, boolean, jsonb) IS
    'Factor-1 projection orchestrator. Composes pvt_collect_fields + pvt_collect_extra_fields + pvt_build_cte_sql + pvt_build_where_from_json + pvt_build_order_conditions + pvt_build_projection into a complete SELECT statement with arbitrary projected columns. p_projection accepts entries {field} | {field_path} | {alias?, $expr:<scalar-expr-node>}. Filter / order / distinct / source-mode / pushdown semantics are identical to pvt_build_query_sql.';


-- ===== 25_object_props_v2.sql =====
-- =====================================================================
-- 25_object_props_v2.sql
-- ---------------------------------------------------------------------
-- Experimental SQL-driven re-implementation of the per-level reader
--   build_hierarchical_properties_optimized()  →  build_hierarchical_properties_sql()
-- and a wrapper
--   get_object_json()  →  get_object_json_v2()
-- for A/B benchmarking against the production reader.
--
-- Goal:
--   replace the plpgsql FOR-LOOP over _scheme_metadata_cache fields with
--   a single SQL statement that materializes all field values at the
--   current nesting level via jsonb_object_agg(field_name, field_value).
--
-- Semantics:
--   MUST be byte-for-byte identical to build_hierarchical_properties_optimized()
--   for a fixed depth. Verified by comparing md5(get_object_json(id)::text)
--   against md5(get_object_json_v2(id)::text) over all objects.
--
-- Notes:
--   - These functions live alongside the production ones; nothing existing
--     is overridden. To switch the reader globally, change get_object_json
--     in redb_json_objects.sql to call build_hierarchical_properties_sql.
--   - File is bundled into pvt_bundle.sql by _bundle.ps1. It only adds new
--     functions, so re-running the bundle on prod is safe.
-- =====================================================================

DROP FUNCTION IF EXISTS build_hierarchical_properties_sql(bigint, bigint, bigint, _values[], integer, text, bigint);
DROP FUNCTION IF EXISTS get_object_json_v2(bigint, integer);

-- ---------------------------------------------------------------------
-- build_hierarchical_properties_sql
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION build_hierarchical_properties_sql(
    object_id bigint,
    parent_structure_id bigint,
    object_scheme_id bigint,
    all_values _values[],
    max_depth integer DEFAULT 10,
    array_index text DEFAULT NULL,
    parent_value_id bigint DEFAULT NULL
) RETURNS jsonb
LANGUAGE plpgsql
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb;
BEGIN
    -- Defensive recursion guard (matches original behavior)
    IF max_depth < -100 THEN
        RETURN jsonb_build_object('error', 'Max recursion depth reached for hierarchical fields');
    END IF;

    -- Auto-populate metadata cache (matches original behavior)
    IF NOT EXISTS(SELECT 1 FROM _scheme_metadata_cache WHERE _scheme_id = object_scheme_id LIMIT 1) THEN
        PERFORM sync_metadata_cache_for_scheme(object_scheme_id);
    END IF;

    -- ONE SQL statement: build all fields at this nesting level.
    -- jsonb_object_agg replaces the plpgsql FOR-LOOP + `||` concat.
    SELECT jsonb_object_agg(field_name, field_value ORDER BY field_order, field_struct_id)
    INTO result_json
    FROM (
        SELECT
            c._name AS field_name,
            c._order AS field_order,
            c._structure_id AS field_struct_id,
            CASE
                -- =========== ARRAY ===========
                WHEN c._collection_type = -9223372036854775668 THEN
                    CASE
                        -- No head record => array property is NULL
                        WHEN NOT EXISTS (
                            SELECT 1 FROM unnest(all_values) v
                            WHERE v._id_structure = c._structure_id
                              AND v._array_index IS NULL
                              AND ((parent_value_id IS NULL AND v._array_parent_id IS NULL)
                                OR (parent_value_id IS NOT NULL AND v._array_parent_id = parent_value_id))
                        ) THEN NULL
                        -- Array of Class
                        WHEN c.type_semantic = 'Object' THEN
                            COALESCE(
                                (SELECT jsonb_agg(
                                            build_hierarchical_properties_sql(
                                                object_id, c._structure_id, object_scheme_id,
                                                all_values, max_depth, v._array_index, v._id)
                                            ORDER BY CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END,
                                                     v._array_index)
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '[]'::jsonb)
                        -- Array of primitives / RObject
                        ELSE
                            COALESCE(
                                (SELECT jsonb_agg(
                                            CASE
                                                WHEN c.type_semantic = '_RObject' AND v._Object IS NOT NULL THEN
                                                    get_object_json(v._Object, max_depth - 1)
                                                WHEN c.db_type = 'String' THEN to_jsonb(v._String)
                                                WHEN c.db_type = 'Long' THEN
                                                    CASE WHEN v._ListItem IS NOT NULL
                                                         THEN build_listitem_jsonb(v._ListItem, max_depth)
                                                         ELSE to_jsonb(v._Long) END
                                                WHEN c.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                                WHEN c.db_type = 'Double' THEN to_jsonb(v._Double)
                                                WHEN c.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                                WHEN c.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                                WHEN c.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                                WHEN c.db_type = 'ListItem' THEN
                                                    build_listitem_jsonb(v._ListItem, max_depth)
                                                WHEN c.db_type = 'ByteArray' THEN
                                                    to_jsonb(encode(decode(v._ByteArray::text, 'base64'), 'base64'))
                                                ELSE NULL
                                            END
                                            ORDER BY CASE WHEN v._array_index ~ '^[0-9]+$' THEN v._array_index::int ELSE 0 END,
                                                     v._array_index)
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '[]'::jsonb)
                    END
                -- =========== DICTIONARY ===========
                WHEN c._collection_type = -9223372036854775667 THEN
                    CASE
                        WHEN NOT EXISTS (
                            SELECT 1 FROM unnest(all_values) v
                            WHERE v._id_structure = c._structure_id
                              AND v._array_index IS NULL
                              AND ((parent_value_id IS NULL AND v._array_parent_id IS NULL)
                                OR (parent_value_id IS NOT NULL AND v._array_parent_id = parent_value_id))
                        ) THEN NULL
                        -- Dict<K, RObject>
                        WHEN c.type_semantic = '_RObject' THEN
                            COALESCE(
                                (SELECT jsonb_object_agg(v._array_index,
                                            CASE WHEN v._Object IS NOT NULL THEN get_object_json(v._Object, max_depth - 1) ELSE NULL END)
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '{}'::jsonb)
                        -- Dict<K, Class>
                        WHEN c.type_semantic = 'Object' THEN
                            COALESCE(
                                (SELECT jsonb_object_agg(v._array_index,
                                            build_hierarchical_properties_sql(
                                                object_id, c._structure_id, object_scheme_id,
                                                all_values, max_depth, NULL, v._id))
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '{}'::jsonb)
                        -- Dict<K, primitive>
                        ELSE
                            COALESCE(
                                (SELECT jsonb_object_agg(v._array_index,
                                            CASE
                                                WHEN c.db_type = 'String' THEN to_jsonb(v._String)
                                                WHEN c.db_type = 'Long' THEN to_jsonb(v._Long)
                                                WHEN c.db_type = 'Guid' THEN to_jsonb(v._Guid)
                                                WHEN c.db_type = 'Double' THEN to_jsonb(v._Double)
                                                WHEN c.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                                                WHEN c.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                                                WHEN c.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                                                ELSE NULL
                                            END)
                                 FROM unnest(all_values) v
                                 WHERE v._id_structure = c._structure_id
                                   AND v._array_index IS NOT NULL
                                   AND v._array_parent_id = (
                                       SELECT v2._id FROM unnest(all_values) v2
                                       WHERE v2._id_structure = c._structure_id
                                         AND v2._array_index IS NULL
                                         AND ((parent_value_id IS NULL AND v2._array_parent_id IS NULL)
                                           OR (parent_value_id IS NOT NULL AND v2._array_parent_id = parent_value_id))
                                       LIMIT 1)),
                                '{}'::jsonb)
                    END
                -- =========== SCALAR RObject ===========
                WHEN c.type_name = 'Object' AND c.type_semantic = '_RObject' THEN
                    (SELECT CASE WHEN v._Object IS NOT NULL THEN get_object_json(v._Object, max_depth - 1) ELSE NULL END
                     FROM unnest(all_values) v
                     WHERE v._id_structure = c._structure_id
                       AND CASE
                           WHEN parent_value_id IS NOT NULL THEN v._array_parent_id = parent_value_id
                           WHEN array_index    IS NOT NULL THEN v._array_index = array_index
                           ELSE                                 v._array_index IS NULL
                           END
                     LIMIT 1)
                -- =========== SCALAR Class ===========
                WHEN c.type_semantic = 'Object' THEN
                    (SELECT CASE WHEN v._Guid IS NULL THEN NULL
                                 ELSE build_hierarchical_properties_sql(
                                          object_id, c._structure_id, object_scheme_id,
                                          all_values, max_depth, NULL, v._id) END
                     FROM unnest(all_values) v
                     WHERE v._id_structure = c._structure_id
                       AND CASE
                           WHEN parent_value_id IS NOT NULL THEN v._array_parent_id = parent_value_id
                           WHEN array_index    IS NOT NULL THEN v._array_index = array_index
                           ELSE                                 v._array_index IS NULL
                           END
                     LIMIT 1)
                -- =========== SCALAR primitives ===========
                ELSE
                    (SELECT
                         CASE
                             WHEN c.db_type = 'String' THEN to_jsonb(v._String)
                             WHEN c.db_type = 'Long' THEN
                                 CASE WHEN v._ListItem IS NOT NULL
                                      THEN build_listitem_jsonb(v._ListItem, max_depth)
                                      ELSE to_jsonb(v._Long) END
                             WHEN c.db_type = 'Guid' THEN to_jsonb(v._Guid)
                             WHEN c.db_type = 'Double' THEN to_jsonb(v._Double)
                             WHEN c.db_type = 'Numeric' THEN to_jsonb(v._Numeric)
                             WHEN c.db_type = 'DateTimeOffset' THEN to_jsonb(v._DateTimeOffset)
                             WHEN c.db_type = 'Boolean' THEN to_jsonb(v._Boolean)
                             WHEN c.db_type = 'ListItem' OR v._ListItem IS NOT NULL THEN
                                 CASE WHEN v._ListItem IS NOT NULL
                                      THEN build_listitem_jsonb(v._ListItem, max_depth)
                                      ELSE NULL END
                             WHEN c.db_type = 'ByteArray' THEN
                                 CASE WHEN v._ByteArray IS NOT NULL
                                      THEN to_jsonb(encode(decode(v._ByteArray::text, 'base64'), 'base64'))
                                      ELSE NULL END
                             ELSE NULL
                         END
                     FROM unnest(all_values) v
                     WHERE v._id_structure = c._structure_id
                       AND CASE
                           WHEN parent_value_id IS NOT NULL THEN v._array_parent_id = parent_value_id
                           WHEN array_index    IS NOT NULL THEN v._array_index = array_index
                           ELSE                                 v._array_index IS NULL
                           END
                     LIMIT 1)
            END AS field_value
        FROM _scheme_metadata_cache c
        WHERE c._scheme_id = object_scheme_id
          AND ((parent_structure_id IS NULL AND c._parent_structure_id IS NULL)
            OR (parent_structure_id IS NOT NULL AND c._parent_structure_id = parent_structure_id))
    ) f
    WHERE f.field_value IS NOT NULL;

    RETURN COALESCE(result_json, '{}'::jsonb);
END;
$BODY$;

-- ---------------------------------------------------------------------
-- get_object_json_v2 — wrapper that calls build_hierarchical_properties_sql.
-- Top-level body identical to get_object_json (only the inner call differs).
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_object_json_v2(
    object_id bigint,
    max_depth integer DEFAULT 10
) RETURNS jsonb
LANGUAGE plpgsql
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
    result_json jsonb;
    object_exists boolean;
    base_info jsonb;
    properties_info jsonb;
    object_scheme_id bigint;
    all_values _values[];
BEGIN
    SELECT EXISTS(SELECT 1 FROM _objects WHERE _id = object_id) INTO object_exists;
    IF NOT object_exists THEN
        RETURN NULL;
    END IF;

    IF max_depth <= 0 THEN
        SELECT jsonb_build_object(
            'id', o._id, 'name', o._name, 'scheme_id', o._id_scheme,
            'scheme_name', sc._name, 'parent_id', o._id_parent, 'owner_id', o._id_owner,
            'who_change_id', o._id_who_change, 'date_create', o._date_create,
            'date_modify', o._date_modify, 'date_begin', o._date_begin,
            'date_complete', o._date_complete, 'key', o._key, 'value_long', o._value_long,
            'value_string', o._value_string, 'value_guid', o._value_guid, 'note', o._note,
            'value_bool', o._value_bool, 'value_double', o._value_double,
            'value_numeric', o._value_numeric, 'value_datetime', o._value_datetime,
            'value_bytes', o._value_bytes, 'hash', o._hash
        ) INTO result_json
        FROM _objects o JOIN _schemes sc ON sc._id = o._id_scheme
        WHERE o._id = object_id;
        RETURN result_json;
    END IF;

    SELECT jsonb_build_object(
        'id', o._id, 'name', o._name, 'scheme_id', o._id_scheme,
        'scheme_name', sc._name, 'parent_id', o._id_parent, 'owner_id', o._id_owner,
        'who_change_id', o._id_who_change, 'date_create', o._date_create,
        'date_modify', o._date_modify, 'date_begin', o._date_begin,
        'date_complete', o._date_complete, 'key', o._key, 'value_long', o._value_long,
        'value_string', o._value_string, 'value_guid', o._value_guid, 'note', o._note,
        'value_bool', o._value_bool, 'value_double', o._value_double,
        'value_numeric', o._value_numeric, 'value_datetime', o._value_datetime,
        'value_bytes', o._value_bytes, 'hash', o._hash
    ), o._id_scheme
    INTO base_info, object_scheme_id
    FROM _objects o JOIN _schemes sc ON sc._id = o._id_scheme
    WHERE o._id = object_id;

    SELECT array_agg(v) INTO all_values
    FROM _values v
    WHERE v._id_object = object_id;

    IF all_values IS NULL THEN
        RETURN base_info || jsonb_build_object('properties', NULL);
    END IF;

    SELECT build_hierarchical_properties_sql(
        object_id, NULL, object_scheme_id,
        COALESCE(all_values, ARRAY[]::_values[]),
        max_depth, NULL, NULL
    ) INTO properties_info;

    RETURN base_info || jsonb_build_object('properties', COALESCE(properties_info, '{}'::jsonb));
END;
$BODY$;


-- ===== 26_pvt_array_groupby.sql =====
-- =====================================================================
-- 26_pvt_array_groupby.sql
-- ---------------------------------------------------------------------
-- Array-element GROUP BY orchestrator. Sister of 22_pvt_groupby.sql but
-- operates on a flat element-level subquery (LEFT JOINs on _values
-- keyed via _array_parent_id), so it intentionally bypasses the
-- object-pivot CTE machinery.
--
--   pvt_build_array_groupby_sql(
--       p_scheme_id     bigint,
--       p_array_path    text,        -- e.g. 'Items'
--       p_filter        jsonb,       -- optional outer object filter (PVT shape); when
--                                    -- non-empty, compiled via pvt_build_query_sql and
--                                    -- applied as arr._id_object IN (<filtered ids>).
--       p_group_by      jsonb,       -- non-empty array of {field, alias?}
--       p_aggregations  jsonb,       -- optional array of legacy agg entries
--                                    -- ({field, func, alias}) - same shape as
--                                    -- aggregate_array_grouped consumes
--       p_having        jsonb,       -- optional bool expression (PVT shape:
--                                    -- $and/$or/$not + $gt/$gte/$lt/$lte/$eq/$ne
--                                    -- with $sum/$count/$avg/$min/$max/$field/$const)
--       p_order         jsonb,       -- optional [{field, asc?}] over outer aliases
--       p_limit         integer,
--       p_offset        integer
--   ) RETURNS text
--
-- Inner subquery shape:
--   SELECT g1.<typed_col> AS "Field1", a1.<typed_col> AS "Field2", ...
--     FROM _values arr
--     JOIN _objects o ON o._id = arr._id_object
--     LEFT JOIN _values g1 ON g1._id_object = arr._id_object
--                         AND g1._id_structure = <sid>
--                         AND g1._array_parent_id = arr._id
--     ...
--    WHERE o._id_scheme = <s> AND arr._id_structure = <arr_sid>
--      AND arr._array_index IS NOT NULL
--
-- Outer query: SELECT/GROUP BY/HAVING/ORDER/LIMIT/OFFSET over inner.
--
-- HAVING reuses pvt_build_bool_expr against a fields-map populated for
-- non-base ('props' kind) entries, so $field references resolve to the
-- bare quoted alias emitted by the inner subquery.
-- =====================================================================

DROP FUNCTION IF EXISTS pvt_build_array_groupby_sql(bigint, text, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer);

CREATE OR REPLACE FUNCTION pvt_build_array_groupby_sql(
    p_scheme_id    bigint,
    p_array_path   text,
    p_filter       jsonb    DEFAULT NULL,
    p_group_by     jsonb    DEFAULT NULL,
    p_aggregations jsonb    DEFAULT NULL,
    p_having       jsonb    DEFAULT NULL,
    p_order        jsonb    DEFAULT NULL,
    p_limit        integer  DEFAULT NULL,
    p_offset       integer  DEFAULT 0
)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
    v_arr_sid       bigint;
    v_grp_entry     jsonb;
    v_agg_entry     jsonb;
    v_ord_entry     jsonb;
    v_field_path    text;
    v_alias         text;
    v_func          text;
    v_resolved      record;
    v_join_alias    text;
    v_col_name      text;
    v_join_idx      integer := 0;
    v_join_parts    text[] := ARRAY[]::text[];
    v_inner_select  text[] := ARRAY[]::text[];
    v_fields_map    jsonb := '{}'::jsonb;
    v_select_parts  text[] := ARRAY[]::text[];
    v_group_parts   text[] := ARRAY[]::text[];
    v_alias_seen    text[] := ARRAY[]::text[];
    v_ord_parts     text[] := ARRAY[]::text[];
    v_having_sql    text := '';
    v_paging        text := '';
    v_inner_sql     text;
    v_filter_sql    text;
    v_filter_clause text := '';
BEGIN
    IF p_scheme_id IS NULL THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_scheme_id is required';
    END IF;
    IF p_array_path IS NULL OR p_array_path = '' THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_array_path is required';
    END IF;
    IF p_group_by IS NULL
       OR jsonb_typeof(p_group_by) <> 'array'
       OR jsonb_array_length(p_group_by) = 0 THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: p_group_by must be a non-empty JSON array';
    END IF;
    -- Outer object filter: compile via pvt_build_query_sql into an
    -- "_id IN (...)" clause applied at the inner level (before unnest).
    IF p_filter IS NOT NULL
       AND jsonb_typeof(p_filter) = 'object'
       AND p_filter <> '{}'::jsonb THEN
        v_filter_sql := pvt_build_query_sql(
            p_scheme_id => p_scheme_id,
            p_filter    => p_filter,
            p_limit     => NULL,
            p_offset    => 0,
            p_order     => NULL,
            p_max_depth => NULL,
            p_distinct  => false);
        v_filter_clause := format(
            E'\n  AND arr._id_object IN (SELECT _id FROM (%s) _filt)',
            v_filter_sql);
    END IF;

    -- Resolve array structure id
    SELECT r.structure_id INTO v_arr_sid
    FROM pvt_resolve_field_path_table(p_scheme_id, p_array_path) r;
    IF v_arr_sid IS NULL THEN
        RAISE EXCEPTION 'pvt_build_array_groupby_sql: array "%" not found in scheme %',
            p_array_path, p_scheme_id;
    END IF;

    -- ---- group_by entries: register joins, inner aliases, outer GROUP BY parts
    FOR v_grp_entry IN SELECT value FROM jsonb_array_elements(p_group_by) LOOP
        v_field_path := v_grp_entry->>'field';
        IF v_field_path IS NULL OR v_field_path = '' THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: group_by entry missing "field"';
        END IF;
        v_alias := COALESCE(v_grp_entry->>'alias', v_field_path);
        IF v_alias = ANY(v_alias_seen) THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: duplicate alias "%"', v_alias;
        END IF;

        SELECT r.structure_id, r.db_type INTO v_resolved
        FROM pvt_resolve_field_path_table(p_scheme_id, p_array_path || '[].' || v_field_path) r;
        IF v_resolved.structure_id IS NULL THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: group field "%" not found inside array "%"',
                v_field_path, p_array_path;
        END IF;

        v_join_idx := v_join_idx + 1;
        v_join_alias := 'g' || v_join_idx::text;
        v_col_name := pvt_db_type_to_value_column(v_resolved.db_type);
        IF v_col_name IS NULL THEN
            RAISE EXCEPTION 'pvt_build_array_groupby_sql: unsupported db_type "%" for field "%"',
                v_resolved.db_type, v_field_path;
        END IF;

        v_join_parts := v_join_parts || format(
            'LEFT JOIN _values %I ON %I._id_object = arr._id_object AND %I._id_structure = %s AND %I._array_parent_id = arr._id',
            v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias);
        -- NOTE: typed value columns (_String/_Long/...) are declared unquoted in DDL
        -- and therefore case-folded to lowercase by PG. Reference them via %s (bare),
        -- never %I (quoted), to avoid 42703 column-not-found errors.
        v_inner_select := v_inner_select || format('%I.%s AS %I',
            v_join_alias, v_col_name, v_field_path);
        v_fields_map := v_fields_map || jsonb_build_object(
            v_field_path,
            jsonb_build_object('kind', 'props', 'db_type', v_resolved.db_type));

        v_select_parts := v_select_parts || (quote_ident(v_field_path) || ' AS ' || quote_ident(v_alias));
        v_group_parts  := v_group_parts  || quote_ident(v_field_path);
        v_alias_seen   := v_alias_seen   || v_alias;
    END LOOP;

    -- ---- aggregations
    IF p_aggregations IS NOT NULL
       AND jsonb_typeof(p_aggregations) = 'array' THEN
        FOR v_agg_entry IN SELECT value FROM jsonb_array_elements(p_aggregations) LOOP
            v_field_path := v_agg_entry->>'field';
            v_func := upper(COALESCE(v_agg_entry->>'func', ''));
            v_alias := COALESCE(v_agg_entry->>'alias', v_func);
            IF v_alias = '' THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: aggregation missing func';
            END IF;
            IF v_alias = ANY(v_alias_seen) THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: duplicate alias "%"', v_alias;
            END IF;

            IF v_func = 'COUNT' AND (v_field_path IS NULL OR v_field_path = '*') THEN
                v_select_parts := v_select_parts || ('COUNT(*) AS ' || quote_ident(v_alias));
                v_alias_seen := v_alias_seen || v_alias;
                CONTINUE;
            END IF;

            -- Add element-field join if not already projected by group_by
            IF NOT (v_fields_map ? v_field_path) THEN
                SELECT r.structure_id, r.db_type INTO v_resolved
                FROM pvt_resolve_field_path_table(p_scheme_id, p_array_path || '[].' || v_field_path) r;
                IF v_resolved.structure_id IS NULL THEN
                    RAISE EXCEPTION 'pvt_build_array_groupby_sql: agg field "%" not found inside array "%"',
                        v_field_path, p_array_path;
                END IF;
                v_join_idx := v_join_idx + 1;
                v_join_alias := 'a' || v_join_idx::text;
                v_col_name := pvt_db_type_to_value_column(v_resolved.db_type);
                IF v_col_name IS NULL THEN
                    RAISE EXCEPTION 'pvt_build_array_groupby_sql: unsupported db_type "%" for agg field "%"',
                        v_resolved.db_type, v_field_path;
                END IF;
                v_join_parts := v_join_parts || format(
                    'LEFT JOIN _values %I ON %I._id_object = arr._id_object AND %I._id_structure = %s AND %I._array_parent_id = arr._id',
                    v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias);
                -- See note above: %s (bare) for typed value columns.
                v_inner_select := v_inner_select || format('%I.%s AS %I',
                    v_join_alias, v_col_name, v_field_path);
                v_fields_map := v_fields_map || jsonb_build_object(
                    v_field_path,
                    jsonb_build_object('kind', 'props', 'db_type', v_resolved.db_type));
            END IF;

            v_select_parts := v_select_parts || (
                v_func || '(' || quote_ident(v_field_path) || ') AS ' || quote_ident(v_alias));
            v_alias_seen := v_alias_seen || v_alias;
        END LOOP;
    END IF;

    -- ---- HAVING: pre-collect $field refs, register joins, then translate
    IF p_having IS NOT NULL
       AND jsonb_typeof(p_having) = 'object'
       AND p_having <> '{}'::jsonb THEN
        FOR v_field_path IN
            SELECT DISTINCT n FROM pvt_expr_field_names(p_having) n WHERE n IS NOT NULL
        LOOP
            IF v_fields_map ? v_field_path THEN
                CONTINUE;
            END IF;
            SELECT r.structure_id, r.db_type INTO v_resolved
            FROM pvt_resolve_field_path_table(p_scheme_id, p_array_path || '[].' || v_field_path) r;
            IF v_resolved.structure_id IS NULL THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: HAVING field "%" not found inside array "%"',
                    v_field_path, p_array_path;
            END IF;
            v_join_idx := v_join_idx + 1;
            v_join_alias := 'h' || v_join_idx::text;
            v_col_name := pvt_db_type_to_value_column(v_resolved.db_type);
            IF v_col_name IS NULL THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: unsupported db_type "%" for HAVING field "%"',
                    v_resolved.db_type, v_field_path;
            END IF;
            v_join_parts := v_join_parts || format(
                'LEFT JOIN _values %I ON %I._id_object = arr._id_object AND %I._id_structure = %s AND %I._array_parent_id = arr._id',
                v_join_alias, v_join_alias, v_join_alias, v_resolved.structure_id, v_join_alias);
            -- See note above: %s (bare) for typed value columns.
            v_inner_select := v_inner_select || format('%I.%s AS %I',
                v_join_alias, v_col_name, v_field_path);
            v_fields_map := v_fields_map || jsonb_build_object(
                v_field_path,
                jsonb_build_object('kind', 'props', 'db_type', v_resolved.db_type));
        END LOOP;

        v_having_sql := E'\nHAVING ' || pvt_build_bool_expr(p_having, v_fields_map, '');
    END IF;

    -- ---- ORDER BY (over outer aliases; minimal asc/desc support)
    IF p_order IS NOT NULL
       AND jsonb_typeof(p_order) = 'array'
       AND jsonb_array_length(p_order) > 0 THEN
        FOR v_ord_entry IN SELECT value FROM jsonb_array_elements(p_order) LOOP
            v_alias := v_ord_entry->>'field';
            IF v_alias IS NULL OR v_alias = '' THEN
                RAISE EXCEPTION 'pvt_build_array_groupby_sql: order entry missing "field"';
            END IF;
            v_ord_parts := v_ord_parts || (
                quote_ident(v_alias) ||
                CASE WHEN COALESCE((v_ord_entry->>'asc')::boolean, true) THEN '' ELSE ' DESC' END);
        END LOOP;
        v_paging := E'\nORDER BY ' || array_to_string(v_ord_parts, ', ');
    END IF;

    IF p_limit IS NOT NULL AND p_limit >= 0 THEN
        v_paging := v_paging || E'\nLIMIT ' || p_limit::text;
    END IF;
    IF COALESCE(p_offset, 0) > 0 THEN
        v_paging := v_paging || E'\nOFFSET ' || p_offset::text;
    END IF;

    -- ---- Assemble
    v_inner_sql := format(
        E'SELECT %s\nFROM _values arr\nJOIN _objects o ON o._id = arr._id_object\n%s\nWHERE o._id_scheme = %s AND arr._id_structure = %s AND arr._array_index IS NOT NULL%s',
        array_to_string(v_inner_select, ', '),
        array_to_string(v_join_parts, E'\n'),
        p_scheme_id::text,
        v_arr_sid::text,
        v_filter_clause);

    RETURN format(
        E'SELECT %s\nFROM (\n%s\n) elements\nGROUP BY %s%s%s',
        array_to_string(v_select_parts, ', '),
        v_inner_sql,
        array_to_string(v_group_parts, ', '),
        v_having_sql,
        v_paging);
END;
$BODY$;

COMMENT ON FUNCTION pvt_build_array_groupby_sql(bigint, text, jsonb, jsonb, jsonb, jsonb, jsonb, integer, integer) IS
'Builds a GROUP BY query over array elements. Element fields are projected into an inner subquery via LEFT JOINs on _values keyed by _array_parent_id; the outer query groups by inner aliases and applies HAVING through pvt_build_bool_expr.';


