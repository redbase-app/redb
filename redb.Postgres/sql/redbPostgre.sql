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
	_login text NOT NULL,
	_password TEXT NOT NULL,
	_name text NOT NULL UNIQUE,
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
CREATE INDEX IF NOT EXISTS "IX__values__structure_parent_batch" 
ON _values (_id_structure, _array_parent_id) 
INCLUDE (_String, _Long, _Double, _Boolean, _Guid)
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
CREATE INDEX IF NOT EXISTS "IX__values__object_structure_lookup" 
ON _values (_id_object, _id_structure, _array_index) 
INCLUDE (_String, _Long, _Double, _DateTimeOffset, _Boolean, _Guid, _Numeric, _ListItem, _Object)
WITH (deduplicate_items=True);

-- 🚀 Covering index for nested fields (Class)
-- Index Only Scan for JOIN nested fields and ORDER BY subqueries
-- INCLUDE eliminates table access for reading values
CREATE INDEX IF NOT EXISTS "IX__values__object_array_null" 
ON _values (_id_object, _id_structure) 
INCLUDE (_String, _Long, _Double, _DateTimeOffset, _Boolean, _Guid, _Numeric, _ListItem, _Object)
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
CREATE INDEX IF NOT EXISTS "IX__values__structure_object_lookup" 
ON _values (_id_structure, _id_object, _String, _Long, _DateTimeOffset, _Boolean, _Double, _Guid, _Numeric, _ListItem, _Object);

-- 2. 🌳 Tree queries
CREATE INDEX IF NOT EXISTS "IX__objects__scheme_parent" 
ON _objects (_id_scheme, _id_parent, _id);

-- 3. 🔢 Arrays
CREATE INDEX IF NOT EXISTS "IX__values__object_structure_array_index" 
ON _values (_id_object, _id_structure, _array_index)
WHERE _array_index IS NOT NULL;

-- 4. 📌 Partial indexes for NOT NULL values
CREATE INDEX IF NOT EXISTS "IX__values__String_not_null" 
ON _values (_id_structure, _id_object, _String)
WHERE _String IS NOT NULL;

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
-- Prohibits deletion and renaming of users with ID 0 (sys) and 1
-- Other operations (password change, email, phone, status) allowed

-- Trigger function
CREATE OR REPLACE FUNCTION protect_system_users()
RETURNS TRIGGER AS $$
BEGIN
    -- Check DELETE operation
    IF TG_OP = 'DELETE' THEN
        -- Prohibit deletion of system users
        IF OLD._id IN (0, 1) THEN
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
        
        -- For system users prohibit changing login and name
        IF OLD._id IN (0, 1) THEN
            -- Prohibit changing login
            IF OLD._login != NEW._login THEN
                RAISE EXCEPTION 'Cannot change login of system user with ID %', OLD._id;
            END IF;
            
            -- Prohibit changing name
            IF OLD._name != NEW._name THEN
                RAISE EXCEPTION 'Cannot change name of system user with ID %', OLD._id;
            END IF;
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
COMMENT ON FUNCTION protect_system_users() IS 'Protects system users (ID 0, 1) from deletion and renaming';
COMMENT ON TRIGGER tr_protect_system_users ON _users IS 'Trigger protecting system users from deletion and renaming of login/name';


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