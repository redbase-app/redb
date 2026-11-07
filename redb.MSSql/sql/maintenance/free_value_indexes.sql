-- =====================================================================
-- REDB (SQL Server): value indexes for the FREE provider
-- =====================================================================
-- The PVT module 0.2.30 replaced eleven object-leading value indexes (_id_structure, _id_object, <value>)
-- with value-leading ones (_id_structure, <value>, _id_object). The new ones serve a filter on a field;
-- the old ones served reading the value of a known object.
--
-- This file brings them back under names of their own (IX__values__free_*). The module drops the old
-- names again on every change of its version; it neither drops nor checks these, so they survive
-- upgrades. Pro with the planner on (the default) does not need them, do not run the file there. Pro with
-- EnablePvtPrefilter = false reads the same forms as the free provider and may gain from them as well.
--
-- Eight indexes, one per value column, and one over the children of a collection element (below). Left out
-- on purpose:
--   IX__values__{Long,DateTimeOffset,Numeric}_not_null - the same definition as the *_filter index of
--       the same column, so each was a duplicate;
--   the string - NVARCHAR(MAX) cannot be an index key;
--   the seven indexes the module 0.2.29 dropped as duplicates: the leading columns of each are the
--       leading columns of an index or UNIQUE constraint of the same table that stays.
-- IX__values__structure_object_lookup was never dropped on SQL Server and stays in the schema.
--
-- What was measured: on the examples database (100 613 objects, 8.46 million values) the eight
-- *_by_object indexes gave no gain. Free queries there read IX__values__object_structure_lookup, which
-- covers every value column, and over a whole run of the examples one of the eight was read 16 times and
-- the others never. They are kept because they restore the indexes the free provider had before the
-- module retired them, for databases whose queries differ from the examples. Only
-- IX__values__free_parent_children has a measured gain (below).
--
-- Price: every index here is updated on each save of a value of its column. Run the file only if free
-- queries got slower, and keep an index only if its usage counters show reads.
--
-- Nothing runs this file automatically. Run it yourself, once per database:
--   sqlcmd -S <server> -d <database> -i free_value_indexes.sql
-- Without ONLINE = ON writes to _values wait while an index is built. ONLINE = ON is available on
-- Enterprise and Developer editions and on Azure SQL: add WITH (ONLINE = ON) to each CREATE there.
-- Running it again is safe: an existing index is kept.
--
-- To remove them:
--   DROP INDEX IF EXISTS [IX__values__free_long_by_object] ON [dbo].[_values]; (and so on for each name below)
-- =====================================================================

-- Filtered indexes need these; sqlcmd starts with QUOTED_IDENTIFIER OFF.
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_long_by_object')
    CREATE INDEX [IX__values__free_long_by_object] ON [dbo].[_values]([_id_structure], [_id_object], [_Long]) WHERE [_Long] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_guid_by_object')
    CREATE INDEX [IX__values__free_guid_by_object] ON [dbo].[_values]([_id_structure], [_id_object], [_Guid]) WHERE [_Guid] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_double_by_object')
    CREATE INDEX [IX__values__free_double_by_object] ON [dbo].[_values]([_id_structure], [_id_object], [_Double]) WHERE [_Double] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_datetimeoffset_by_object')
    CREATE INDEX [IX__values__free_datetimeoffset_by_object] ON [dbo].[_values]([_id_structure], [_id_object], [_DateTimeOffset]) WHERE [_DateTimeOffset] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_boolean_by_object')
    CREATE INDEX [IX__values__free_boolean_by_object] ON [dbo].[_values]([_id_structure], [_id_object], [_Boolean]) WHERE [_Boolean] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_numeric_by_object')
    CREATE INDEX [IX__values__free_numeric_by_object] ON [dbo].[_values]([_id_structure], [_id_object], [_Numeric]) WHERE [_Numeric] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_listitem_by_object')
    CREATE INDEX [IX__values__free_listitem_by_object] ON [dbo].[_values]([_id_structure], [_id_object], [_ListItem]) WHERE [_ListItem] IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_object_by_object')
    CREATE INDEX [IX__values__free_object_by_object] ON [dbo].[_values]([_id_structure], [_id_object], [_Object]) WHERE [_Object] IS NOT NULL;
GO

-- Children of a collection element with their field and string value. A field of a class inside a dictionary
-- or an array (Dict[key].Field) is joined to its element by _array_parent_id alone, and the field is picked
-- afterwards; no other index carries _id_structure, _array_index and _String by parent, so SQL Server read the
-- whole _values instead. Example E062 (Dictionary, nested class property), 100 613 objects, 8.46 million
-- values: 13.4 s -> 1.9 s. Holds every child value of every collection element, the string included, so it
-- is large where collections of classes carry long strings.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._values') AND name = N'IX__values__free_parent_children')
    CREATE INDEX [IX__values__free_parent_children] ON [dbo].[_values]([_array_parent_id])
        INCLUDE ([_id_structure], [_array_index], [_String])
        WHERE [_array_parent_id] IS NOT NULL;
GO

UPDATE STATISTICS [dbo].[_values];
GO
