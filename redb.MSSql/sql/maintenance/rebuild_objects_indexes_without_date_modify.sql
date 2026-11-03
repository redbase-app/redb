-- =====================================================================
-- REDB (SQL Server): bring three _objects indexes of an EXISTING database to the current schema
-- =====================================================================
-- redbMSSQL.sql no longer puts _date_modify into INCLUDE of these indexes. That file runs only on a
-- new database, so an existing one keeps the old definitions until this file is run.
--
-- Why it matters: _date_modify changes on every save, so an index that carries it is rewritten for
-- every saved object. On PostgreSQL, with frequent saves, these three had grown to 700-1400 bytes per
-- entry and shrank 10-20 times when rebuilt without the column.
--
-- Nothing runs this file automatically. Run it yourself, once per database:
--   sqlcmd -S <server> -d <database> -i rebuild_objects_indexes_without_date_modify.sql
--
-- Each index is rebuilt in place under its own name (DROP_EXISTING = ON); an index that is missing is
-- created. Without ONLINE = ON writes to _objects wait while an index is rebuilt. ONLINE = ON is
-- available on Enterprise and Developer editions and on Azure SQL: add it to the WITH clauses there.
-- Running the file again only pays for one more rebuild.
-- =====================================================================

-- Filtered indexes need these; sqlcmd starts with QUOTED_IDENTIFIER OFF.
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
SET NOCOUNT ON;
GO

-- 1. Descendants of an object
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'IX__objects__parent_id_descendant_lookup')
    CREATE INDEX [IX__objects__parent_id_descendant_lookup] ON [dbo].[_objects]([_id_parent], [_id_scheme])
        INCLUDE ([_id], [_id_owner], [_date_create]) WHERE [_id_parent] IS NOT NULL
        WITH (DROP_EXISTING = ON);
ELSE
    CREATE INDEX [IX__objects__parent_id_descendant_lookup] ON [dbo].[_objects]([_id_parent], [_id_scheme])
        INCLUDE ([_id], [_id_owner], [_date_create]) WHERE [_id_parent] IS NOT NULL;
GO

-- 2. Tree queries sorted by creation date
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'IX__objects__scheme_parent_date_create')
    CREATE INDEX [IX__objects__scheme_parent_date_create] ON [dbo].[_objects]([_id_scheme], [_id_parent], [_date_create] DESC)
        INCLUDE ([_id], [_id_owner])
        WITH (DROP_EXISTING = ON);
ELSE
    CREATE INDEX [IX__objects__scheme_parent_date_create] ON [dbo].[_objects]([_id_scheme], [_id_parent], [_date_create] DESC)
        INCLUDE ([_id], [_id_owner]);
GO

-- 3. Tree queries filtered by owner or name
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo._objects') AND name = N'IX__objects__scheme_parent_owner')
    CREATE INDEX [IX__objects__scheme_parent_owner] ON [dbo].[_objects]([_id_scheme], [_id_parent])
        INCLUDE ([_id], [_id_owner], [_date_create], [_name]) WHERE [_id_parent] IS NOT NULL
        WITH (DROP_EXISTING = ON);
ELSE
    CREATE INDEX [IX__objects__scheme_parent_owner] ON [dbo].[_objects]([_id_scheme], [_id_parent])
        INCLUDE ([_id], [_id_owner], [_date_create], [_name]) WHERE [_id_parent] IS NOT NULL;
GO

UPDATE STATISTICS [dbo].[_objects];
GO
