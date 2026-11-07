-- =====================================================
-- REDB Pro: Таблица истории миграций (SQL Server)
-- =====================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '_migrations')
BEGIN
    CREATE TABLE _migrations (
        -- No IDENTITY: like every other redb table, the id is supplied by the application
        -- (NextObjectIdAsync) and MigrationExecutor passes it explicitly. IDENTITY made that
        -- INSERT fail with "Cannot insert explicit value for identity column ... IDENTITY_INSERT
        -- is OFF", so migration history could never be written on MSSql.
        _id BIGINT NOT NULL PRIMARY KEY,
        _migration_id NVARCHAR(500) NOT NULL,                   -- уникальный ID миграции "OrderProps_TotalPrice_v1"
        _scheme_id BIGINT NOT NULL REFERENCES _schemes(_id) ON DELETE CASCADE,
        _structure_id BIGINT REFERENCES _structures(_id),       -- NULL = вся схема (ON DELETE SET NULL not supported with CASCADE on same table)
        _property_name NVARCHAR(500),                           -- имя свойства (для логов)
        _expression_hash NVARCHAR(500),                         -- MD5 от Expression для детекции изменений
        _migration_type NVARCHAR(200) NOT NULL,                 -- ComputedFrom, TypeChange, DefaultValue, Transform
        _applied_at DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
        _applied_by NVARCHAR(500),                              -- кто применил (user/system)
        _sql_executed NVARCHAR(MAX),                            -- SQL который был выполнен (для аудита)
        _affected_rows INT,                                     -- сколько записей затронуто
        _duration_ms INT,                                       -- время выполнения
        _dry_run BIT NOT NULL DEFAULT 0,                        -- это был dry-run?
        
        CONSTRAINT uq_migration_scheme UNIQUE(_scheme_id, _migration_id)
    );
END;

-- Индексы для быстрого поиска
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_migrations_scheme')
    CREATE INDEX idx_migrations_scheme ON _migrations(_scheme_id);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_migrations_applied')
    CREATE INDEX idx_migrations_applied ON _migrations(_applied_at DESC);
GO