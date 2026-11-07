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
