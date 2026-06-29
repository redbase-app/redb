-- =====================================================================
-- 99_smoke_tests.sql
-- ---------------------------------------------------------------------
-- Ручные smoke-тесты v2-pvt. Все SELECT'ы закомментированы — файл
-- безопасно деплоить как есть. Скопируй нужный сниппет в psql и
-- замени плейсхолдеры <SCHEME_ID> / <PARENT_ID> / <ROOT_ID> на
-- реальные id из своей БД.
--
-- НОВАЯ АРХИТЕКТУРА (v0.1.0):
--   * SQL-сторона отдаёт ТОЛЬКО строку SQL (pvt_build_query_sql).
--   * Никакого RETURN NEXT / jsonb_agg внутри pvt-функций нет.
--   * Материализацию (get_object_json, COUNT, EXISTS, projection)
--     накладывает клиентский слой (C# / Python / Go).
--   * Здесь демонстрируются оба round-trip'а — фаза build и фаза exec.
-- =====================================================================

/*
-- 0. Module version check ----------------------------------------------
SELECT pvt_module_version();

-- 1. Фаза build — простой $eq на скалярном поле -----------------------
SELECT pvt_build_query_sql(
    p_scheme_id => <SCHEME_ID>,
    p_filter    => jsonb_build_object('Name', jsonb_build_object('$eq', 'Acme'))
);

-- 2. Range + IN + sort + paging — фаза build ---------------------------
SELECT pvt_build_query_sql(
    p_scheme_id => <SCHEME_ID>,
    p_filter    => '{
        "$and": [
            { "Age":    { "$gte": 18, "$lte": 65 } },
            { "Status": { "$in":  ["active","trial"] } }
        ]
    }'::jsonb,
    p_order     => '[{"field":"Age","dir":"desc","nulls":"last"}]'::jsonb,
    p_limit     => 50,
    p_offset    => 0
);

-- 3. NULL-check — выбирает LEFT JOIN внутри CTE ------------------------
SELECT pvt_build_query_sql(
    p_scheme_id => <SCHEME_ID>,
    p_filter    => '{ "MiddleName": { "$null": true } }'::jsonb
);

-- 4. Логические $or + $not --------------------------------------------
SELECT pvt_build_query_sql(
    p_scheme_id => <SCHEME_ID>,
    p_filter    => '{
        "$or": [
            { "Name": { "$ilike": "%foo%" } },
            { "$not": { "Age": { "$lt": 10 } } }
        ]
    }'::jsonb
);

-- 5. Tree mode (descendant scan) --------------------------------------
SELECT pvt_build_query_sql(
    p_scheme_id   => <SCHEME_ID>,
    p_filter      => NULL,
    p_source_mode => 'tree',
    p_tree_ids    => ARRAY[<ROOT_ID>]::bigint[],
    p_max_depth   => 5
);

-- 6. Фаза execute — full materialization (имитация C#-обёртки) --------
DO $$
DECLARE
    v_inner   text;
    v_wrapped text;
    v_row     jsonb;
BEGIN
    v_inner := pvt_build_query_sql(
        <SCHEME_ID>,
        jsonb_build_object('Name', jsonb_build_object('$startsWith', 'A')),
        5, 0, NULL, NULL, false, 'flat', NULL
    );
    v_wrapped := 'SELECT get_object_json(t._id, 3) FROM (' || v_inner || ') t';
    RAISE NOTICE 'wrapped SQL: %', v_wrapped;
    FOR v_row IN EXECUTE v_wrapped LOOP
        RAISE NOTICE 'row: %', v_row;
    END LOOP;
END $$;

-- 7. COUNT-обёртка (то же, что C# делает через ExecuteScalarAsync<long>)
WITH inner_sql AS (
    SELECT pvt_build_query_sql(
        <SCHEME_ID>,
        jsonb_build_object('Status', jsonb_build_object('$eq', 'active')),
        NULL, 0, NULL, NULL, false, 'flat', NULL
    ) AS sql_text
)
SELECT 'SELECT count(*) FROM (' || sql_text || ') t' AS count_sql
FROM inner_sql;

-- 8. EXISTS-обёртка ---------------------------------------------------
WITH inner_sql AS (
    SELECT pvt_build_query_sql(
        <SCHEME_ID>,
        jsonb_build_object('Email', jsonb_build_object('$eq', 'a@b.c')),
        1, 0, NULL, NULL, false, 'flat', NULL
    ) AS sql_text
)
SELECT 'SELECT EXISTS (' || sql_text || ')' AS exists_sql
FROM inner_sql;
*/

DO $$
BEGIN
    RAISE NOTICE 'v2-pvt smoke-tests file loaded (all statements are commented out).';
END $$;
