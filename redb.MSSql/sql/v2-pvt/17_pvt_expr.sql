-- =====================================================================
-- 17_pvt_expr.sql  (MSSql v2-pvt) — expression-form pushdown classifier
-- ---------------------------------------------------------------------
-- Port of PG's `pvt_expr_field_names` / `pvt_expr_is_base_only`
-- (see redb.Postgres/sql/v2-pvt/17_pvt_expr.sql), tailored for T-SQL
-- scalar UDF constraints.
--
-- Unlike PG we DO NOT reintroduce a separate scalar-expression compiler
-- here. T-SQL already has the proven `pvt_b2_expr_sql` (file 14) and
-- `pvt_build_where_from_json` (file 14) which together cover the same
-- scalar + predicate surface used by Pro/PG. The split optimizer
-- (file 16) reuses them to emit the pushed-down SQL once
-- pvt_expr_is_base_only confirms every `$field` reference resolves to
-- a base column.
--
-- Function:
--   dbo.pvt_expr_is_base_only(@node, @fields) RETURNS BIT
--     1 iff every "$field" reference inside @node has metadata with
--     kind='base' in @fields. NULL / non-JSON nodes are vacuously base.
--     Returns 0 if a referenced field has no metadata at all (so a
--     mistyped $field name does not silently get pushed down).
--
-- Walker rules:
--   * Recurses into every object / array value via OPENJSON.
--   * Treats a `$const` key as opaque (its value is a literal and may
--     contain strings shaped like field names — must not be walked).
--   * Treats a `$field` STRING value as a field reference.
--
-- Limitations vs the PG walker:
--   * Does not special-case unit-string literals of $cast / $dateAdd /
--     $dateDiff / $dateTrunc / $fts.language. In practice those are
--     plain JSON strings (type=1 in OPENJSON), never objects with a
--     `$field` key, so the naive walk does not generate false positives.
-- =====================================================================

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER FUNCTION dbo.pvt_expr_is_base_only(
    @node    NVARCHAR(MAX),
    @fields  NVARCHAR(MAX)
)
RETURNS BIT
AS
BEGIN
    -- Vacuously base: NULL / non-JSON / bare scalar literal.
    IF @node IS NULL RETURN 1;
    IF ISJSON(@node) = 0 RETURN 1;

    DECLARE @k NVARCHAR(400), @v NVARCHAR(MAX), @t INT;
    DECLARE @lk NVARCHAR(50);
    DECLARE @meta NVARCHAR(MAX);
    DECLARE @kind NVARCHAR(50);

    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT [key], [value], [type] FROM OPENJSON(@node);
    OPEN c;
    FETCH NEXT FROM c INTO @k, @v, @t;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @lk = LOWER(@k);

        -- $const subtree: opaque literal, never walked.
        IF @lk = N'$const'
        BEGIN
            FETCH NEXT FROM c INTO @k, @v, @t;
            CONTINUE;
        END;

        -- $field leaf: must resolve to kind='base'.
        IF @lk = N'$field' AND @t = 1
        BEGIN
            SET @meta = JSON_QUERY(@fields, N'$."' + STRING_ESCAPE(@v, 'json') + N'"');
            IF @meta IS NULL
            BEGIN
                CLOSE c; DEALLOCATE c;
                RETURN 0;
            END;
            SET @kind = JSON_VALUE(@meta, N'$.kind');
            IF @kind IS NULL OR @kind <> N'base'
            BEGIN
                CLOSE c; DEALLOCATE c;
                RETURN 0;
            END;
        END
        -- Object / array value: recurse.
        ELSE IF @t IN (4, 5)
        BEGIN
            IF dbo.pvt_expr_is_base_only(@v, @fields) = 0
            BEGIN
                CLOSE c; DEALLOCATE c;
                RETURN 0;
            END;
        END;
        -- String / number / bool / null leaves not under $field: ignore.

        FETCH NEXT FROM c INTO @k, @v, @t;
    END;
    CLOSE c; DEALLOCATE c;
    RETURN 1;
END;
GO
