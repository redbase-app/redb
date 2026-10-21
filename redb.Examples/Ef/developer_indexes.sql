-- Indexes an EF Core developer adds by hand for the queries of the EF twins (ExamplesEf/*_ef.cs).
--
-- The flat schema from `redb flat --ddl-only` already has the keys, the unique constraints and an index on
-- every foreign key (EF's own convention). What it lacks are indexes on the columns the queries filter and
-- sort by: those an EF application chooses for its own queries, while redb's indexes come out of the box.
--
-- E203_ef drops them before the seed and creates them after it, inside the measured time, as E203 sets redb's
-- plain indexes aside and rebuilds them. One statement per line; lines starting with -- are skipped.

-- Trigram matching for Contains / ILIKE '%...%' (E030, E032).
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Filters and sorting on the employee itself.
CREATE INDEX "IX_EmployeeProps_Salary" ON "EmployeeProps" ("Salary");
CREATE INDEX "IX_EmployeeProps_Age" ON "EmployeeProps" ("Age");
CREATE INDEX "IX_EmployeeProps_Department" ON "EmployeeProps" ("Department");
CREATE INDEX "IX_EmployeeProps_Position" ON "EmployeeProps" ("Position");
CREATE INDEX "IX_EmployeeProps_HireDate" ON "EmployeeProps" ("HireDate");
CREATE INDEX "IX_EmployeeProps_LastName" ON "EmployeeProps" ("LastName");
CREATE INDEX "IX_EmployeeProps_LastName_trgm" ON "EmployeeProps" USING gin ("LastName" gin_trgm_ops);
-- StartsWith (E031): LIKE 'John%' uses a b-tree only with text_pattern_ops under a non-C collation.
CREATE INDEX "IX_EmployeeProps_FirstName_pattern" ON "EmployeeProps" ("FirstName" text_pattern_ops);

-- Filters on nested classes.
CREATE INDEX "IX_EmployeeProps.HomeAddress_City" ON "EmployeeProps.HomeAddress" ("City");
CREATE INDEX "IX_EmployeeProps.HomeAddress.Building_Floor" ON "EmployeeProps.HomeAddress.Building" ("Floor");

-- Filters on collections: by value first, the owner after it.
CREATE INDEX "IX_EmployeeProps.Skills_value_owner" ON "EmployeeProps.Skills" ("_value", "_owner_id");
CREATE INDEX "IX_EmployeeProps.PhoneDirectory_key_owner" ON "EmployeeProps.PhoneDirectory" ("_key", "_owner_id");
CREATE INDEX "IX_EmployeeProps.BonusByYear_key_value" ON "EmployeeProps.BonusByYear" ("_key", "_value");
CREATE INDEX "IX_EmployeeProps.OfficeLocations_key_City" ON "EmployeeProps.OfficeLocations" ("_key", "City");
CREATE INDEX "IX_EmployeeProps.PerformanceReviews_key_value" ON "EmployeeProps.PerformanceReviews" ("_key_Item1", "_key_Item2", "_value");
