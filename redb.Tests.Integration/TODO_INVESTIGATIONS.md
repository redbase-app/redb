# Исследование проблем интеграционных тестов redb

Все найденные баги исправлены и перенесены в CHANGELOG.md (v1.2.15).

---

## Открыто: E142c и E142d зависят от накопленных строк в общей БД

**Найдено:** 2026-08-24, при полном прогоне после правок семантики даты.
**Затрагивает:** `Tests/Base/BugRegressionTestsBase.cs`, тесты `E142c_GroupByArray_WithOuterPropsWhere_GroupsMatchEmployees` и `E142d_GroupByArray_OuterWhereAndHaving_Combined`.
**Это дефект набора тестов, а не продукта.**

`SeedEmployeesAsync` это просто `TestDataFactory.SeedEmployees(Redb, 20)` без всякой очистки (`BugRegressionTestsBase.cs:35`). Сотрудников сеют ещё и `WhereTestsBase`, `GroupByTestsBase`, `AggregationTestsBase`, `OrderingTestsBase` и другие, а Free и Pro фикстуры делят одну базу на каждом провайдере. Строки копятся из прогона в прогон.

Оба теста сравнивают счётчик группы со счётчиком **всех** подходящих сотрудников:

```csharp
var matched = (await Redb.Query<EmployeeProps>().Where(e => e.IsRemote == true).ToListAsync()).Count;
...
results.Should().AllSatisfy(r => r.Count.Should().Be(matched));
```

Равенство держится, пока набор строк совпадает с ожидаемым, и рвётся после некоторого порога накопления.

### Воспроизведение и подтверждение

| Условие | Итог |
|---|---|
| Полный прогон, в базе накопилось 703 строки `EmployeeProps` | E142d падает |
| Только класс `PostgresBugRegressionTests`, те же 703 строки | падают E142c и E142d |
| Только `E142d`, изолированно | проходит |
| Только класс, **после** `DELETE FROM _objects` по схеме `EmployeeProps` | **12/12 проходит** |
| Тот же класс на дереве **до** правок семантики даты (отдельный worktree, HEAD) | E142d падает так же |

Последняя строка важна: падение воспроизводится на неисправленном коде, то есть к правкам даты отношения не имеет. Правки не касаются ни bool-фильтров, ни `GroupByArray`, ни `Count`.

### Что надо сделать

Изолировать сидинг: удалять объекты схемы перед посевом, как это делает `TemporalRoundTripTestsBase.EnsureSeededAsync`. Грабли при этом: имя схемы в БД это **CLR FullName**, позиционный аргумент `[RedbScheme("...")]` задаёт **алиас**, а не имя. Удаление по алиасу молча не находит ничего.

```csharp
await Redb.Context.ExecuteAsync(
    "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = "
    + $"'{typeof(EmployeeProps).FullName}')");
```

Альтернатива поменьше: заменить сравнение с глобальным счётчиком на сравнение с числом строк, посеянных именно этим тестом.

---

## Открыто: SQLite-наборы требуют собранного нативного расширения

Фикстура ищет `redb.SQLite/native/build/redbsqlite.{dll,so,dylib}`, поднимаясь вверх по дереву. В свежем `git worktree` этого файла нет, и все SQLite Free-наборы падают по среде, а не по коду. Стоит иметь в виду при сравнительных прогонах: результаты SQLite из worktree нерепрезентативны, пока расширение туда не собрано или не скопировано.
