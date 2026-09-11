using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Инварианты сохранения (ревью 2026-09-03, волна 1.2). Главный: повторный save без правок -
/// ноль диффа; точечная правка меняет только своё. На фикстурах с ChangeTracking
/// (<see cref="ExpectStableValueRowIds"/> = true) дополнительно проверяется, что строки _values
/// нетронутых полей сохраняют свои _id - Delete/Insert их пересоздаёт, CT обязан не трогать.
/// Тест канона хеша массива - пин под снос расходящегося пересчёта (CT-2): хеш базовой записи
/// массива после CT-правки элемента обязан совпадать с хешем свежесозданного объекта с тем же
/// содержимым.
/// </summary>
public abstract class CtSaveInvariantsTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected CtSaveInvariantsTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>true на фикстурах со стратегией ChangeTracking: строки нетронутых значений обязаны сохранять _id.</summary>
    protected virtual bool ExpectStableValueRowIds => false;

    public async Task InitializeAsync() => await ResetAsync();
    public async Task DisposeAsync() => await ResetAsync();

    private async Task ResetAsync()
    {
        // Порядок обязателен: FK fk__values__objects_ref запрещает удалять объект, на который
        // жива ссылка _values._Object. Сначала родители (их строки-ссылки уходят каскадом
        // вместе с values), потом дети.
        await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = 'CtProbe')");
        await Redb.Context.ExecuteAsync(
            "DELETE FROM _objects WHERE _id_scheme IN (SELECT _id FROM _schemes WHERE _name = 'CtProbeChild')");
    }

    private async Task<long> SeedAsync(CtProbeProps props)
    {
        await Redb.SyncSchemeAsync<CtProbeProps>();
        return await Redb.SaveAsync(new RedbObject<CtProbeProps> { name = "ct-probe", Props = props });
    }

    private sealed class ValueRow
    {
        public long Id { get; set; }
        public long IdStructure { get; set; }
        public string? ArrayIndex { get; set; }
    }

    private async Task<List<ValueRow>> SnapshotRowsAsync(long objectId)
        => await Redb.Context.QueryAsync<ValueRow>(
            $"SELECT _id AS Id, _id_structure AS IdStructure, _array_index AS ArrayIndex FROM _values WHERE _id_object = {objectId} ORDER BY _id");

    private async Task<Guid?> ArrayBaseHashAsync(long objectId, string fieldName)
        => await Redb.Context.ExecuteScalarAsync<Guid?>(
            "SELECT v._Guid FROM _values v JOIN _structures s ON s._id = v._id_structure " +
            $"WHERE v._id_object = {objectId} AND s._name = '{fieldName}' AND v._array_index IS NULL AND v._array_parent_id IS NULL");

    /// <summary>Ш4: после любой операции ресейв без правок обязан быть ноль-диффом
    /// (хеш стабилен; на CT-фикстурах - и _id строк значений).</summary>
    private async Task AssertResaveIsZeroDiffAsync(long id)
    {
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        var hash = loaded!.hash;
        var rows = await SnapshotRowsAsync(id);

        await Redb.SaveAsync(loaded);

        var after = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        after!.hash.Should().Be(hash, "ресейв без правок обязан быть ноль-диффом");
        if (ExpectStableValueRowIds)
            (await SnapshotRowsAsync(id)).Select(r => r.Id).Should().Equal(rows.Select(r => r.Id),
                "ресейв без правок не имеет права пересоздавать строки значений");
    }

    private static CtProbeProps FullProps() => new()
    {
        Label = "base",
        Longs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        Decimals = [1.5m, 2.5m, 3.5m],
        Dict = new() { ["a"] = "1", ["b"] = "2" },
        Items =
        [
            new CtProbeItem { Name = "i0", Price = 10m },
            new CtProbeItem { Name = "i1", Price = 20m },
            new CtProbeItem { Name = "i2", Price = 30m },
        ],
    };

    [Fact]
    public async Task ResaveWithoutChanges_IsAZeroDiff()
    {
        var id = await SeedAsync(FullProps());
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        var hashBefore = loaded!.hash;
        var rowsBefore = await SnapshotRowsAsync(id);

        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.hash.Should().Be(hashBefore, "содержимое не менялось - хеш обязан быть стабилен");
        reloaded.Props.Longs.Should().Equal(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        reloaded.Props.Dict.Should().Equal(new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" });

        if (ExpectStableValueRowIds)
        {
            var rowsAfter = await SnapshotRowsAsync(id);
            rowsAfter.Select(r => r.Id).Should().Equal(rowsBefore.Select(r => r.Id),
                "ChangeTracking при нулевом диффе не имеет права пересоздавать строки значений");
        }
    }

    [Fact]
    public async Task SingleElementEdit_InALongArray_TouchesOnlyItsRow()
    {
        // Массив из 12 элементов - индексы с двузначными номерами: строковая сортировка
        // индексов ("10" < "2") ломала бы порядок; численная - нет.
        var id = await SeedAsync(FullProps());
        var rowsBefore = await SnapshotRowsAsync(id);

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Longs![10] = 999;
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.Longs.Should().Equal(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 999, 12);

        if (ExpectStableValueRowIds)
        {
            var rowsAfter = await SnapshotRowsAsync(id);
            var untouchedBefore = rowsBefore.Where(r => r.ArrayIndex != "10").Select(r => r.Id);
            var untouchedAfter = rowsAfter.Where(r => r.ArrayIndex != "10").Select(r => r.Id);
            untouchedAfter.Should().BeEquivalentTo(untouchedBefore,
                "правка одного элемента не имеет права пересоздавать строки остальных");
        }
    }

    [Fact]
    public async Task ArrayBaseHash_AfterEdit_MatchesTheCanonicalHash()
    {
        // Пин под снос CT-2: после правки элемента хеш базовой записи массива обязан совпадать
        // с хешем свежесозданного объекта с тем же содержимым (канон - RedbHash.ComputeForProps
        // от CLR-массива, не пересчёт по строкам _values).
        var id = await SeedAsync(FullProps());
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Longs![10] = 999;
        loaded.Props.Decimals![1] = 9.75m;
        await Redb.SaveAsync(loaded);

        var canonId = await SeedAsync(new CtProbeProps
        {
            Label = "canon",
            Longs = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 999, 12],
            Decimals = [1.5m, 9.75m, 3.5m],
        });

        (await ArrayBaseHashAsync(id, "Longs")).Should().Be(await ArrayBaseHashAsync(canonId, "Longs"),
            "хеш массива после правки обязан быть каноническим");
        (await ArrayBaseHashAsync(id, "Decimals")).Should().Be(await ArrayBaseHashAsync(canonId, "Decimals"),
            "хеш decimal-массива после правки обязан быть каноническим");
    }


    [Fact]
    public async Task TupleKeyedDictionary_AddEditRemove_RoundTrips()
    {
        // Ш1 (стратегия владельца 2026-09-03): ключ словаря - ValueTuple, в _array_index он
        // лежит сериализованным (Base64(JSON), Item1..ItemN вручную). Пин детерминизма
        // сериализации ключа через полный цикл: правка значения по ключу, удаление ключа,
        // добавление нового, перечитка, ресейв без правок = ноль диффа.
        var id = await SeedAsync(new CtProbeProps
        {
            Label = "tuple",
            TupleDict = new Dictionary<(int Year, string Quarter), string>
            {
                [(2026, "Q1")] = "start",
                [(2026, "Q2")] = "mid",
            },
        });

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.TupleDict.Should().NotBeNull("словарь с tuple-ключом обязан перечитываться");
        loaded.Props.TupleDict![(2026, "Q1")] = "changed";
        loaded.Props.TupleDict.Remove((2026, "Q2"));
        loaded.Props.TupleDict[(2027, "Q1")] = "new-year";
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.TupleDict.Should().Equal(new Dictionary<(int Year, string Quarter), string>
        {
            [(2026, "Q1")] = "changed",
            [(2027, "Q1")] = "new-year",
        });

        // Ресейв без правок: хеш стабилен, на CT-фикстурах строки не пересозданы.
        var hashBefore = reloaded.hash;
        var rowsBefore = await SnapshotRowsAsync(id);
        await Redb.SaveAsync(reloaded);
        var after = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        after!.hash.Should().Be(hashBefore, "ресейв без правок не имеет права двигать хеш");
        if (ExpectStableValueRowIds)
        {
            var rowsAfter = await SnapshotRowsAsync(id);
            rowsAfter.Select(r => r.Id).Should().Equal(rowsBefore.Select(r => r.Id),
                "нулевой дифф tuple-словаря не имеет права пересоздавать строки");
        }
    }

    [Fact]
    public async Task Dictionary_EditRemoveAdd_RoundTrips()
    {
        var id = await SeedAsync(FullProps());
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Dict!["a"] = "changed";
        loaded.Props.Dict.Remove("b");
        loaded.Props.Dict["c"] = "new";
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.Dict.Should().Equal(new Dictionary<string, string>
        {
            ["a"] = "changed",
            ["c"] = "new",
        });
    }


    private async Task<RedbObject<CtProbeChildProps>> SeedChildAsync(string tag)
    {
        await Redb.SyncSchemeAsync<CtProbeChildProps>();
        var child = new RedbObject<CtProbeChildProps> { name = $"child-{tag}", Props = new CtProbeChildProps { Tag = tag } };
        await Redb.SaveAsync(child);
        return child;
    }

    private async Task<Guid?> ElementGuidAsync(long objectId, string fieldName, string arrayIndex)
        => await Redb.Context.ExecuteScalarAsync<Guid?>(
            "SELECT v._Guid FROM _values v JOIN _structures s ON s._id = v._id_structure " +
            $"WHERE v._id_object = {objectId} AND s._name = '{fieldName}' AND v._array_index = '{arrayIndex}'");

    [Fact]
    public async Task ReferenceArray_ReplaceOneElement_TouchesOnlyItsRow()
    {
        // Ш2: массив РЕФЕРЕНСОВ - в строках только _Object=id + _Guid=персистентный хеш ребёнка.
        // Замена одного элемента-ссылки не имеет права трогать строки соседей, а _Guid заменённой
        // строки обязан стать хешем НОВОГО ребёнка.
        var c1 = await SeedChildAsync("c1");
        var c2 = await SeedChildAsync("c2");
        var c3 = await SeedChildAsync("c3");
        var id = await SeedAsync(new CtProbeProps { Label = "refs", Refs = [c1, c2] });
        var rowsBefore = await SnapshotRowsAsync(id);

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Refs.Should().HaveCount(2);
        loaded.Props.Refs![1] = c3;
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.Refs!.Select(r => r.id).Should().Equal(c1.id, c3.id);

        (await ElementGuidAsync(id, "Refs", "1")).Should().Be(c3.hash,
            "строка-ссылка несёт персистентный хеш нового ребёнка");

        if (ExpectStableValueRowIds)
        {
            var rowsAfter = await SnapshotRowsAsync(id);
            var untouchedBefore = rowsBefore.Where(r => r.ArrayIndex != "1").Select(r => r.Id);
            var untouchedAfter = rowsAfter.Where(r => r.ArrayIndex != "1").Select(r => r.Id);
            untouchedAfter.Should().BeEquivalentTo(untouchedBefore,
                "замена одной ссылки не имеет права пересоздавать строки соседей");
        }
    }

    [Fact]
    public async Task ReferenceDictionary_ReplaceByKey_RoundTrips()
    {
        // Ш2: словарь референсов - ключ в _array_index, значение _Object=id + _Guid=хеш.
        var c1 = await SeedChildAsync("d1");
        var c2 = await SeedChildAsync("d2");
        var c3 = await SeedChildAsync("d3");
        var id = await SeedAsync(new CtProbeProps
        {
            Label = "refdict",
            RefDict = new Dictionary<string, RedbObject<CtProbeChildProps>> { ["a"] = c1, ["b"] = c2 },
        });
        var rowsBefore = await SnapshotRowsAsync(id);

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.RefDict.Should().NotBeNull();
        loaded.Props.RefDict!["b"] = c3;
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.RefDict!.Keys.Should().BeEquivalentTo(new[] { "a", "b" });
        reloaded.Props.RefDict["a"].id.Should().Be(c1.id);
        reloaded.Props.RefDict["b"].id.Should().Be(c3.id);

        (await ElementGuidAsync(id, "RefDict", "b")).Should().Be(c3.hash,
            "строка-ссылка словаря несёт персистентный хеш нового ребёнка");

        if (ExpectStableValueRowIds)
        {
            var rowsAfter = await SnapshotRowsAsync(id);
            var untouchedBefore = rowsBefore.Where(r => r.ArrayIndex != "b").Select(r => r.Id);
            var untouchedAfter = rowsAfter.Where(r => r.ArrayIndex != "b").Select(r => r.Id);
            untouchedAfter.Should().BeEquivalentTo(untouchedBefore,
                "замена значения по одному ключу не имеет права пересоздавать строки соседей");
        }
    }




    [Fact]
    public async Task NullElements_InPrimitiveArray_RoundTrip()
    {
        // Ш2б («null должен быть null»): null-элементы строкового массива переживают цикл,
        // правка соседа их не трогает, ресейв - ноль диффа. Red-before: Pro-читатель выкидывал
        // строки без значений (В-3).
        var id = await SeedAsync(new CtProbeProps { Label = "nulls", Tags = ["a", null, "b"] });

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Tags.Should().Equal("a", null, "b");

        loaded.Props.Tags![0] = "a2";
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.Tags.Should().Equal("a2", null, "b");

        var hashBefore = reloaded.hash;
        await Redb.SaveAsync(reloaded);
        (await Redb.LoadAsync<CtProbeProps>(id, depth: 2))!.hash.Should().Be(hashBefore,
            "null-элементы не имеют права дрожать между ресейвами");
    }

    [Fact]
    public async Task NullValue_InDictionary_RoundTrips()
    {
        // В-2 («null должен быть null»): ключ с null-значением обязан сохраниться при чтении.
        var id = await SeedAsync(new CtProbeProps
        {
            Label = "dict-null",
            Dict = new() { ["a"] = "1", ["b"] = null! },
        });

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Dict.Should().ContainKey("b");
        loaded.Props.Dict!["b"].Should().BeNull("ключ с null-значением обязан вернуться ключом с null");

        loaded.Props.Dict["a"] = "2";
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.Dict.Should().HaveCount(2);
        reloaded.Props.Dict!["a"].Should().Be("2");
        reloaded.Props.Dict["b"].Should().BeNull();
    }


    [Fact]
    public async Task NullElement_InClassList_RoundTrips()
    {
        // В-1 («null должен быть null»): null-элемент списка КЛАССОВ обязан вернуться null-ом.
        // Дифференциатор в базе: null-элемент - строка без _Guid; у класса хеш есть всегда.
        var id = await SeedAsync(new CtProbeProps
        {
            Label = "class-nulls",
            Items =
            [
                new CtProbeItem { Name = "i0", Price = 10m },
                null!,
                new CtProbeItem { Name = "i2", Price = 30m },
            ],
        });

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Items.Should().HaveCount(3);
        loaded.Props.Items![1].Should().BeNull("null-элемент списка классов обязан вернуться null-ом");

        loaded.Props.Items[2] = new CtProbeItem { Name = "i2", Price = 31m };
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.Items.Should().HaveCount(3);
        reloaded.Props.Items![1].Should().BeNull();
        reloaded.Props.Items[2]!.Price.Should().Be(31m);
    }


    [Fact]
    public async Task LegacyClassElement_WithoutGuid_StillReadsAsObject()
    {
        // Р-1 (ревью null-фиксов): дифференциатор «null-элемент = строка без _Guid» усилен
        // legacy-толерантностью «...И без детей». Если исторический писатель не ставил хеш
        // класс-элементу, но дети есть - это КЛАСС, и его данные обязаны читаться, а не
        // превращаться в null. У настоящего null-элемента детей не бывает никогда.
        var id = await SeedAsync(new CtProbeProps
        {
            Label = "legacy",
            Items = [new CtProbeItem { Name = "keep-me", Price = 7m }],
        });

        // Имитация legacy-строки: снести хеш у класс-элемента (дети остаются).
        await Redb.Context.ExecuteAsync(
            "UPDATE _values SET _Guid = NULL WHERE _id_object = " + id +
            " AND _array_index IS NOT NULL AND _id_structure IN (SELECT _id FROM _structures WHERE _name = 'Items')");

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Items.Should().HaveCount(1);
        loaded.Props.Items![0].Should().NotBeNull("класс-элемент с детьми обязан читаться объектом даже без legacy-хеша");
        loaded.Props.Items[0]!.Name.Should().Be("keep-me");
    }

    [Fact]
    public async Task NestedClassInObjectArray_Edit_RoundTrips()
    {
        var id = await SeedAsync(FullProps());
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Items![1].Price = 21.5m;
        await Redb.SaveAsync(loaded);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        reloaded!.Props.Items!.Select(i => i.Price).Should().Equal(10m, 21.5m, 30m);
        reloaded.Props.Items.Select(i => i.Name).Should().Equal("i0", "i1", "i2");
    }

    [Fact]
    public async Task ValuelessObject_Load_TerminatesAndReturnsObject()
    {
        // Объект без единой value-строки (properties:null в json) - легальное состояние:
        // чужой процесс успел стереть values (гонка), или объект создан голым. Free-путь
        // одиночной ленивой загрузки на таком объекте уходил в вечную рекурсию
        // get_Props -> LoadProps -> LoadPropsAsync -> return obj.Props (петля съедала по
        // треду пула на виток). Загрузка обязана завершиться, а не жевать пул.
        var id = await SeedAsync(new CtProbeProps { Label = "soon-valueless" });
        await Redb.Context.ExecuteAsync("DELETE FROM _values WHERE _id_object = " + id);

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded.Should().NotBeNull();
        loaded!.id.Should().Be(id);

        // Петля живёт не в LoadAsync, а в СИНХРОННОМ геттере Props стаба (Install повесил
        // лоадер на корень) - гвардим само обращение, иначе красный прогон висит вечно.
        var propsTask = Task.Run(() => loaded.GetPropsDirectly() ?? loaded.Props);
        var winner = await Task.WhenAny(propsTask, Task.Delay(TimeSpan.FromSeconds(30)));
        winner.Should().BeSameAs(propsTask, "доступ к Props объекта без values обязан завершаться, а не зацикливаться");
    }

    [Fact]
    public async Task ListItemRename_DoesNotMoveOwnerHash_AndResaveIsAZeroDiff()
    {
        // О-1/Б-3 (вердикт владельца): в _values живёт ИДЕНТИФИКАТОР элемента справочника,
        // хеш владельца - от идентификаторов; за содержимое ListItem values-слой не отвечает.
        // Переименование значения в _list_items не имеет права двигать хеш владельца и
        // порождать дифф при ресейве без правок.
        var listName = "CtProbeRoles";
        var existing = await Redb.ListProvider.GetListByNameAsync(listName);
        if (existing != null)
        {
            var stale = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            if (stale.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(stale.Select(i => i.Id).ToList());
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }
        var list = await Redb.ListProvider.SaveListAsync(RedbList.Create(listName, "ct-probe roles"));
        var items = await Redb.ListProvider.AddItemsAsync(list, ["Admin", "User", "Viewer"]);

        var id = await SeedAsync(new CtProbeProps
        {
            Label = "listitem",
            Status = items[0],
            Roles = [items[1], items[2]],
        });
        var hashBefore = (await Redb.LoadAsync<CtProbeProps>(id, depth: 2))!.hash;
        var rowsBefore = await SnapshotRowsAsync(id);

        // Переименование значения справочника - контент ListItem живёт своей жизнью.
        items[1].Value = "User-Renamed";
        await Redb.ListProvider.SaveListItemAsync(items[1]);

        var reloaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        await Redb.SaveAsync(reloaded!);

        var after = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        after!.hash.Should().Be(hashBefore,
            "хеш владельца считается от идентификаторов ListItem - переименование значения не двигает его");
        after.Props.Status!.Id.Should().Be(items[0].Id);
        after.Props.Roles!.Select(r => r.Id).Should().Equal(items[1].Id, items[2].Id);

        if (ExpectStableValueRowIds)
        {
            var rowsAfter = await SnapshotRowsAsync(id);
            rowsAfter.Select(r => r.Id).Should().Equal(rowsBefore.Select(r => r.Id),
                "ресейв без правок владельца обязан быть ноль-диффом и после переименования в справочнике");
        }
    }

    // ===== Ш4: жёсткая матрица вложенности (только тесты, Б-4) =====

    private static CtProbeProps NestedMatrixProps() => new()
    {
        Label = "matrix",
        Items =
        [
            new CtProbeItem
            {
                Name = "i0", Price = 10m,
                Codes = [100, 101, 102],
                Meta = new() { ["k0"] = "v0", ["k1"] = "v1" },
            },
            new CtProbeItem
            {
                Name = "i1", Price = 20m,
                Codes = [200, 201],
                Meta = new() { ["m0"] = "w0" },
            },
        ],
    };

    [Fact]
    public async Task NestedCollectionsInsideArrayElement_EditAtEachLevel_RoundTrips()
    {
        // Класс-элемент массива с СОБСТВЕННЫМИ массивом и словарём: правка на каждом
        // уровне вложенности, после каждой операции - перечитка и ноль-дифф ресейва.
        var id = await SeedAsync(NestedMatrixProps());
        await AssertResaveIsZeroDiffAsync(id);

        // Уровень 3: элемент вложенного массива класса-элемента.
        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 3);
        loaded!.Props.Items![0].Codes![1] = 999;
        await Redb.SaveAsync(loaded);
        var r1 = await Redb.LoadAsync<CtProbeProps>(id, depth: 3);
        r1!.Props.Items![0].Codes.Should().Equal(100, 999, 102);
        r1.Props.Items[1].Codes.Should().Equal(200, 201);
        await AssertResaveIsZeroDiffAsync(id);

        // Уровень 3: значение ключа вложенного словаря + правка поля самого класса.
        loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 3);
        loaded!.Props.Items![1].Meta!["m0"] = "w0-changed";
        loaded.Props.Items[1].Price = 21.5m;
        await Redb.SaveAsync(loaded);
        var r2 = await Redb.LoadAsync<CtProbeProps>(id, depth: 3);
        r2!.Props.Items![1].Meta.Should().Equal(new Dictionary<string, string> { ["m0"] = "w0-changed" });
        r2.Props.Items[1].Price.Should().Be(21.5m);
        r2.Props.Items[0].Meta.Should().Equal(new Dictionary<string, string> { ["k0"] = "v0", ["k1"] = "v1" });
        await AssertResaveIsZeroDiffAsync(id);

        // Add/delete на вложенных коллекциях: рост массива i1, усушка массива i0,
        // добавление и удаление ключей словаря i0 вперемешку.
        loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 3);
        loaded!.Props.Items![1].Codes = [200, 201, 202, 203];
        loaded.Props.Items[0].Codes = [100, 999];
        loaded.Props.Items[0].Meta!.Remove("k0");
        loaded.Props.Items[0].Meta["k2"] = "v2";
        await Redb.SaveAsync(loaded);
        var r3 = await Redb.LoadAsync<CtProbeProps>(id, depth: 3);
        r3!.Props.Items![1].Codes.Should().Equal(200, 201, 202, 203);
        r3.Props.Items[0].Codes.Should().Equal(100, 999);
        r3.Props.Items[0].Meta.Should().Equal(new Dictionary<string, string> { ["k1"] = "v1", ["k2"] = "v2" });
        await AssertResaveIsZeroDiffAsync(id);
    }

    [Fact]
    public async Task ArrayElementAddDelete_WithNestedCollections_RoundTrips()
    {
        // Add/delete самих класс-элементов (у каждого - собственные коллекции):
        // удаление элемента с детьми, добавление нового с детьми.
        var id = await SeedAsync(NestedMatrixProps());

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 3);
        loaded!.Props.Items!.RemoveAt(0); // i0 уходит вместе со своими Codes/Meta
        loaded.Props.Items.Add(new CtProbeItem
        {
            Name = "i2", Price = 30m,
            Codes = [300],
            Meta = new() { ["z"] = "zz" },
        });
        await Redb.SaveAsync(loaded);

        var r = await Redb.LoadAsync<CtProbeProps>(id, depth: 3);
        r!.Props.Items!.Select(i => i.Name).Should().Equal("i1", "i2");
        r.Props.Items[0].Codes.Should().Equal(200, 201);
        r.Props.Items[1].Codes.Should().Equal(300);
        r.Props.Items[1].Meta.Should().Equal(new Dictionary<string, string> { ["z"] = "zz" });
        await AssertResaveIsZeroDiffAsync(id);
    }

    [Fact]
    public async Task LargeArrayReorder_RoundTrips()
    {
        // Reorder большого массива примитивов и reorder класс-элементов: порядок - часть
        // содержимого, перечитка обязана вернуть новый порядок, ресейв после - ноль-дифф.
        var id = await SeedAsync(FullProps());

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Longs = [.. loaded.Props.Longs!.Reverse()];
        await Redb.SaveAsync(loaded);
        var r1 = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        r1!.Props.Longs.Should().Equal(12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1);
        await AssertResaveIsZeroDiffAsync(id);

        // Reorder класс-элементов: [i0,i1,i2] -> [i2,i0,i1].
        loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        var items = loaded!.Props.Items!;
        loaded.Props.Items = [items[2], items[0], items[1]];
        await Redb.SaveAsync(loaded);
        var r2 = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        r2!.Props.Items!.Select(i => i.Name).Should().Equal("i2", "i0", "i1");
        r2.Props.Items.Select(i => i.Price).Should().Equal(30m, 10m, 20m);
        await AssertResaveIsZeroDiffAsync(id);
    }

    [Fact]
    public async Task DictionaryKeysAddDeleteMixed_RoundTrips()
    {
        // Перемешанные add+delete ключей корневого словаря за один save: удаление,
        // добавление двух, правка выжившего.
        var id = await SeedAsync(FullProps());

        var loaded = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        loaded!.Props.Dict!.Remove("a");
        loaded.Props.Dict["c"] = "3";
        loaded.Props.Dict["d"] = "4";
        loaded.Props.Dict["b"] = "2-changed";
        await Redb.SaveAsync(loaded);

        var r = await Redb.LoadAsync<CtProbeProps>(id, depth: 2);
        r!.Props.Dict.Should().Equal(new Dictionary<string, string>
        {
            ["b"] = "2-changed",
            ["c"] = "3",
            ["d"] = "4",
        });
        await AssertResaveIsZeroDiffAsync(id);
    }

    [Fact]
    public async Task ArrayBaseHash_DependsOnElementContent_NotJustSize()
    {
        // Canon defect found while preparing F2: ComputeForProps(collection) reflected over the
        // collection type's OWN properties (List: Capacity|Count; long[]: Length|...), so two
        // same-sized collections with different content produced the SAME base-row hash. Any
        // consumer trusting the base hash (the F2 short-circuit above all) would then treat
        // different arrays as equal. The base hash must depend on element content.
        var idA = await SeedAsync(new CtProbeProps
        {
            Label = "canonA",
            Longs = [1, 2, 3, 4, 5],
            Items = [new CtProbeItem { Name = "x", Price = 1m }],
        });
        var idB = await SeedAsync(new CtProbeProps
        {
            Label = "canonB",
            Longs = [9, 9, 9, 9, 9],
            Items = [new CtProbeItem { Name = "y", Price = 2m }],
        });

        var longsA = await ArrayBaseHashAsync(idA, "Longs");
        var longsB = await ArrayBaseHashAsync(idB, "Longs");
        longsA.Should().NotBeNull();
        longsA.Should().NotBe(longsB!.Value,
            "same-length arrays with different content must not share a base hash");

        var itemsA = await ArrayBaseHashAsync(idA, "Items");
        var itemsB = await ArrayBaseHashAsync(idB, "Items");
        itemsA.Should().NotBeNull();
        itemsA.Should().NotBe(itemsB!.Value,
            "class arrays with different element content must not share a base hash");
    }

    [Fact]
    public async Task BatchResave_UntouchedNeighborKeepsItsValues()
    {
        // F1 guard (hash shortcut): an unchanged object in a batch is excluded from the value
        // pipeline entirely. A filtering bug that let it reach the diff with an empty new tree
        // would read as "delete all its values" - this pin makes that loud on every provider.
        var idA = await SeedAsync(FullProps());
        var idB = await SeedAsync(new CtProbeProps { Label = "neighbor", Longs = [7, 8, 9] });

        var a = await Redb.LoadAsync<CtProbeProps>(idA, depth: 2);
        var b = await Redb.LoadAsync<CtProbeProps>(idB, depth: 2);
        var bRowsBefore = await SnapshotRowsAsync(idB);

        a!.Props.Longs![0] = 111;
        await Redb.SaveAsync(new List<Core.Models.Contracts.IRedbObject> { a, b! });

        var rA = await Redb.LoadAsync<CtProbeProps>(idA, depth: 2);
        rA!.Props.Longs![0].Should().Be(111);
        var rB = await Redb.LoadAsync<CtProbeProps>(idB, depth: 2);
        rB!.Props.Label.Should().Be("neighbor");
        rB.Props.Longs.Should().Equal(7, 8, 9);
        rB.Props.Dict.Should().BeNull();

        if (ExpectStableValueRowIds)
            (await SnapshotRowsAsync(idB)).Select(v => v.Id).Should().Equal(bRowsBefore.Select(v => v.Id),
                "untouched batch neighbor's value rows must survive byte-for-byte");
    }
}
