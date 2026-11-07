using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// CT-4 (решение владельца 2026-09-04, «как у EF»): контракт «один scope - один поток» защищён
/// операционным детектором уровня EF DbContext. Командный страж соединения ловит только
/// НАЛОЖИВШИЕСЯ команды; два перемежающихся async-сохранения могли молча перемешать
/// pending-состояние ChangeTracking. Детектор ядра общий - одного провайдера достаточно.
/// </summary>
[Collection("Postgres")]
public class PostgresSaveOperationGuardTests
{
    private readonly PostgresFixture _fixture;

    public PostgresSaveOperationGuardTests(PostgresFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task ParallelSaves_OnOneScope_AreLoudlyDetected()
    {
        var redb = _fixture.Redb;
        await redb.SyncSchemeAsync<SimpleProps>();

        var t1 = redb.SaveAsync(new RedbObject<SimpleProps> { name = "guard-1", Props = new SimpleProps { Title = "a" } });
        var t2 = redb.SaveAsync(new RedbObject<SimpleProps> { name = "guard-2", Props = new SimpleProps { Title = "b" } });

        var act = async () => await Task.WhenAll(t1, t2);
        (await act.Should().ThrowAsync<InvalidOperationException>(
                "второе сохранение, вошедшее до завершения первого, обязано быть громким, а не молча перемешивать состояние"))
            .WithMessage("*SaveAsync*");
    }
}
