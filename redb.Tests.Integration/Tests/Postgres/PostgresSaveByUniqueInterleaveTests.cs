using Microsoft.Extensions.DependencyInjection;
using redb.Core.Data;
using redb.Core.Exceptions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Providers;
using redb.Core.Serialization;
using redb.Postgres.Providers;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Postgres;

/// <summary>
/// BR-8 (Tsak report, 2026-09-02), the vanish race: SaveByUniqueAsync resolved the key's row, the
/// row was deleted and the key recreated elsewhere (Remove+Set+Set interleave) - the save then
/// inserted via the default AutoSwitchToInsert, hit the object-key index, and the old retry
/// (guarded by "no existing id") let the violation escape to the caller. The interleave is made
/// deterministic through the virtual resolve seam. The retry logic lives in
/// ObjectStorageProviderBase and is provider-agnostic; one provider exercises it.
/// </summary>
[Collection("Postgres")]
public class PostgresSaveByUniqueInterleaveTests
{
    private readonly PostgresFixture _fixture;

    public PostgresSaveByUniqueInterleaveTests(PostgresFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task VanishedWinner_RecreatedElsewhere_IsRetriedOntoTheNewWinner()
    {
        var redb = _fixture.Redb;
        await redb.SyncSchemeAsync<UniqueOrderProps>();

        var key = $"VU-VANISH-{Guid.NewGuid():N}";
        var firstId = await redb.SaveAsync(new RedbObject<UniqueOrderProps>
            { name = "vanish-a", ValueUnique = key, Props = new UniqueOrderProps { Note = "first" } });

        long recreatedId = 0;
        var provider = CreateHookedProvider();
        provider.AfterFirstResolve = async () =>
        {
            // The interleave, between SaveByUniqueAsync's lookup and its save: the resolved row
            // vanishes outright and the key is recreated on a fresh row by "someone else".
            await redb.Context.ExecuteAsync($"DELETE FROM _objects WHERE _id = {firstId}");
            recreatedId = await redb.SaveAsync(new RedbObject<UniqueOrderProps>
                { name = "vanish-b", ValueUnique = key, Props = new UniqueOrderProps { Note = "recreated" } });
        };

        var upsertId = await provider.SaveByUniqueAsync(new RedbObject<UniqueOrderProps>
            { name = "vanish-c", ValueUnique = key, Props = new UniqueOrderProps { Note = "last-writer" } });

        upsertId.Should().Be(recreatedId, "the upsert must land on the CURRENT owner of the key");
        (await redb.LoadAsync<UniqueOrderProps>(recreatedId, depth: 1))!.Props.Note.Should().Be("last-writer",
            "exactly one object per key, last writer's content");
        (await redb.Context.ExecuteScalarAsync<long>(
                "SELECT COUNT(*) FROM _objects o JOIN _schemes s ON s._id = o._id_scheme " +
                $"WHERE s._name = 'UniqueOrder' AND o._value_unique = '{key}'"))
            .Should().Be(1);
    }

    private InterleavingProvider CreateHookedProvider()
    {
        var sp = _fixture.ServiceProvider;
        return new InterleavingProvider(
            sp.GetRequiredService<IRedbContext>(),
            sp.GetRequiredService<IRedbObjectSerializer>(),
            sp.GetRequiredService<IPermissionProvider>(),
            sp.GetRequiredService<IRedbSecurityContext>(),
            sp.GetRequiredService<ISchemeSyncProvider>(),
            sp.GetRequiredService<RedbServiceConfiguration>(),
            sp.GetService<IListProvider>());
    }

    private sealed class InterleavingProvider : PostgresObjectStorageProvider
    {
        public Func<Task>? AfterFirstResolve;
        private int _resolves;

        public InterleavingProvider(
            IRedbContext context,
            IRedbObjectSerializer serializer,
            IPermissionProvider permissionProvider,
            IRedbSecurityContext securityContext,
            ISchemeSyncProvider schemeSync,
            RedbServiceConfiguration configuration,
            IListProvider? listProvider)
            : base(context, serializer, permissionProvider, securityContext, schemeSync, configuration, listProvider)
        {
        }

        protected override async Task<long?> ResolveObjectIdByUniqueKeyAsync(long schemeId, string? valueUnique, System.Threading.CancellationToken cancellationToken = default)
        {
            var id = await base.ResolveObjectIdByUniqueKeyAsync(schemeId, valueUnique, cancellationToken);
            if (System.Threading.Interlocked.Increment(ref _resolves) == 1 && AfterFirstResolve is { } hook)
                await hook();
            return id;
        }
    }
}
