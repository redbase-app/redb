using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Core.Models.Enums;
using redb.Core.Models.Users;
using redb.SQLite.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// The permission cache is shared by the process, and ids are per database: two databases hand out the same
/// user and object ids in the same order (the system admin is id 1 in every one of them). A permission worked
/// out in one database must never answer for another (review 2026-09-24, C-1: the key was user and object
/// only, the same class of defect as the structure-tree cache fixed earlier as B-10).
/// </summary>
public class SqlitePermissionCacheDomainTests
{
    [Fact]
    public async Task APermissionOfOneDatabase_IsNotServedForAnother()
    {
        await using var providerA = Build("a");
        await using var providerB = Build("b");
        var a = providerA.GetRequiredService<IRedbService>();
        var b = providerB.GetRequiredService<IRedbService>();

        // The same steps in the same order on two fresh databases give the same ids.
        var (userA, objectA) = await SeedAsync(a);
        var (userB, objectB) = await SeedAsync(b);
        userA.Id.Should().Be(userB.Id, "precondition: the two databases gave the user the same id");
        objectA.Id.Should().Be(objectB.Id, "precondition: the two databases gave the object the same id");

        // Database A lets the user delete the object; database B lets them only read it. (With no permission row at
        // all the check throws rather than answers, so B gets a row that says no.)
        await a.GrantPermissionAsync(userA, objectA, PermissionAction.Delete);
        await b.GrantPermissionAsync(userB, objectB, PermissionAction.Select);

        (await b.CanUserDeleteObject(objectB.Id, userB.Id)).Should().BeFalse("database B granted reading only");
        (await a.CanUserDeleteObject(objectA.Id, userA.Id)).Should().BeTrue(
            "database A granted delete, and the answer of database B for the same ids must not stand in for it");
        (await b.CanUserDeleteObject(objectB.Id, userB.Id)).Should().BeFalse(
            "and the other way round: A's grant is not B's");
    }

    private static async Task<(Core.Models.Contracts.IRedbUser User, RedbObject<SimpleProps> Object)> SeedAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        var user = await redb.UserProvider.CreateUserAsync(
            new CreateUserRequest { Login = "perm-user", Name = "perm-user", Password = "perm-password-1" });
        var obj = new RedbObject<SimpleProps> { name = "perm-target", Props = new SimpleProps { Title = "target" } };
        await redb.SaveAsync(obj);
        return (user, obj);
    }

    private static ServiceProvider Build(string suffix)
    {
        global::redb.SQLite.Data.SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var cs = $"Data Source=redb_tests_perm_{suffix}_{Guid.NewGuid():N}.db";
        SqliteTestSupport.DeleteDbFiles(cs);
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c => c.CacheDomain = $"perm-{suffix}-{Guid.NewGuid():N}");
        });
        return services.BuildServiceProvider();
    }
}
