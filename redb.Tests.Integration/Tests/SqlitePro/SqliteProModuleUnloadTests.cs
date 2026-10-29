using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Interception;
using redb.Core.Models.Configuration;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Pro.Extensions;
using redb.SQLite.Data;
using redb.SQLite.Pro.Extensions;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.SqlitePro;

/// <summary>
/// A module of Tsak, end to end: its Props type and its save interceptor come from a collectible AssemblyLoadContext,
/// it syncs its scheme, saves, loads through the props cache, subscribes to the interceptor registry, deletes and
/// unsubscribes; then the context unloads while the host's redb service lives on. Nothing of redb may keep the
/// context alive.
/// </summary>
[Collection("SqlitePro")]
public class SqliteProModuleUnloadTests
{
    public SqliteProModuleUnloadTests(SqliteProFixture _) { }

    // A database of its own, created by the test: the "Sqlite" connection string it used is the file of the Free
    // collection, which had a schema only when that collection, or a previous run, made it.
    private const string Cs = "Data Source=redb_tests_module_unload.db";

    private static ServiceProvider BuildHost()
    {
        var cs = Cs;
        SqliteDataSource.NativeExtensionPath ??= SqliteTestSupport.ResolveNativeExtension();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedbPro(options =>
        {
            options.UseSqlite(cs);
            options.Configure(c =>
            {
                c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;
                c.EnablePropsCache = true;
                c.CacheDomain = "module-unload";
            });
        });
        return services.BuildServiceProvider();
    }

    private static MethodInfo ServiceMethod(string name, int genericArity, params Type[] parameters)
        => new[] { typeof(IRedbService) }.Concat(typeof(IRedbService).GetInterfaces())
               .SelectMany(i => i.GetMethods())
               .First(m => m.Name == name
                           && (m.IsGenericMethodDefinition ? m.GetGenericArguments().Length : 0) == genericArity
                           && m.GetParameters().Select(p => p.ParameterType.IsGenericParameter ? typeof(object) : p.ParameterType)
                               .SequenceEqual(parameters));

    private static async Task<object?> Await(object task)
    {
        await (Task)task;
        return task.GetType().GetProperty("Result")?.GetValue(task);
    }

    // Everything that touches a type of the module runs here, so no local of the test method keeps it.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static async Task<(WeakReference Context, int Saved, int Deleted)> RunModuleAndUnload(IServiceProvider host)
    {
        var context = new AssemblyLoadContext("module-unload", isCollectible: true);
        var assembly = context.LoadFromAssemblyPath(typeof(ModuleUnloadProps).Assembly.Location);
        var propsType = assembly.GetType(typeof(ModuleUnloadProps).FullName!)!;
        var interceptorType = assembly.GetType(typeof(ModuleUnloadInterceptor).FullName!)!;

        var redb = host.GetRequiredService<IRedbService>();
        var registry = host.GetRequiredService<IRedbSaveInterceptorRegistry>();
        var interceptor = (IRedbSaveInterceptor)Activator.CreateInstance(interceptorType)!;

        await Await(ServiceMethod("SyncSchemeAsync", 1, typeof(CancellationToken))
            .MakeGenericMethod(propsType).Invoke(redb, [CancellationToken.None])!);

        int saved, deleted;
        using (registry.Add(interceptor))
        {
            var obj = (RedbObject)Activator.CreateInstance(typeof(RedbObject<>).MakeGenericType(propsType))!;
            obj.name = "module-unload-probe";
            var id = (long)(await Await(ServiceMethod("SaveAsync", 0, typeof(IRedbObject), typeof(CancellationToken))
                .Invoke(redb, [obj, CancellationToken.None])!))!;

            var load = ServiceMethod("LoadAsync", 1, typeof(long), typeof(int), typeof(CancellationToken)).MakeGenericMethod(propsType);
            var loaded = (IRedbObject)(await Await(load.Invoke(redb, [id, 10, CancellationToken.None])!))!;
            loaded.Should().NotBeNull();
            (await Await(load.Invoke(redb, [id, 10, CancellationToken.None])!)).Should().NotBeNull("the second load is served by the props cache");

            await Await(ServiceMethod("DeleteAsync", 0, typeof(IRedbObject), typeof(CancellationToken))
                .Invoke(redb, [loaded, CancellationToken.None])!);

            saved = (int)interceptorType.GetField(nameof(ModuleUnloadInterceptor.Saved))!.GetValue(interceptor)!;
            deleted = (int)interceptorType.GetField(nameof(ModuleUnloadInterceptor.Deleted))!.GetValue(interceptor)!;
        }

        var weak = new WeakReference(context);
        context.Unload();
        return (weak, saved, deleted);
    }

    [Fact]
    public async Task ModuleWithASchemeAndAnInterceptor_Unloads()
    {
        SqliteTestSupport.DeleteDbFiles(Cs);
        await using var host = BuildHost();
        var redb = host.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);

        var (weak, saved, deleted) = await RunModuleAndUnload(host);

        saved.Should().Be(1, "the module's interceptor saw its save");
        deleted.Should().Be(1, "and its delete");

        for (var i = 0; i < 20 && weak.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            await Task.Yield();
        }

        weak.IsAlive.Should().BeFalse("the host's redb service lives on, and nothing of it may keep the unloaded module alive");
    }
}
