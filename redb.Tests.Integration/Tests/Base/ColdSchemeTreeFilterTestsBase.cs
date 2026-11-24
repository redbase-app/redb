using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// WhereHasAncestor / WhereHasDescendant on a node whose metadata cache has never seen the target's scheme - a fresh
/// process, a Tsak worker that does not sync it. The target scheme is found as Query&lt;T&gt;() finds it, in the cache and
/// then in the database; the filters read the cache alone and threw "scheme for type ... not found" for a scheme the
/// database has.
/// </summary>
public abstract class ColdSchemeTreeFilterTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(string cacheDomain)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = cacheDomain;
            });
        });
        return services.BuildServiceProvider();
    }

    private sealed record Seed(string Tag, long Department, long Child);

    /// <summary>A department with a child below it, and a second object of the child's scheme outside it.</summary>
    private static async Task<Seed> SeedAsync(ServiceProvider writer)
    {
        var tag = "cst-" + Guid.NewGuid().ToString("N")[..8];
        var redb = writer.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<SimpleProps>();
        await redb.SyncSchemeAsync<DepartmentProps>();
        await redb.InitializeTypeRegistryAsync();

        var department = new TreeRedbObject<DepartmentProps> { name = tag + "-d", Props = new DepartmentProps { Name = tag, Code = tag + "-A" } };
        department.id = await redb.SaveAsync(department);
        var child = new TreeRedbObject<SimpleProps> { name = tag + "-c", Props = new SimpleProps { Title = tag } };
        child.id = await redb.CreateChildAsync(child, department);
        await redb.SaveAsync(new TreeRedbObject<SimpleProps> { name = tag + "-o", Props = new SimpleProps { Title = tag } });
        return new Seed(tag, department.id, child.id);
    }

    /// <summary>A reader with a cache domain of its own, which has never seen the scheme of <typeparamref name="TTarget"/>.</summary>
    private async Task RunOnColdReaderAsync<TTarget>(Func<IRedbService, Seed, Task> run) where TTarget : class, new()
    {
        var id = Guid.NewGuid().ToString("N")[..8];
        await using var writer = Build($"cold-tree-writer-{id}");
        var seed = await SeedAsync(writer);

        await using var reader = Build($"cold-tree-reader-{id}");
        await using var scope = reader.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        redb.Cache.GetSchemeIdByClrType(typeof(TTarget)).Should().BeNull("precondition: the reader's metadata cache has never seen the target scheme");

        await run(redb, seed);
    }

    [Fact]
    public Task WhereHasAncestor_TargetSchemeOnlyInTheDatabase_Filters()
        => RunOnColdReaderAsync<DepartmentProps>(async (redb, seed) =>
        {
            var nodes = await redb.TreeQuery<SimpleProps>().Where(p => p.Title == seed.Tag)
                .WhereHasAncestor<DepartmentProps>(d => d.Code == seed.Tag + "-A").ToListAsync();

            nodes.Select(n => n.Id).Should().Equal(new[] { seed.Child }, "only the child lies below the department");
        });

    [Fact]
    public Task WhereHasDescendant_TargetSchemeOnlyInTheDatabase_Filters()
        => RunOnColdReaderAsync<SimpleProps>(async (redb, seed) =>
        {
            var nodes = await redb.TreeQuery<DepartmentProps>().Where(d => d.Name == seed.Tag)
                .WhereHasDescendant<SimpleProps>(s => s.Title == seed.Tag).ToListAsync();

            nodes.Select(n => n.Id).Should().Equal(new[] { seed.Department }, "the department has the child below it");
        });
}
