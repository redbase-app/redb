using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Interception;
using redb.Core.Models.Configuration;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Save-pipeline interceptors (discussion #12, items 3-4), EF-shaped by the owner's decision:
/// an exception from Saving cancels the save (nothing reaches the database), an exception from
/// Saved propagates but the data IS committed, and under ChangeTracking the Saved context carries
/// the value-level diff. Each test builds its own ServiceProvider - interceptors are a DI
/// concern, and the shared fixtures must not see them.
/// </summary>
public abstract class SaveInterceptorTestsBase
{
    /// <summary>appsettings connection-string name: Postgres / MSSql / Sqlite.</summary>
    protected abstract string ConnectionStringName { get; }

    /// <summary>Provider registration (Free: options.UseX; Pro subclasses use the Pro overload).</summary>
    protected abstract void UseProvider(RedbOptionsBuilder options, string connectionString);

    /// <summary>services.AddRedb for Free, services.AddRedbPro for Pro subclasses.</summary>
    protected virtual void AddRedbServices(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    /// <summary>Pro subclasses run ChangeTracking - the Changes feed exists only there.</summary>
    protected virtual PropsSaveStrategy Strategy => PropsSaveStrategy.DeleteInsert;

    private static async Task CleanupNameAsync(IRedbService redb, string name)
        => await redb.Context.ExecuteAsync($"DELETE FROM _objects WHERE _name = '{name}'");

    private ServiceProvider Build(RecordingInterceptor recorder)
    {
        var cs = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build()
            .GetConnectionString(ConnectionStringName)!;
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSingleton<IRedbSaveInterceptor>(recorder);
        AddRedbServices(services, options =>
        {
            UseProvider(options, cs);
            options.Configure(c =>
            {
                c.PropsSaveStrategy = Strategy;
                c.EnablePropsCache = false;
                c.CacheDomain = "save-interceptor";
            });
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task SavingAndSaved_SeeTheSave_InOrder()
    {
        var recorder = new RecordingInterceptor();
        await using var sp = Build(recorder);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-probe");

        var obj = TestDataFactory.CreateSimple("interceptor-probe", 5m);
        var id = await redb.SaveAsync(obj);

        recorder.SavingContexts.Should().HaveCount(1);
        recorder.SavedContexts.Should().HaveCount(1);
        recorder.Order.Should().Equal("saving", "saved");

        var saving = recorder.SavingContexts[0];
        saving.RootObjects.Should().ContainSingle().Which.Name.Should().Be("interceptor-probe");
        saving.AllObjects.Should().NotBeEmpty();
        saving.Strategy.Should().Be(Strategy);

        var saved = recorder.SavedContexts[0];
        saved.SavedIds.Should().Equal(id);
        saving.NewObjects.Should().ContainSingle("the object arrived without an id - this save creates it");
        saved.NewObjectIds.Should().Contain(id, "Saved reports the freshly assigned id");
    }

    [Fact]
    public async Task ThrowInSaving_CancelsTheSave()
    {
        var recorder = new RecordingInterceptor { ThrowInSaving = true };
        await using var sp = Build(recorder);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-cancelled");

        var obj = TestDataFactory.CreateSimple("interceptor-cancelled", 5m);
        var act = async () => await redb.SaveAsync(obj);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*saving-veto*");
        recorder.SavedContexts.Should().BeEmpty("a cancelled save must not report success");

        var count = await redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _objects WHERE _name = 'interceptor-cancelled'");
        count.Should().Be(0, "Saving runs before the transaction opens - nothing may reach the database");
    }

    [Fact]
    public async Task ThrowInSaved_Propagates_ButTheDataIsCommitted()
    {
        var recorder = new RecordingInterceptor { ThrowInSaved = true };
        await using var sp = Build(recorder);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-saved-throw");

        var obj = TestDataFactory.CreateSimple("interceptor-saved-throw", 5m);
        var act = async () => await redb.SaveAsync(obj);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*saved-veto*",
            "EF-style: the Saved exception reaches the caller");

        var count = await redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _objects WHERE _name = 'interceptor-saved-throw'");
        count.Should().Be(1, "Saved runs after the commit - the data stays saved");
    }

    [Fact]
    public async Task ChangesFeed_MatchesTheStrategy()
    {
        var recorder = new RecordingInterceptor();
        await using var sp = Build(recorder);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-diff");

        var obj = TestDataFactory.CreateSimple("interceptor-diff", 5m);
        obj.id = await redb.SaveAsync(obj);

        var loaded = await redb.LoadAsync<SimpleProps>(obj.id, depth: 1);
        loaded!.Props.Title = "interceptor-diff-changed";
        await redb.SaveAsync(loaded);

        var saved = recorder.SavedContexts[^1];
        saved.NewObjectIds.Should().BeEmpty("the second save updates an existing object, creates nothing");
        if (Strategy == PropsSaveStrategy.ChangeTracking)
        {
            saved.Changes.Should().NotBeNull("ChangeTracking computes a diff - the feed hands it over");
            saved.Changes!.Should().Contain(c =>
                c.Type == RedbValueChangeType.Update &&
                c.PropertyPath == "Title" &&
                c.NewValue != null && c.NewValue.String == "interceptor-diff-changed",
                "the edited value must appear as an Update at the model-level path, carrying the new content");
        }
        else
        {
            saved.Changes.Should().BeNull("DeleteInsert rewrites everything and diffs nothing");
        }
    }

    [Fact]
    public async Task DeletingAndDeleted_SeeTheDelete_AndVetoWorks()
    {
        // Happy path + both veto semantics for the delete hooks (owner decision: deletes are
        // intercepted too). Deleting cancels; Deleted propagates but the row is gone.
        var recorder = new RecordingInterceptor();
        await using var sp = Build(recorder);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-delete");

        var obj = TestDataFactory.CreateSimple("interceptor-delete", 5m);
        obj.id = await redb.SaveAsync(obj);

        (await redb.DeleteAsync(obj)).Should().BeTrue();
        recorder.DeletingContexts.Should().ContainSingle().Which.ObjectIds.Should().Equal(obj.id);
        recorder.DeletedContexts.Should().ContainSingle().Which.DeletedCount.Should().BeGreaterThan(0);
        recorder.DeletingContexts[0].KnownObjects.Should().ContainSingle("delete-by-object hands the instance over");
    }

    [Fact]
    public async Task ThrowInDeleting_CancelsTheDelete()
    {
        var recorder = new RecordingInterceptor { ThrowInDeleting = true };
        await using var sp = Build(recorder);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-delete-veto");

        var obj = TestDataFactory.CreateSimple("interceptor-delete-veto", 5m);
        obj.id = await redb.SaveAsync(obj);

        var act = async () => await redb.DeleteAsync(obj.id);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*deleting-veto*");

        var count = await redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(*) FROM _objects WHERE _id = {obj.id}");
        count.Should().Be(1, "Deleting runs before the SQL - the object must survive the veto");
        recorder.DeletedContexts.Should().BeEmpty();
    }

    [Fact]
    public async Task ThrowInDeleted_Propagates_ButTheRowIsGone()
    {
        var recorder = new RecordingInterceptor { ThrowInDeleted = true };
        await using var sp = Build(recorder);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-deleted-throw");

        var obj = TestDataFactory.CreateSimple("interceptor-deleted-throw", 5m);
        obj.id = await redb.SaveAsync(obj);

        var act = async () => await redb.DeleteAsync(obj.id);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*deleted-veto*");

        var count = await redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(*) FROM _objects WHERE _id = {obj.id}");
        count.Should().Be(0, "Deleted runs after the SQL - the row is gone whatever the interceptor throws");
    }

    [Fact]
    public async Task RegistrySubscription_InALiveScope_RunsAfterDi_AndStopsOnDispose()
    {
        // A module subscribes after the service exists (its scope is open, the DI interceptors fixed): the very next
        // save reaches it after the DI one; once disposed, nothing reaches it, while the DI interceptor goes on.
        var journal = new List<string>();
        var fromDi = new RecordingInterceptor { Tag = "di", Journal = journal };
        await using var sp = Build(fromDi);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-registry");

        var module = new RecordingInterceptor { Tag = "module", Journal = journal };
        var subscription = sp.GetRequiredService<IRedbSaveInterceptorRegistry>().Add(module);

        var obj = TestDataFactory.CreateSimple("interceptor-registry", 5m);
        obj.id = await redb.SaveAsync(obj);
        journal.Should().Equal("di:saving", "module:saving", "di:saved", "module:saved");
        module.SavedContexts.Should().ContainSingle().Which.SavedIds.Should().Equal(obj.id);

        subscription.Dispose();
        journal.Clear();
        obj.Props.Title = "interceptor-registry-2";
        await redb.SaveAsync(obj);
        await redb.DeleteAsync(obj.id);

        journal.Should().Equal("di:saving", "di:saved", "di:deleting", "di:deleted");
        module.SavedContexts.Should().HaveCount(1, "a disposed subscription is not called again");
    }

    [Fact]
    public async Task RegistrySubscription_SeesDeletes_AndItsVetoCancelsAnySave()
    {
        var recorderFromDi = new RecordingInterceptor();
        await using var sp = Build(recorderFromDi);
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-registry-delete");
        await CleanupNameAsync(redb, "interceptor-registry-veto");
        var registry = sp.GetRequiredService<IRedbSaveInterceptorRegistry>();

        var obj = TestDataFactory.CreateSimple("interceptor-registry-delete", 5m);
        obj.id = await redb.SaveAsync(obj);

        var module = new RecordingInterceptor();
        using (registry.Add(module))
            (await redb.DeleteAsync(obj)).Should().BeTrue();

        module.DeletingContexts.Should().ContainSingle().Which.ObjectIds.Should().Equal(obj.id);
        module.DeletedContexts.Should().ContainSingle();

        var veto = new RecordingInterceptor { ThrowInSaving = true };
        using (registry.Add(veto))
        {
            var act = async () => await redb.SaveAsync(TestDataFactory.CreateSimple("interceptor-registry-veto", 5m));
            await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*saving-veto*");
        }

        var count = await redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _objects WHERE _name = 'interceptor-registry-veto'");
        count.Should().Be(0, "a registry interceptor vetoes like a DI one: nothing reaches the database");
    }

    [Fact]
    public async Task RegistrySubscription_MadeDuringASave_WaitsForTheNextOne()
    {
        // One snapshot per operation: an interceptor subscribed while a save runs must not get a Saved without its
        // Saving. The next save sees it from the start.
        var registry = default(IRedbSaveInterceptorRegistry);
        var late = new RecordingInterceptor { Tag = "late" };
        IDisposable? lateSubscription = null;
        var fromDi = new RecordingInterceptor
        {
            OnSaving = () => lateSubscription ??= registry!.Add(late),
        };
        await using var sp = Build(fromDi);
        registry = sp.GetRequiredService<IRedbSaveInterceptorRegistry>();
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.SyncSchemeAsync<SimpleProps>();
        await CleanupNameAsync(redb, "interceptor-registry-late");

        var obj = TestDataFactory.CreateSimple("interceptor-registry-late", 5m);
        obj.id = await redb.SaveAsync(obj);

        late.SavingContexts.Should().BeEmpty();
        late.SavedContexts.Should().BeEmpty("the save had started without it");

        obj.Props.Title = "interceptor-registry-late-2";
        await redb.SaveAsync(obj);
        late.Order.Should().Equal("saving", "saved");

        lateSubscription!.Dispose();
    }

    private sealed class RecordingInterceptor : IRedbSaveInterceptor
    {
        /// <summary>Prefix of the entries this recorder writes into <see cref="Journal"/>.</summary>
        public string Tag { get; init; } = "";

        /// <summary>A journal shared with other recorders, to see their relative order.</summary>
        public List<string>? Journal { get; init; }

        /// <summary>Runs inside SavingAsync, before the recording.</summary>
        public Action? OnSaving { get; init; }

        public List<RedbSavingContext> SavingContexts { get; } = new();
        public List<RedbSavedContext> SavedContexts { get; } = new();
        public List<RedbDeletingContext> DeletingContexts { get; } = new();
        public List<RedbDeletedContext> DeletedContexts { get; } = new();
        public List<string> Order { get; } = new();
        public bool ThrowInSaving { get; init; }
        public bool ThrowInSaved { get; init; }
        public bool ThrowInDeleting { get; init; }
        public bool ThrowInDeleted { get; init; }

        private void Record(string hook)
        {
            Order.Add(hook);
            Journal?.Add($"{Tag}:{hook}");
        }

        public Task SavingAsync(RedbSavingContext context, CancellationToken cancellationToken = default)
        {
            OnSaving?.Invoke();
            SavingContexts.Add(context);
            Record("saving");
            if (ThrowInSaving) throw new InvalidOperationException("saving-veto");
            return Task.CompletedTask;
        }

        public Task SavedAsync(RedbSavedContext context, CancellationToken cancellationToken = default)
        {
            SavedContexts.Add(context);
            Record("saved");
            if (ThrowInSaved) throw new InvalidOperationException("saved-veto");
            return Task.CompletedTask;
        }

        public Task DeletingAsync(RedbDeletingContext context, CancellationToken cancellationToken = default)
        {
            DeletingContexts.Add(context);
            Record("deleting");
            if (ThrowInDeleting) throw new InvalidOperationException("deleting-veto");
            return Task.CompletedTask;
        }

        public Task DeletedAsync(RedbDeletedContext context, CancellationToken cancellationToken = default)
        {
            DeletedContexts.Add(context);
            Record("deleted");
            if (ThrowInDeleted) throw new InvalidOperationException("deleted-veto");
            return Task.CompletedTask;
        }
    }
}
