using redb.Core.Attributes;
using redb.Core.Interception;

namespace redb.Tests.Integration.Models;

/// <summary>
/// The Props type of a "module": the module unload test loads this assembly again into a collectible
/// AssemblyLoadContext and works with the copy of this type found there.
/// </summary>
[RedbScheme(Name = "test.module_unload")]
public class ModuleUnloadProps
{
    public string Title { get; set; } = "";
}

/// <summary>The save interceptor of the "module" (see <see cref="ModuleUnloadProps"/>).</summary>
public sealed class ModuleUnloadInterceptor : IRedbSaveInterceptor
{
    public int Saved;
    public int Deleted;

    public Task SavedAsync(RedbSavedContext context, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Saved);
        return Task.CompletedTask;
    }

    public Task DeletedAsync(RedbDeletedContext context, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref Deleted);
        return Task.CompletedTask;
    }
}
