using Microsoft.Extensions.DependencyInjection;
using redb.Core.Data;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A context whose DI scope has ended must refuse to work, not quietly open a fresh physical
/// connection on behalf of a dead scope. Before the fix, <c>GetOpenConnectionAsync</c> saw the
/// <c>null</c> left behind by Dispose and re-opened: the new connection belonged to nobody, the
/// second Dispose was a no-op, and the pool slot was gone for the life of the process (prod tsum,
/// 2026-09: ~30 idle PostgreSQL sessions a day until restart).
/// </summary>
public abstract class DisposedScopeGuardTestsBase
{
    private readonly IServiceProvider _root;

    protected DisposedScopeGuardTestsBase(IServiceProvider root) => _root = root;

    [Fact]
    public async Task Context_AfterItsScopeIsDisposed_ThrowsInsteadOfReopening()
    {
        var scope = _root.CreateAsyncScope();
        var ctx = scope.ServiceProvider.GetRequiredService<IRedbContext>();

        // The context has a live connection now; the scope then ends.
        (await ctx.ExecuteScalarAsync<int>("SELECT 1")).Should().Be(1);
        await scope.DisposeAsync();
        ctx.IsDisposed.Should().BeTrue();

        var act = () => ctx.ExecuteScalarAsync<int>("SELECT 1");
        await act.Should().ThrowAsync<ObjectDisposedException>(
            "a disposed context must fail loudly; re-opening a connection here leaks it for the life of the process");
    }
}
