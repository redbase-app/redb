using Microsoft.Extensions.DependencyInjection;
using redb.Core;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The fact <see cref="RedbServiceProviders.IsRoot"/> rests on: in Microsoft.Extensions.DependencyInjection the root
/// scope's provider - what a service resolved from the container itself receives - is its own
/// <see cref="IServiceScopeFactory"/>, and a child scope's provider is not. A change of the container would fail here,
/// not in production.
/// </summary>
public class RootProviderDetectionTests
{
    private sealed class Probe
    {
        public Probe(IServiceProvider serviceProvider) => FromRoot = RedbServiceProviders.IsRoot(serviceProvider);

        public bool FromRoot { get; }
    }

    [Fact]
    public void TheRootProvider_IsRecognised_AndAScopeIsNot()
    {
        var services = new ServiceCollection();
        services.AddScoped<Probe>();
        using var root = services.BuildServiceProvider();

        root.GetRequiredService<Probe>().FromRoot.Should().BeTrue("resolved from the container itself");

        using var scope = root.CreateScope();
        scope.ServiceProvider.GetRequiredService<Probe>().FromRoot.Should().BeFalse("resolved from a scope");
    }
}
