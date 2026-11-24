using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// WhereHasAncestor / WhereHasDescendant over a type that has no scheme fail at the query, naming the type, as
/// Query&lt;T&gt;() fails for it. Pro dropped the filter and returned every node; Free ran into a removed legacy path
/// ("Legacy ... SQL path is removed").
/// </summary>
public abstract class UnregisteredTreeTargetTestsBase
{
    /// <summary>A plain class: no [RedbScheme], never synced.</summary>
    public sealed class NotAScheme
    {
        public string Name { get; set; } = "";
    }

    protected readonly IRedbService Redb;

    protected UnregisteredTreeTargetTestsBase(IRedbService redb) => Redb = redb;

    private async Task<long> SeedRootAsync()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var tag = "utt-" + Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<SimpleProps> { name = tag, Props = new SimpleProps { Title = tag } };
        return await Redb.SaveAsync(root);
    }

    [Fact]
    public async Task WhereHasAncestor_TypeWithoutScheme_FailsNamingTheType()
    {
        var root = await SeedRootAsync();

        var unrooted = () => Redb.TreeQuery<SimpleProps>().WhereHasAncestor<NotAScheme>(a => a.Name == "x");
        var rooted = () => Redb.TreeQuery<SimpleProps>(root).WhereHasAncestor<NotAScheme>(a => a.Name == "x");

        unrooted.Should().Throw<InvalidOperationException>().WithMessage("WhereHasAncestor<NotAScheme>: scheme for type 'NotAScheme' not found*");
        rooted.Should().Throw<InvalidOperationException>().WithMessage("WhereHasAncestor<NotAScheme>:*");
    }

    [Fact]
    public async Task WhereHasDescendant_TypeWithoutScheme_FailsNamingTheType()
    {
        var root = await SeedRootAsync();

        var unrooted = () => Redb.TreeQuery<SimpleProps>().WhereHasDescendant<NotAScheme>(a => a.Name == "x");
        var rooted = () => Redb.TreeQuery<SimpleProps>(root).WhereHasDescendant<NotAScheme>(a => a.Name == "x");

        unrooted.Should().Throw<InvalidOperationException>().WithMessage("WhereHasDescendant<NotAScheme>: scheme for type 'NotAScheme' not found*");
        rooted.Should().Throw<InvalidOperationException>().WithMessage("WhereHasDescendant<NotAScheme>:*");
    }

    /// <summary>The same type in a plain query fails the same way: same exception type, same words.</summary>
    [Fact]
    public void Query_TypeWithoutScheme_FailsTheSameWay()
    {
        var act = () => Redb.Query<NotAScheme>();

        act.Should().Throw<InvalidOperationException>().WithMessage("*for type 'NotAScheme' not found*");
    }
}
