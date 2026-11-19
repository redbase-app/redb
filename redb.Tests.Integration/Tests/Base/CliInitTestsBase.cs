using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// <c>redb init</c> on PostgreSQL and SQL Server, through the command itself (<c>Program.InitSchemaAsync</c>): into an
/// empty database it creates the schema; a database that already holds the redb schema is refused before anything
/// runs. Run over an existing database, the SQL Server script dropped every foreign key and the id sequence before its
/// first CREATE TABLE failed, and the database went on without them. Scratch databases of their own, dropped afterwards.
/// </summary>
public abstract class CliInitTestsBase
{
    /// <summary>postgres or mssql.</summary>
    protected abstract string ProviderName { get; }

    /// <summary>The module version this build requires.</summary>
    protected abstract string RequiredVersion { get; }

    protected abstract Task<string> NewDatabaseAsync(string suffix);

    protected abstract ServiceProvider Build(string cs);

    protected abstract Task<string?> ScalarAsync(string cs, string sql);

    /// <summary>The sequences, the foreign keys, the triggers with their state, the module version.</summary>
    protected abstract Task<List<string>> CatalogAsync(string cs);

    [Fact]
    public async Task Init_IntoAnEmptyDatabase_CreatesTheSchemaWithTheRequiredModule()
    {
        var cs = await NewDatabaseAsync("init");

        var (code, output) = await InitAsync(cs);

        code.Should().Be(0, output);
        (await CatalogAsync(cs)).Should().Contain($"module {RequiredVersion}");
        await using var provider = Build(cs);
        await provider.GetRequiredService<IRedbService>().InitializeAsync();
    }

    [Fact]
    public async Task ASecondInit_IsRefused_AndTouchesNothing()
    {
        var cs = await NewDatabaseAsync("again");
        (await InitAsync(cs)).Code.Should().Be(0);
        await using (var provider = Build(cs))
        {
            var redb = provider.GetRequiredService<IRedbService>();
            await redb.InitializeAsync();
            await redb.SyncSchemeAsync<SimpleProps>();
            await redb.SaveAsync(new RedbObject<SimpleProps> { name = "kept", Props = new SimpleProps { Title = "k" } });
        }
        var before = await StateAsync(cs);
        before.Should().Contain(l => l.StartsWith("fk ", StringComparison.Ordinal), "precondition: init created the foreign keys");
        before.Should().Contain(l => l.StartsWith("sequence ", StringComparison.Ordinal), "precondition: init created the id sequence");

        var (code, output) = await InitAsync(cs);

        (await StateAsync(cs)).Should().Equal(before);
        code.Should().Be(1);
        output.Should().Contain("already holds the redb schema").And.Contain("has changed nothing")
            .And.Contain($"redb schema --upgrade --provider {ProviderName}");
    }

    /// <summary>Runs <c>redb init</c> and returns its exit code and what it printed.</summary>
    protected async Task<(int Code, string Output)> InitAsync(string cs)
    {
        var console = Console.Out;
        var output = new StringWriter();
        Console.SetOut(output);
        try
        {
            var code = await global::redb.CLI.Program.InitSchemaAsync(cs, ProviderName, verbose: false, CancellationToken.None);
            return (code, output.ToString());
        }
        finally
        {
            Console.SetOut(console);
        }
    }

    private async Task<List<string>> StateAsync(string cs)
    {
        var lines = await CatalogAsync(cs);
        foreach (var table in new[] { "_schemes", "_structures", "_objects", "_values", "_users", "_roles" })
            lines.Add($"{table}={await ScalarAsync(cs, $"SELECT COUNT(*) FROM {table}")}");
        return lines;
    }
}
