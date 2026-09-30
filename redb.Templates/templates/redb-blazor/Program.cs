using redb.Core;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Core.Pro.Extensions;
using redb.SQLite.Pro.Extensions;
using RedbBlazor.Components;
using RedbBlazor.Models;
using RedbBlazor.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// --- RedBase ---
// The connection string lives in appsettings.json ("ConnectionStrings:Redb"). In a container it is
// overridden by the ConnectionStrings__Redb environment variable (see deploy/*.yml).
var connectionString = builder.Configuration.GetConnectionString("Redb")
    ?? throw new InvalidOperationException("ConnectionStrings:Redb is missing in appsettings.json.");

builder.Services.AddRedbPro(options => options
    .UseSqlite(connectionString)
    // To use PostgreSQL or SQL Server instead: swap the package in RedbBlazor.csproj
    // (redb.Postgres.Pro / redb.MSSql.Pro), replace the using redb.SQLite.Pro.Extensions above
    // with redb.Postgres.Pro.Extensions / redb.MSSql.Pro.Extensions, and use one of:
    //.UsePostgres(connectionString)
    //.UseMsSql(connectionString)
    .Configure(c =>
    {
        // Narrows the object set before the pivot step, so a selective filter does not scan the
        // whole scheme. It never changes results.
        c.EnablePvtPrefilter = true;

        // Saves only the properties that changed since the object was loaded.
        c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;

        // Props cache: off by default. Turn it on when the same objects are read again and
        // again; the limit is per process, entries past the TTL are loaded again.
        // c.EnablePropsCache = true;
        // c.PropsCacheMaxSize = 10_000;
        // c.PropsCacheTtl = TimeSpan.FromMinutes(60);
    }));

// Components use RedbWork, not IRedbService directly: see the remarks in Services/RedbWork.cs.
builder.Services.AddSingleton<RedbWork>();

var app = builder.Build();

// --- Initialize RedBase once at startup ---
// ensureCreated builds the RedBase tables on the first run and is idempotent afterwards. It does not
// create the database itself: for PostgreSQL and SQL Server the database has to exist already.
await app.Services.GetRequiredService<RedbWork>().RunAsync(async redb =>
{
    await redb.InitializeAsync(ensureCreated: true);
    await SeedAsync(redb);
});

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
}

app.UseAntiforgery();
app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();

// Sample data on the first run, so the pages are not empty.
static async Task SeedAsync(IRedbService redb)
{
    if (!await redb.Query<Product>().AnyAsync())
    {
        var samples = new (string Name, string Category, decimal Price, bool InStock)[]
        {
            ("Laptop 14", "Laptops", 1299m, true),
            ("Laptop 16", "Laptops", 1899m, false),
            ("Monitor 27", "Monitors", 349m, true),
            ("Keyboard", "Accessories", 79m, true),
            ("Mouse", "Accessories", 39m, true),
        };
        foreach (var s in samples)
        {
            await redb.SaveAsync(new RedbObject<Product>
            {
                Name = s.Name,
                Props = new Product { Category = s.Category, Price = s.Price, InStock = s.InStock }
            });
        }
    }

    if (!await redb.TreeQuery<Category>().AnyAsync())
    {
        var root = new TreeRedbObject<Category> { Name = "Electronics", Props = new Category { SortOrder = 1 } };
        await redb.SaveAsync(root);

        var computers = new TreeRedbObject<Category> { Name = "Computers", Props = new Category { SortOrder = 1 } };
        await redb.CreateChildAsync(computers, root);
        await redb.CreateChildAsync(new TreeRedbObject<Category> { Name = "Laptops", Props = new Category { SortOrder = 1 } }, computers);
        await redb.CreateChildAsync(new TreeRedbObject<Category> { Name = "Monitors", Props = new Category { SortOrder = 2 } }, root);
    }
}
