using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
#if (pro)
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
#else
using redb.Core.Extensions;
#endif
#if (UsePostgres && pro)
using redb.Postgres.Pro.Extensions;
#elif (UsePostgres)
using redb.Postgres.Extensions;
#elif (UseMsSql && pro)
using redb.MSSql.Pro.Extensions;
#elif (UseMsSql)
using redb.MSSql.Extensions;
#elif (UseSqlite && pro)
using redb.SQLite.Pro.Extensions;
#elif (UseSqlite)
using redb.SQLite.Extensions;
#endif
using RedbApp.Models;

namespace RedbApp;

class Program
{
    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;

        // --- DI Setup ---
        var services = new ServiceCollection();
        services.AddLogging(b => b
            .AddConsole()
            .SetMinimumLevel(LogLevel.Information)
            .AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning));

#if (pro)
        // Pro: LINQ compilation, change tracking, analytics, window functions.
        // To switch provider: uncomment the line you want, comment the current one, and swap the
        // redb.* PackageReference in RedbApp.csproj to match (see the comment there).
        services.AddRedbPro(options => options
#if (UsePostgres)
            .UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=redb_app;Pooling=true;Include Error Detail=true;Options=-c jit=off")
            //.UseMsSql("Server=127.0.0.1,1433;Database=redb_app;User Id=sa;Password=YOUR_PASSWORD;TrustServerCertificate=true;Command Timeout=600;")  // 127.0.0.1, not localhost: localhost resolves to ::1 first and a container listening on [::]:1433 makes the connect hang ~63s
            //.UseSqlite(@"Data Source=redb_app.db")
#elif (UseMsSql)
            //.UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=redb_app;Pooling=true;Include Error Detail=true;Options=-c jit=off")
            .UseMsSql("Server=127.0.0.1,1433;Database=redb_app;User Id=sa;Password=YOUR_PASSWORD;TrustServerCertificate=true;Command Timeout=600;")  // 127.0.0.1, not localhost: localhost resolves to ::1 first and a container listening on [::]:1433 makes the connect hang ~63s
            //.UseSqlite(@"Data Source=redb_app.db")
#else
            //.UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=redb_app;Pooling=true;Include Error Detail=true;Options=-c jit=off")
            //.UseMsSql("Server=127.0.0.1,1433;Database=redb_app;User Id=sa;Password=YOUR_PASSWORD;TrustServerCertificate=true;Command Timeout=600;")  // 127.0.0.1, not localhost: localhost resolves to ::1 first and a container listening on [::]:1433 makes the connect hang ~63s
            .UseSqlite(@"Data Source=redb_app.db")  // Pro tier runs in Blazor WASM / mobile too
#endif
            // .WithLicense("YOUR_LICENSE_KEY")  // Not needed: the whole 4.x line is free and unrestricted
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
#else
        // Free edition.
        // To switch provider: uncomment the line you want, comment the current one, and swap the
        // redb.* PackageReference in RedbApp.csproj to match (see the comment there).
        services.AddRedb(options => options
#if (UsePostgres)
            .UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=redb_app;Pooling=true;Include Error Detail=true;Options=-c jit=off")
            //.UseMsSql("Server=127.0.0.1,1433;Database=redb_app;User Id=sa;Password=YOUR_PASSWORD;TrustServerCertificate=true;Command Timeout=600;")  // 127.0.0.1, not localhost: localhost resolves to ::1 first and a container listening on [::]:1433 makes the connect hang ~63s
            //.UseSqlite(@"Data Source=redb_app.db")
#elif (UseMsSql)
            //.UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=redb_app;Pooling=true;Include Error Detail=true;Options=-c jit=off")
            .UseMsSql("Server=127.0.0.1,1433;Database=redb_app;User Id=sa;Password=YOUR_PASSWORD;TrustServerCertificate=true;Command Timeout=600;")  // 127.0.0.1, not localhost: localhost resolves to ::1 first and a container listening on [::]:1433 makes the connect hang ~63s
            //.UseSqlite(@"Data Source=redb_app.db")
#else
            //.UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=redb_app;Pooling=true;Include Error Detail=true;Options=-c jit=off")
            //.UseMsSql("Server=127.0.0.1,1433;Database=redb_app;User Id=sa;Password=YOUR_PASSWORD;TrustServerCertificate=true;Command Timeout=600;")  // 127.0.0.1, not localhost: localhost resolves to ::1 first and a container listening on [::]:1433 makes the connect hang ~63s
            // The Free tier hosts its server-side SQL functions in a native SQLite extension. The
            // redb.SQLite package ships it and the provider picks it up next to the app, so there is
            // nothing to install. macOS binaries are not built yet: use Pro there.
            .UseSqlite(@"Data Source=redb_app.db")
#endif
        );
#endif

        await using var provider = services.BuildServiceProvider();

        // One scope per unit of work. An IRedbService is one connection: a service kept for the life of the process

        // cannot serve parallel flows (a web request, a job, a message handler each resolve their own from a scope).

        // A console does its work in one flow, so the scope here only shows the shape that carries over to hosts.

        await using var scope = provider.CreateAsyncScope();

        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();

        // --- Initialize: create the schema if absent, sync schemes, warm up caches ---
        // ensureCreated builds the redb tables on first run and is idempotent afterwards, so it is
        // safe to leave on. It does not CREATE DATABASE: for Postgres and SQL Server the database in
        // the connection string has to exist already. SQLite creates its file by itself.
        Console.WriteLine("Initializing RedBase...");
        await redb.InitializeAsync(ensureCreated: true);
        Console.WriteLine("Ready.");
        Console.WriteLine();

        // -----------------------------------------------
        // 1. Create
        // -----------------------------------------------
        // An object is a plain `new`: Name is the object's own field, Props is your class.
        var product = new RedbObject<Product>
        {
            Name = "MacBook Pro 16",
            Props = new Product
            {
                Price = 2499.99m,
                Category = "Laptops",
                InStock = true,
                Tags = ["apple", "laptop", "pro"]
            }
        };
        await redb.SaveAsync(product);
        Console.WriteLine($"Created: #{product.Id} {product.Name}");

        // -----------------------------------------------
        // 2. Load
        // -----------------------------------------------
        var loaded = await redb.LoadAsync<Product>(product.Id);
        Console.WriteLine($"Loaded:  #{loaded.Id} {loaded.Name} — {loaded.Props!.Category}, ${loaded.Props.Price:F2}");
        Console.WriteLine($"  Tags:  {string.Join(", ", loaded.Props.Tags)}");

        // -----------------------------------------------
        // 3. Update
        // -----------------------------------------------
        loaded.Props.Price = 2299.99m;
        loaded.Props.Tags = ["apple", "laptop", "pro", "sale"];
        await redb.SaveAsync(loaded);
        Console.WriteLine($"Updated: price -> ${loaded.Props.Price:F2}");

        // -----------------------------------------------
        // 4. Query — LINQ over your own properties, no SQL
        // -----------------------------------------------
        // Query() is a Free-tier capability. The lambda runs against Props, so the fields you
        // filter on are the ones you declared in the Product class.
        var query = redb.Query<Product>();
        var laptops = await query
            .Where(x => x.Category == "Laptops" && x.InStock)
            .OrderByDescending(x => x.Price)
            .ToListAsync();
        Console.WriteLine($"Query:   {laptops.Count} laptop(s) in stock");

#if (pro)
        // -----------------------------------------------
        // 5. Pro: Aggregation pushed down to the database
        // -----------------------------------------------
        var avgPrice = await redb.Query<Product>().AverageAsync(x => x.Price);
        Console.WriteLine($"Avg:     ${avgPrice:F2}");
#endif

        // -----------------------------------------------
        // Tree: parent-child hierarchy
        // -----------------------------------------------
        Console.WriteLine();
        Console.WriteLine("--- Tree Demo ---");

        // A node of a hierarchy is a TreeRedbObject; CreateChildAsync takes the node and its
        // parent, and returns the new id.
        var root = new TreeRedbObject<Category>
        {
            Name = "Electronics",
            Props = new Category { SortOrder = 1 }
        };
        await redb.SaveAsync(root);

        var child = new TreeRedbObject<Category>
        {
            Name = "Laptops",
            Props = new Category { SortOrder = 1 }
        };
        await redb.CreateChildAsync(child, root);

        var grandchild = new TreeRedbObject<Category>
        {
            Name = "Gaming Laptops",
            Props = new Category { SortOrder = 2 }
        };
        await redb.CreateChildAsync(grandchild, child);

        var tree = await redb.LoadTreeAsync<Category>(root.Id);
        PrintTree(tree, 0);

        // -----------------------------------------------
        // Cleanup
        // -----------------------------------------------
        Console.WriteLine();
        await redb.DeleteAsync(product.Id);
        await redb.DeleteAsync(root.Id);  // Cascade deletes children
        Console.WriteLine("Cleaned up. Done!");
    }

    // ITreeRedbObject, not TreeRedbObject<Category>: Children is a collection of the interface,
    // so a typed parameter would not accept what the recursion hands it.
    static void PrintTree(ITreeRedbObject node, int indent)
    {
        Console.WriteLine($"{new string(' ', indent * 2)}{node.Name} (id={node.Id})");
        foreach (var child in node.Children)
            PrintTree(child, indent + 1);
    }
}
