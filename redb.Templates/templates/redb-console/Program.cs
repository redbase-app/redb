using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Models.Entities;
#if (pro)
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
        // Pro: LINQ compilation, change tracking, analytics, window functions
        services.AddRedbPro(options => options
#if (UsePostgres)
            .UsePostgres("Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=redb_app;Include Error Detail=true")
#else
            .UseMsSql("Server=localhost;Database=redb_app;User Id=sa;Password=YOUR_PASSWORD;TrustServerCertificate=true")
#endif
            // .WithLicense("YOUR_LICENSE_KEY")  // Uncomment when you have a Pro license
            .Configure(c =>
            {
                // c.EavSaveStrategy = EavSaveStrategy.ChangeTracking;  // Diff-tree save (Pro)
                // c.EnableLazyLoadingForProps = true;                   // Lazy-load Props on access
                // c.EnablePropsCache = true;                            // Cache materialized Props
            }));
#else
        // Free edition
        services.AddRedb(options =>
        {
#if (UsePostgres)
            options.ConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=YOUR_PASSWORD;Database=redb_app;Include Error Detail=true";
#else
            options.ConnectionString = "Server=localhost;Database=redb_app;User Id=sa;Password=YOUR_PASSWORD;TrustServerCertificate=true";
#endif
        });
#endif

        var provider = services.BuildServiceProvider();
        var redb = provider.GetRequiredService<IRedbService>();

        // --- Initialize: sync schemes, warm up caches ---
        Console.WriteLine("Initializing RedBase...");
        await redb.InitializeAsync();
        Console.WriteLine("Ready.");
        Console.WriteLine();

        // -----------------------------------------------
        // 1. Create
        // -----------------------------------------------
        var product = await redb.CreateAsync<Product>();
        product.Name = "MacBook Pro 16";
        product.Props = new Product
        {
            Price = 2499.99m,
            Category = "Laptops",
            InStock = true,
            Tags = ["apple", "laptop", "pro"]
        };
        await redb.SaveAsync(product);
        Console.WriteLine($"Created: #{product.Id} {product.Name}");

        // -----------------------------------------------
        // 2. Load
        // -----------------------------------------------
        var loaded = await redb.LoadAsync<Product>(product.Id);
        Console.WriteLine($"Loaded:  #{loaded.Id} {loaded.Name} — {loaded.Props!.Category}, ${loaded.Props.Price}");
        Console.WriteLine($"  Tags:  {string.Join(", ", loaded.Props.Tags)}");

        // -----------------------------------------------
        // 3. Update
        // -----------------------------------------------
        loaded.Props.Price = 2299.99m;
        loaded.Props.Tags = ["apple", "laptop", "pro", "sale"];
        await redb.SaveAsync(loaded);
        Console.WriteLine($"Updated: price -> ${loaded.Props.Price}");

        // -----------------------------------------------
        // 4. Query (search by Name)
        // -----------------------------------------------
        var found = await redb.SearchAsync<Product>(product.Name);
        Console.WriteLine($"Search:  found {found.Count()} object(s) matching \"{product.Name}\"");

#if (pro)
        // -----------------------------------------------
        // 5. Pro: LINQ Query
        // -----------------------------------------------
        var query = redb.Query<Product>();
        var expensive = await query
            .Where(x => x.Price > 1000 && x.Category == "Laptops")
            .OrderByDescending(x => x.Price)
            .ToListAsync();
        Console.WriteLine($"LINQ:    {expensive.Count} laptop(s) over $1000");

        // -----------------------------------------------
        // 6. Pro: Aggregation
        // -----------------------------------------------
        var avgPrice = await query.AverageAsync(x => x.Price);
        Console.WriteLine($"Avg:     ${avgPrice:F2}");
#endif

        // -----------------------------------------------
        // Tree: parent-child hierarchy
        // -----------------------------------------------
        Console.WriteLine();
        Console.WriteLine("--- Tree Demo ---");

        var root = await redb.CreateAsync<Category>();
        root.Name = "Electronics";
        root.Props = new Category { SortOrder = 1 };
        await redb.SaveAsync(root);

        var child = await redb.CreateChildAsync<Category>(root.Id);
        child.Name = "Laptops";
        child.Props = new Category { SortOrder = 1 };
        await redb.SaveAsync(child);

        var grandchild = await redb.CreateChildAsync<Category>(child.Id);
        grandchild.Name = "Gaming Laptops";
        grandchild.Props = new Category { SortOrder = 2 };
        await redb.SaveAsync(grandchild);

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

    static void PrintTree(TreeRedbObject<Category> node, int indent)
    {
        Console.WriteLine($"{new string(' ', indent * 2)}{node.Name} (id={node.Id})");
        foreach (var child in node.Children)
            PrintTree(child, indent + 1);
    }
}
