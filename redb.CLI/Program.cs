using System.CommandLine;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using redb.Core.Attributes;
using redb.Export.Flat;
using redb.Export.Providers;
using redb.Export.Services;

namespace redb.CLI;

/// <summary>
/// REDB CLI - Command line tool for database export/import operations.
/// 
/// === PostgreSQL ===
/// 
/// Export (with compression):
///   dotnet run -- export -p postgres -c "Host=localhost;Port=5432;Username=postgres;Password=1;Database=redb;Pooling=true;Include Error Detail=true;Timeout=600;Command Timeout=600" -o data.redb --compress --batch-size 100000 -v
/// 
/// Import (with clean):
///   dotnet run -- import -p postgres -c "Host=localhost;Port=5432;Username=postgres;Password=1;Database=redb;Pooling=true;Include Error Detail=true;Timeout=600;Command Timeout=600" -i data.redb --clean -v --batch-size 100000
/// 
/// === MS SQL Server ===
/// 
/// Export (with compression):
///   dotnet run -- export -p mssql -c "Server=localhost;Database=redb;User Id=sa;Password=1;TrustServerCertificate=True;Command Timeout=600" -o data.redb --compress --batch-size 100000 -v
/// 
/// Import (with clean):
///   dotnet run -- import -p mssql -c "Server=localhost;Database=redb;User Id=sa;Password=1;TrustServerCertificate=True;Command Timeout=600" -i data.redb --clean -v --batch-size 100000
/// 
/// === Flat export (redb objects -> ordinary relational tables) ===
///
/// Print the DDL of the flat tables, no database opened:
///   dotnet run -- flat -a MyApp.Models.dll -tp postgres --ddl-only
///
/// Unfold and copy (source and target are independent connections, of any two providers):
///   dotnet run -- flat -a MyApp.Models.dll -sp postgres -sc "Host=localhost;Database=redb;..." -tp mssql -tc "Server=localhost;Database=flat;..." --batch-size 10000 -v
///
///   --types       - Props classes to unfold (name or full name, comma-separated);
///                   default: every class marked with [RedbScheme] in the assembly
///
/// === Options ===
///   --compress    - compress exported file with ZIP (export only)
///   --clean       - clean database before import (import only)
///   --schemes     - export only specified scheme IDs: --schemes 100,200
///   --dry-run     - show what would be done without execution
///   --batch-size  - batch size for operations (default: 1000)
///   -v, --verbose - enable verbose output
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        // The texts of System.CommandLine itself (help headings, parse errors) follow the UI culture; keep them in
        // English like the rest of the tool, whatever the language of the operating system.
        CultureInfo.DefaultThreadCurrentUICulture = CultureInfo.InvariantCulture;
        CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;

        var rootCommand = new RootCommand("REDB CLI - Database export/import tool");

        // Common options
        var connectionOption = new Option<string>("--connection", "-c")
        {
            Description = "Database connection string",
            Required = true
        };

        var providerOption = new Option<string>("--provider", "-p")
        {
            Description = "Database provider: postgres | mssql | oracle | sqlite",
            Required = true
        };

        var verboseOption = new Option<bool>("--verbose", "-v")
        {
            Description = "Enable verbose output"
        };

        var batchSizeOption = new Option<int>("--batch-size")
        {
            Description = "Batch size for operations (default: 1000)",
            DefaultValueFactory = _ => 1000
        };

        var dryRunOption = new Option<bool>("--dry-run")
        {
            Description = "Show what would be done without making changes"
        };

        // Export command
        var exportCommand = new Command("export", "Export database to .redb file");

        var outputOption = new Option<string>("--output", "-o")
        {
            Description = "Output file path (.redb)",
            Required = true
        };

        var compressOption = new Option<bool>("--compress")
        {
            Description = "Compress output file with ZIP"
        };

        var schemesOption = new Option<string?>("--schemes")
        {
            Description = "Export only specified scheme IDs (comma-separated): --schemes 100,200,300"
        };

        exportCommand.Options.Add(connectionOption);
        exportCommand.Options.Add(providerOption);
        exportCommand.Options.Add(outputOption);
        exportCommand.Options.Add(compressOption);
        exportCommand.Options.Add(schemesOption);
        exportCommand.Options.Add(verboseOption);
        exportCommand.Options.Add(batchSizeOption);
        exportCommand.Options.Add(dryRunOption);

        exportCommand.SetAction(async (parseResult, ct) =>
        {
            var connection = parseResult.GetValue(connectionOption)!;
            var provider = parseResult.GetValue(providerOption)!;
            var output = parseResult.GetValue(outputOption)!;
            var compress = parseResult.GetValue(compressOption);
            var schemesStr = parseResult.GetValue(schemesOption);
            var verbose = parseResult.GetValue(verboseOption);
            var batchSize = parseResult.GetValue(batchSizeOption);
            var dryRun = parseResult.GetValue(dryRunOption);

            long[]? schemeIds = null;
            if (!string.IsNullOrWhiteSpace(schemesStr))
            {
                schemeIds = schemesStr.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => long.Parse(s.Trim()))
                    .ToArray();
            }

            return await ExportAsync(connection, provider, output, compress, schemeIds,
                              verbose, batchSize, dryRun, ct);
        });

        // Import command
        var importCommand = new Command("import", "Import database from .redb file");

        var inputOption = new Option<string>("--input", "-i")
        {
            Description = "Input file path (.redb)",
            Required = true
        };

        var cleanOption = new Option<bool>("--clean")
        {
            Description = "Clean database before import"
        };

        importCommand.Options.Add(connectionOption);
        importCommand.Options.Add(providerOption);
        importCommand.Options.Add(inputOption);
        var parallelOption = new Option<int>("--parallel", "-j")
        {
            Description = "Number of connections to load over (postgres). 1 keeps the whole import in one " +
                          "transaction. More than 1 loads in parallel and rebuilds the secondary indexes of the " +
                          "large tables afterwards: much faster on a large file, but not atomic - a run that " +
                          "fails leaves a partly loaded database to be imported again with --clean.",
            DefaultValueFactory = _ => 1
        };
        importCommand.Options.Add(cleanOption);
        importCommand.Options.Add(parallelOption);
        importCommand.Options.Add(verboseOption);
        importCommand.Options.Add(batchSizeOption);
        importCommand.Options.Add(dryRunOption);

        importCommand.SetAction(async (parseResult, ct) =>
        {
            var connection = parseResult.GetValue(connectionOption)!;
            var provider = parseResult.GetValue(providerOption)!;
            var input = parseResult.GetValue(inputOption)!;
            var clean = parseResult.GetValue(cleanOption);
            var verbose = parseResult.GetValue(verboseOption);
            var batchSize = parseResult.GetValue(batchSizeOption);
            var dryRun = parseResult.GetValue(dryRunOption);
            var parallel = parseResult.GetValue(parallelOption);

            return await ImportAsync(connection, provider, input, clean,
                              verbose, batchSize, dryRun, parallel, ct);
        });

        rootCommand.Subcommands.Add(exportCommand);
        rootCommand.Subcommands.Add(importCommand);

        // Init command — create REDB schema in a database
        var initCommand = new Command("init", "Create REDB schema in an existing database");
        initCommand.Options.Add(connectionOption);
        initCommand.Options.Add(providerOption);
        initCommand.Options.Add(verboseOption);

        initCommand.SetAction(async (parseResult, ct) =>
        {
            var connection = parseResult.GetValue(connectionOption)!;
            var provider = parseResult.GetValue(providerOption)!;
            var verbose = parseResult.GetValue(verboseOption);

            return await InitSchemaAsync(connection, provider, verbose, ct);
        });

        // Schema command — export REDB schema SQL to stdout or file
        var schemaCommand = new Command("schema", "Export REDB schema SQL script");
        schemaCommand.Options.Add(providerOption);

        var schemaOutputOption = new Option<string?>("--output", "-o")
        {
            Description = "Output file path. If omitted, writes to stdout."
        };
        schemaCommand.Options.Add(schemaOutputOption);

        var schemaUpgradeOption = new Option<bool>("--upgrade")
        {
            Description = "Export the upgrade script for an EXISTING database (the versioned SQL module) " +
                          "instead of the full schema. Hand it to the schema owner when the application " +
                          "is not allowed to apply it itself. Idempotent, safe to re-run. " +
                          "Not available for sqlite (the database file upgrades itself)."
        };
        schemaCommand.Options.Add(schemaUpgradeOption);

        schemaCommand.SetAction(parseResult =>
        {
            var provider = parseResult.GetValue(providerOption)!;
            var output = parseResult.GetValue(schemaOutputOption);
            var upgrade = parseResult.GetValue(schemaUpgradeOption);

            ExportSchemaScript(provider, output, upgrade);
        });

        rootCommand.Subcommands.Add(initCommand);
        rootCommand.Subcommands.Add(schemaCommand);

        // Flat command — unfold redb objects into ordinary relational tables
        var flatCommand = new Command("flat",
            "Unfold redb objects into flat relational tables and copy the data into them");

        var sourceConnectionOption = new Option<string?>("--source-connection", "-sc")
        {
            Description = "Connection string of the redb database to read"
        };

        var sourceProviderOption = new Option<string?>("--source-provider", "-sp")
        {
            Description = "Provider of the source database: postgres | mssql | sqlite"
        };

        var targetConnectionOption = new Option<string?>("--target-connection", "-tc")
        {
            Description = "Connection string of the database that receives the flat tables " +
                          "(may be the source database itself; the tables must not exist yet)"
        };

        var targetProviderOption = new Option<string>("--target-provider", "-tp")
        {
            Description = "Provider of the target database: postgres | mssql | sqlite",
            Required = true
        };

        var assemblyOption = new Option<string[]>("--assembly", "-a")
        {
            Description = "Path to an assembly that declares Props classes. Give several paths, or repeat " +
                          "the option, when the classes live in more than one project. The dependencies of " +
                          "each assembly are resolved from the folder it lies in.",
            Required = true,
            AllowMultipleArgumentsPerToken = true
        };

        var typesOption = new Option<string?>("--types")
        {
            Description = "Props classes to unfold, by name or full name (comma-separated). " +
                          "When omitted, every class marked with [RedbScheme] in the given assemblies is taken."
        };

        var ddlOnlyOption = new Option<bool>("--ddl-only")
        {
            Description = "Print the DDL of the flat tables for the target provider and exit. " +
                          "No database is opened; only --assembly and --target-provider are needed."
        };

        flatCommand.Options.Add(sourceConnectionOption);
        flatCommand.Options.Add(sourceProviderOption);
        flatCommand.Options.Add(targetConnectionOption);
        flatCommand.Options.Add(targetProviderOption);
        flatCommand.Options.Add(assemblyOption);
        flatCommand.Options.Add(typesOption);
        flatCommand.Options.Add(ddlOnlyOption);
        flatCommand.Options.Add(verboseOption);
        flatCommand.Options.Add(batchSizeOption);

        flatCommand.SetAction(async (parseResult, ct) =>
        {
            var sourceConnection = parseResult.GetValue(sourceConnectionOption);
            var sourceProvider = parseResult.GetValue(sourceProviderOption);
            var targetConnection = parseResult.GetValue(targetConnectionOption);
            var targetProvider = parseResult.GetValue(targetProviderOption)!;
            var assemblyPaths = parseResult.GetValue(assemblyOption)!;
            var typesStr = parseResult.GetValue(typesOption);
            var ddlOnly = parseResult.GetValue(ddlOnlyOption);
            var verbose = parseResult.GetValue(verboseOption);
            var batchSize = parseResult.GetValue(batchSizeOption);

            return await FlatAsync(sourceConnection, sourceProvider, targetConnection, targetProvider,
                            assemblyPaths, typesStr, ddlOnly, verbose, batchSize, ct);
        });

        rootCommand.Subcommands.Add(flatCommand);

        return await rootCommand.Parse(args).InvokeAsync();
    }

    // === Flat Export ===

    private static async Task<int> FlatAsync(
        string? sourceConnection,
        string? sourceProvider,
        string? targetConnection,
        string targetProvider,
        string[] assemblyPaths,
        string? typesStr,
        bool ddlOnly,
        bool verbose,
        int batchSize,
        CancellationToken ct)
    {
        try
        {
            var propsTypes = LoadPropsTypes(assemblyPaths, typesStr);

            await using var target = ProviderFactory.Create(targetProvider);

            if (ddlOnly)
            {
                // Built without the source, so without structure ids: the shape of the tables does
                // not depend on them. Nothing but the script goes to stdout.
                var model = new FlatModelBuilder().Build(propsTypes);
                Console.WriteLine(new FlatDdlGenerator(target.Name).Script(model));
                return 0;
            }

            if (string.IsNullOrWhiteSpace(sourceConnection) ||
                string.IsNullOrWhiteSpace(sourceProvider) ||
                string.IsNullOrWhiteSpace(targetConnection))
            {
                throw new ArgumentException(
                    "--source-connection, --source-provider and --target-connection are required unless --ddl-only is given.");
            }

            Console.WriteLine("REDB Flat Export");
            Console.WriteLine("================");
            Console.WriteLine();

            await using var source = ProviderFactory.Create(sourceProvider);

            if (verbose)
            {
                Console.WriteLine($"Props types ({propsTypes.Count}): {string.Join(", ", propsTypes.Select(t => t.Name))}");
                Console.WriteLine($"Connecting to source ({sourceProvider}) and target ({targetProvider})...");
            }

            await source.OpenAsync(sourceConnection, ct);
            await target.OpenAsync(targetConnection, ct);

            if (verbose)
            {
                Console.WriteLine("Connected.");
                Console.WriteLine();
            }

            var flatExportService = new FlatExportService(source, target, verbose, batchSize);
            await flatExportService.ExportAsync(propsTypes, ct);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("Flat export completed successfully.");
            Console.ResetColor();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();

            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine(ex.StackTrace);
            }

            return 1;
        }

        return 0;
    }

    /// <summary>
    /// Loads the Props classes to unfold from the user assemblies: the ones named in
    /// <paramref name="typesStr"/> (by name or full name), or every class marked with
    /// <see cref="RedbSchemeAttribute"/> when none is named. The classes are looked up across all
    /// the given assemblies, so a scheme and the scheme it references may live in different projects.
    /// </summary>
    private static IReadOnlyList<Type> LoadPropsTypes(string[] assemblyPaths, string? typesStr)
    {
        var classes = new List<Type>();
        foreach (var assemblyPath in assemblyPaths)
        {
            var fullPath = Path.GetFullPath(assemblyPath);
            if (!File.Exists(fullPath))
                throw new FileNotFoundException($"Assembly not found: {fullPath}");

            // LoadFrom resolves the assembly's own dependencies from the folder it lies in.
            classes.AddRange(Assembly.LoadFrom(fullPath).GetTypes().Where(t => t.IsClass && !t.IsAbstract));
        }

        var where = string.Join(", ", assemblyPaths.Select(Path.GetFileName));

        if (string.IsNullOrWhiteSpace(typesStr))
        {
            var marked = classes
                .Where(t => t.GetCustomAttribute<RedbSchemeAttribute>() != null)
                .OrderBy(t => t.FullName, StringComparer.Ordinal)
                .ToList();

            if (marked.Count == 0)
                throw new InvalidOperationException(
                    $"No class marked with [RedbScheme] in {where}. Name the Props classes with --types.");

            return marked;
        }

        var selected = new List<Type>();
        foreach (var name in typesStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var matches = classes.Where(t => t.FullName == name || t.Name == name).ToList();
            if (matches.Count == 0)
                throw new InvalidOperationException($"Class '{name}' not found in {where}.");
            if (matches.Count > 1)
                throw new InvalidOperationException(
                    $"Class name '{name}' is ambiguous in {where}: {string.Join(", ", matches.Select(t => t.AssemblyQualifiedName))}. Use the full name.");

            selected.Add(matches[0]);
        }

        return selected;
    }
    
    private static async Task<int> ExportAsync(
        string connection,
        string provider,
        string output,
        bool compress,
        long[]? schemeIds,
        bool verbose,
        int batchSize,
        bool dryRun,
        CancellationToken ct)
    {
        Console.WriteLine("REDB Export");
        Console.WriteLine("===========");
        Console.WriteLine();
        
        await using var dataProvider = ProviderFactory.Create(provider);
        
        try
        {
            if (verbose)
            {
                Console.WriteLine($"Connecting to {provider}...");
            }
            
            await dataProvider.OpenAsync(connection, ct);
            
            if (verbose)
            {
                Console.WriteLine("Connected.");
                Console.WriteLine();
            }
            
            var exportService = new ExportService(dataProvider, verbose, batchSize);
            await exportService.ExportAsync(output, schemeIds, compress, dryRun, ct);
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
            
            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine(ex.StackTrace);
            }
            
            return 1;
        }

        return 0;
    }
    
    private static async Task<int> ImportAsync(
        string connection,
        string provider,
        string input,
        bool clean,
        bool verbose,
        int batchSize,
        bool dryRun,
        int parallel,
        CancellationToken ct)
    {
        Console.WriteLine("REDB Import");
        Console.WriteLine("===========");
        Console.WriteLine();
        
        await using var dataProvider = ProviderFactory.Create(provider);
        
        try
        {
            if (verbose)
            {
                Console.WriteLine($"Connecting to {provider}...");
            }
            
            await dataProvider.OpenAsync(connection, ct);
            
            if (verbose)
            {
                Console.WriteLine("Connected.");
                Console.WriteLine();
            }
            
            var importService = new ImportService(dataProvider, verbose, batchSize, parallel);
            await importService.ImportAsync(input, clean, dryRun, ct);
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
            
            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine(ex.StackTrace);
            }
            
            return 1;
        }

        return 0;
    }
    
    // === Schema Management Commands ===
    
    private static string GetEmbeddedSql(string providerName)
    {
        var (assembly, resourceName) = providerName.ToLowerInvariant() switch
        {
            "postgres" or "postgresql" or "pgsql" =>
                (typeof(redb.Postgres.RedbService).Assembly, "redb.Postgres.sql.redb_init.sql"),
            "mssql" or "sqlserver" =>
                (typeof(redb.MSSql.RedbService).Assembly, "redb.MSSql.sql.redb_init.sql"),
            "sqlite" =>
                (typeof(redb.SQLite.RedbService).Assembly, "redb.SQLite.sql.redbSqlite.sql"),
            _ => throw new ArgumentException($"Unknown provider: {providerName}")
        };
        
        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{resourceName}' not found in {assembly.GetName().Name}.");
        
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
    
    private static bool IsMssql(string providerName)
        => providerName.ToLowerInvariant() is "mssql" or "sqlserver";
    
    private static async Task<int> InitSchemaAsync(
        string connection, string provider, bool verbose, CancellationToken ct)
    {
        Console.WriteLine("REDB Schema Init");
        Console.WriteLine("=================");
        Console.WriteLine();
        
        await using var dataProvider = ProviderFactory.Create(provider);
        
        try
        {
            if (verbose)
                Console.WriteLine($"Connecting to {provider}...");
            
            await dataProvider.OpenAsync(connection, ct);
            
            if (verbose)
                Console.WriteLine("Connected. Reading schema script...");
            
            var sql = GetEmbeddedSql(provider);
            
            if (verbose)
                Console.WriteLine($"Schema script: {sql.Length:N0} characters.");
            
            Console.WriteLine("Executing schema script...");
            
            if (IsMssql(provider))
            {
                // MSSQL: split by GO batch separator
                var batches = Regex.Split(sql, @"^\s*GO\s*$",
                    RegexOptions.Multiline | RegexOptions.IgnoreCase);
                
                var executed = 0;
                foreach (var batch in batches)
                {
                    var trimmed = batch.Trim();
                    if (string.IsNullOrEmpty(trimmed)) continue;
                    
                    await using var cmd = dataProvider.Connection.CreateCommand();
                    cmd.CommandText = trimmed;
                    cmd.CommandTimeout = 300;
                    await cmd.ExecuteNonQueryAsync(ct);
                    executed++;
                }
                
                if (verbose)
                    Console.WriteLine($"Executed {executed} SQL batches.");
            }
            else
            {
                await using var cmd = dataProvider.Connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.CommandTimeout = 300;
                await cmd.ExecuteNonQueryAsync(ct);
            }
            
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("REDB schema created successfully.");
            Console.ResetColor();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
            
            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine(ex.StackTrace);
            }
            
            return 1;
        }

        return 0;
    }
    
    private static void ExportSchemaScript(string provider, string? outputPath, bool upgrade = false)
    {
        var sql = upgrade ? GetEmbeddedUpgradeSql(provider) : GetEmbeddedSql(provider);
        var kind = upgrade ? "Upgrade" : "Schema";

        if (string.IsNullOrEmpty(outputPath))
        {
            Console.Write(sql);
        }
        else
        {
            File.WriteAllText(outputPath, sql);
            Console.WriteLine($"{kind} script written to {outputPath} ({sql.Length:N0} characters).");
        }
    }

    /// <summary>
    /// The versioned SQL module bundle — what start-up applies automatically when the connected role
    /// may, and what the DBA applies by hand when it may not (RedbSchemaOutdatedException names this
    /// command). Same embedded resource the provider's RedbService reads, so the two texts cannot
    /// drift.
    /// </summary>
    private static string GetEmbeddedUpgradeSql(string providerName)
    {
        var (assembly, resourceName) = providerName.ToLowerInvariant() switch
        {
            "postgres" or "postgresql" or "pgsql" =>
                (typeof(redb.Postgres.RedbService).Assembly, "redb.Postgres.sql.v2-pvt.pvt_bundle.sql"),
            "mssql" or "sqlserver" =>
                (typeof(redb.MSSql.RedbService).Assembly, "redb.MSSql.sql.v2-pvt.pvt_bundle.sql"),
            "sqlite" =>
                throw new ArgumentException(
                    "SQLite has no upgrade script: the database file belongs to the process and is upgraded on open."),
            _ => throw new ArgumentException($"Unknown provider: {providerName}")
        };

        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{resourceName}' not found in {assembly.GetName().Name}.");

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
