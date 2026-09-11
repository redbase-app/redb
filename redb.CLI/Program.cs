using System.CommandLine;
using System.Data.Common;
using System.Reflection;
using System.Text.RegularExpressions;
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
        var rootCommand = new RootCommand("REDB CLI - Database export/import tool");
        
        // Common options
        var connectionOption = new Option<string>(
            aliases: ["--connection", "-c"],
            description: "Database connection string")
        { IsRequired = true };
        
        var providerOption = new Option<string>(
            aliases: ["--provider", "-p"],
            description: "Database provider: postgres | mssql | oracle | sqlite")
        { IsRequired = true };
        
        var verboseOption = new Option<bool>(
            aliases: ["--verbose", "-v"],
            description: "Enable verbose output");
        
        var batchSizeOption = new Option<int>(
            aliases: ["--batch-size"],
            getDefaultValue: () => 1000,
            description: "Batch size for operations (default: 1000)");
        
        var dryRunOption = new Option<bool>(
            aliases: ["--dry-run"],
            description: "Show what would be done without making changes");
        
        // Export command
        var exportCommand = new Command("export", "Export database to .redb file");
        
        var outputOption = new Option<string>(
            aliases: ["--output", "-o"],
            description: "Output file path (.redb)")
        { IsRequired = true };
        
        var compressOption = new Option<bool>(
            aliases: ["--compress"],
            description: "Compress output file with ZIP");
        
        var schemesOption = new Option<string?>(
            aliases: ["--schemes"],
            description: "Export only specified scheme IDs (comma-separated): --schemes 100,200,300");
        
        exportCommand.AddOption(connectionOption);
        exportCommand.AddOption(providerOption);
        exportCommand.AddOption(outputOption);
        exportCommand.AddOption(compressOption);
        exportCommand.AddOption(schemesOption);
        exportCommand.AddOption(verboseOption);
        exportCommand.AddOption(batchSizeOption);
        exportCommand.AddOption(dryRunOption);
        
        exportCommand.SetHandler(async (context) =>
        {
            var connection = context.ParseResult.GetValueForOption(connectionOption)!;
            var provider = context.ParseResult.GetValueForOption(providerOption)!;
            var output = context.ParseResult.GetValueForOption(outputOption)!;
            var compress = context.ParseResult.GetValueForOption(compressOption);
            var schemesStr = context.ParseResult.GetValueForOption(schemesOption);
            var verbose = context.ParseResult.GetValueForOption(verboseOption);
            var batchSize = context.ParseResult.GetValueForOption(batchSizeOption);
            var dryRun = context.ParseResult.GetValueForOption(dryRunOption);
            
            long[]? schemeIds = null;
            if (!string.IsNullOrWhiteSpace(schemesStr))
            {
                schemeIds = schemesStr.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => long.Parse(s.Trim()))
                    .ToArray();
            }
            
            await ExportAsync(connection, provider, output, compress, schemeIds, 
                              verbose, batchSize, dryRun, context.GetCancellationToken());
        });
        
        // Import command
        var importCommand = new Command("import", "Import database from .redb file");
        
        var inputOption = new Option<string>(
            aliases: ["--input", "-i"],
            description: "Input file path (.redb)")
        { IsRequired = true };
        
        var cleanOption = new Option<bool>(
            aliases: ["--clean"],
            description: "Clean database before import");
        
        importCommand.AddOption(connectionOption);
        importCommand.AddOption(providerOption);
        importCommand.AddOption(inputOption);
        importCommand.AddOption(cleanOption);
        importCommand.AddOption(verboseOption);
        importCommand.AddOption(batchSizeOption);
        importCommand.AddOption(dryRunOption);
        
        importCommand.SetHandler(async (context) =>
        {
            var connection = context.ParseResult.GetValueForOption(connectionOption)!;
            var provider = context.ParseResult.GetValueForOption(providerOption)!;
            var input = context.ParseResult.GetValueForOption(inputOption)!;
            var clean = context.ParseResult.GetValueForOption(cleanOption);
            var verbose = context.ParseResult.GetValueForOption(verboseOption);
            var batchSize = context.ParseResult.GetValueForOption(batchSizeOption);
            var dryRun = context.ParseResult.GetValueForOption(dryRunOption);
            
            await ImportAsync(connection, provider, input, clean, 
                              verbose, batchSize, dryRun, context.GetCancellationToken());
        });
        
        rootCommand.AddCommand(exportCommand);
        rootCommand.AddCommand(importCommand);
        
        // Init command — create REDB schema in a database
        var initCommand = new Command("init", "Create REDB schema in an existing database");
        initCommand.AddOption(connectionOption);
        initCommand.AddOption(providerOption);
        initCommand.AddOption(verboseOption);
        
        initCommand.SetHandler(async (context) =>
        {
            var connection = context.ParseResult.GetValueForOption(connectionOption)!;
            var provider = context.ParseResult.GetValueForOption(providerOption)!;
            var verbose = context.ParseResult.GetValueForOption(verboseOption);
            
            await InitSchemaAsync(connection, provider, verbose, context.GetCancellationToken());
        });
        
        // Schema command — export REDB schema SQL to stdout or file
        var schemaCommand = new Command("schema", "Export REDB schema SQL script");
        schemaCommand.AddOption(providerOption);
        
        var schemaOutputOption = new Option<string?>(
            aliases: ["--output", "-o"],
            description: "Output file path. If omitted, writes to stdout.");
        schemaCommand.AddOption(schemaOutputOption);

        var schemaUpgradeOption = new Option<bool>(
            aliases: ["--upgrade"],
            description: "Export the upgrade script for an EXISTING database (the versioned SQL module) " +
                         "instead of the full schema. Hand it to the schema owner when the application " +
                         "is not allowed to apply it itself. Idempotent, safe to re-run. " +
                         "Not available for sqlite (the database file upgrades itself).");
        schemaCommand.AddOption(schemaUpgradeOption);

        schemaCommand.SetHandler(async (context) =>
        {
            var provider = context.ParseResult.GetValueForOption(providerOption)!;
            var output = context.ParseResult.GetValueForOption(schemaOutputOption);
            var upgrade = context.ParseResult.GetValueForOption(schemaUpgradeOption);

            await Task.CompletedTask;
            ExportSchemaScript(provider, output, upgrade);
        });
        
        rootCommand.AddCommand(initCommand);
        rootCommand.AddCommand(schemaCommand);
        
        return await rootCommand.InvokeAsync(args);
    }
    
    private static async Task ExportAsync(
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
            
            Environment.ExitCode = 1;
        }
    }
    
    private static async Task ImportAsync(
        string connection,
        string provider,
        string input,
        bool clean,
        bool verbose,
        int batchSize,
        bool dryRun,
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
            
            var importService = new ImportService(dataProvider, verbose, batchSize);
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
            
            Environment.ExitCode = 1;
        }
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
    
    private static async Task InitSchemaAsync(
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
            
            Environment.ExitCode = 1;
        }
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
