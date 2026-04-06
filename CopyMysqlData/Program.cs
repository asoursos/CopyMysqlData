using System.Diagnostics;
using CopyMysqlData;
using MySqlConnector;

Console.WriteLine("=== MySQL Table Data Copier ===\n");

string sourceConnectionString;
string destinationConnectionString;
string[] tableNames;
bool truncate;
bool preserveIdentity;

if (args.Length >= 3)
{
    sourceConnectionString = args[0];
    destinationConnectionString = args[1];
    tableNames = args[2].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    truncate = args.Contains("--truncate", StringComparer.OrdinalIgnoreCase);
    preserveIdentity = !args.Contains("--no-preserve-identity", StringComparer.OrdinalIgnoreCase);
}
else
{
    Console.Write("Source connection string          : ");
    sourceConnectionString = Console.ReadLine() ?? string.Empty;

    Console.Write("Destination connection string     : ");
    destinationConnectionString = Console.ReadLine() ?? string.Empty;

    Console.Write("Table name(s) (comma-separated)   : ");
    tableNames = (Console.ReadLine() ?? string.Empty)
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    Console.Write("Truncate destination table before copy? (y/N): ");
    truncate = Console.ReadLine()?.Trim().Equals("y", StringComparison.OrdinalIgnoreCase) ?? false;

    Console.Write("Preserve original IDs (auto-increment)? (Y/n): ");
    preserveIdentity = !(Console.ReadLine()?.Trim().Equals("n", StringComparison.OrdinalIgnoreCase) ?? false);
}

if (string.IsNullOrWhiteSpace(sourceConnectionString) ||
    string.IsNullOrWhiteSpace(destinationConnectionString) ||
    tableNames.Length == 0)
{
    Console.Error.WriteLine("Error: source connection string, destination connection string, and at least one table name are all required.");
    return 1;
}

var copier = new TableCopier(sourceConnectionString, destinationConnectionString);
var stopwatch = Stopwatch.StartNew();
int totalRowsCopied = 0;
int exitCode = 0;

foreach (var tableName in tableNames)
{
    Console.WriteLine($"\nCopying table `{tableName}`...\n");
    try
    {
        int rowsCopied = await copier.CopyTableAsync(tableName, truncate, preserveIdentity);
        totalRowsCopied += rowsCopied;
        Console.WriteLine($"  Table `{tableName}`: {rowsCopied} row(s) copied.");
    }
    catch (MySqlException ex)
    {
        Console.Error.WriteLine($"\nMySQL Error [{ex.ErrorCode}] on table `{tableName}`: {ex.Message}");
        exitCode = 1;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"\nError on table `{tableName}`: {ex.Message}");
        exitCode = 1;
    }
}

stopwatch.Stop();
Console.WriteLine($"\nDone! {totalRowsCopied} total row(s) copied across {tableNames.Length} table(s) in {stopwatch.Elapsed.TotalSeconds:F2}s.");
return exitCode;
