using System.Diagnostics;
using CopyMysqlData;
using MySqlConnector;

Console.WriteLine("=== MySQL Table Data Copier ===\n");

string sourceConnectionString;
string destinationConnectionString;
string tableName;
bool truncate;
bool preserveIdentity;

if (args.Length >= 3)
{
    sourceConnectionString = args[0];
    destinationConnectionString = args[1];
    tableName = args[2];
    truncate = args.Contains("--truncate", StringComparer.OrdinalIgnoreCase);
    preserveIdentity = !args.Contains("--no-preserve-identity", StringComparer.OrdinalIgnoreCase);
}
else
{
    Console.Write("Source connection string      : ");
    sourceConnectionString = Console.ReadLine() ?? string.Empty;

    Console.Write("Destination connection string : ");
    destinationConnectionString = Console.ReadLine() ?? string.Empty;

    Console.Write("Table name                    : ");
    tableName = Console.ReadLine() ?? string.Empty;

    Console.Write("Truncate destination table before copy? (y/N): ");
    truncate = Console.ReadLine()?.Trim().Equals("y", StringComparison.OrdinalIgnoreCase) ?? false;

    Console.Write("Preserve original IDs (auto-increment)? (Y/n): ");
    preserveIdentity = !(Console.ReadLine()?.Trim().Equals("n", StringComparison.OrdinalIgnoreCase) ?? false);
}

if (string.IsNullOrWhiteSpace(sourceConnectionString) ||
    string.IsNullOrWhiteSpace(destinationConnectionString) ||
    string.IsNullOrWhiteSpace(tableName))
{
    Console.Error.WriteLine("Error: source connection string, destination connection string, and table name are all required.");
    return 1;
}

Console.WriteLine($"\nCopying table `{tableName}`...\n");
var stopwatch = Stopwatch.StartNew();

try
{
    var copier = new TableCopier(sourceConnectionString, destinationConnectionString);
    int rowsCopied = await copier.CopyTableAsync(tableName, truncate, preserveIdentity);
    stopwatch.Stop();
    Console.WriteLine($"\nDone! {rowsCopied} row(s) copied in {stopwatch.Elapsed.TotalSeconds:F2}s.");
    return 0;
}
catch (MySqlException ex)
{
    Console.Error.WriteLine($"\nMySQL Error [{ex.ErrorCode}]: {ex.Message}");
    return 1;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"\nError: {ex.Message}");
    return 1;
}
