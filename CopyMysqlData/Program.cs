using CopyMysqlData;
using System.Diagnostics;

Console.WriteLine("=== MySQL Table Data Copier ===\n");

string command;
string sourceConnectionString;
string destinationConnectionString;
string[] tableNames;
bool truncate = false;
bool preserveIdentity = true;

if (args.Length >= 4)
{
    command = args[0];
    sourceConnectionString = args[1];
    destinationConnectionString = args[2];
    tableNames = args[3].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    truncate = args.Contains("--truncate", StringComparer.OrdinalIgnoreCase);
    preserveIdentity = !args.Contains("--no-preserve-identity", StringComparer.OrdinalIgnoreCase);
}
else
{
    Console.Write("Command (copy-tables / check-data)           : ");
    command = Console.ReadLine()?.Trim() ?? string.Empty;

    Console.Write("Source connection string                     : ");
    sourceConnectionString = Console.ReadLine() ?? string.Empty;

    Console.Write("Destination connection string                : ");
    destinationConnectionString = Console.ReadLine() ?? string.Empty;

    Console.Write("Table name(s) (comma-separated)              : ");
    tableNames = (Console.ReadLine() ?? string.Empty)
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    if (command.Equals("copy-tables", StringComparison.OrdinalIgnoreCase))
    {
        Console.Write("Truncate destination table before copy? (y/N): ");
        truncate = Console.ReadLine()?.Trim().Equals("y", StringComparison.OrdinalIgnoreCase) ?? false;

        Console.Write("Preserve original IDs (auto-increment)? (Y/n): ");
        preserveIdentity = !(Console.ReadLine()?.Trim().Equals("n", StringComparison.OrdinalIgnoreCase) ?? false);
    }
}

if (string.IsNullOrWhiteSpace(command) ||
    string.IsNullOrWhiteSpace(sourceConnectionString) ||
    string.IsNullOrWhiteSpace(destinationConnectionString) ||
    tableNames.Length == 0)
{
    Console.Error.WriteLine("Error: command, source connection string, destination connection string, and at least one table name are all required.");
    return 1;
}

if (!command.Equals("copy-tables", StringComparison.OrdinalIgnoreCase) &&
    !command.Equals("check-data", StringComparison.OrdinalIgnoreCase))
{
    Console.Error.WriteLine($"Error: unknown command '{command}'. Valid commands are: copy-tables, check-data.");
    return 1;
}

var copier = new TableCopier(sourceConnectionString, destinationConnectionString);
return await copier.ExecuteAsync(command, tableNames, truncate, preserveIdentity);
