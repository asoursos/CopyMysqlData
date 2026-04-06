using System.Diagnostics;
using MySqlConnector;

namespace CopyMysqlData;

public sealed class TableCopier
{
    private readonly string _sourceConnectionString;
    private readonly string _destinationConnectionString;
    private const int BatchSize = 1_000;

    public TableCopier(string sourceConnectionString, string destinationConnectionString)
    {
        _sourceConnectionString = sourceConnectionString;
        _destinationConnectionString = destinationConnectionString;
    }

    public Task<int> ExecuteAsync(
        string command,
        string[] tableNames,
        bool truncate = false,
        bool preserveIdentity = true,
        CancellationToken cancellationToken = default)
    {
        return command.ToLowerInvariant() switch
        {
            "copy-tables" => ExecuteCopyTablesAsync(tableNames, truncate, preserveIdentity, cancellationToken),
            "check-data"  => ExecuteCheckDataAsync(tableNames, cancellationToken),
            _ => throw new ArgumentException($"Unknown command '{command}'.")
        };
    }

    private async Task<int> ExecuteCopyTablesAsync(
        string[] tableNames,
        bool truncate,
        bool preserveIdentity,
        CancellationToken cancellationToken)
    {
        int exitCode = 0;
        int totalRowsCopied = 0;
        var stopwatch = Stopwatch.StartNew();

        foreach (var tableName in tableNames)
        {
            Console.WriteLine($"\nCopying table `{tableName}`...\n");
            try
            {
                int rowsCopied = await CopyTableAsync(tableName, truncate, preserveIdentity, cancellationToken);
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
    }

    private async Task<int> ExecuteCheckDataAsync(
        string[] tableNames,
        CancellationToken cancellationToken)
    {
        int exitCode = 0;
        var stopwatch = Stopwatch.StartNew();

        foreach (var tableName in tableNames)
        {
            Console.WriteLine($"\nChecking table `{tableName}`...");
            try
            {
                var result = await CheckTableAsync(tableName, cancellationToken);

                if (!result.HasPrimaryKey)
                    Console.WriteLine("  (no primary key found \u2014 row-count comparison only)");

                Console.WriteLine($"  Source rows      : {result.SourceRowCount}");
                Console.WriteLine($"  Destination rows : {result.DestinationRowCount}");
                Console.WriteLine($"  Missing in dest  : {result.MissingInDestination}");
                Console.WriteLine($"  Extra in dest    : {result.ExtraInDestination}");
                Console.WriteLine($"  Mismatched rows  : {result.Mismatched}");
                Console.WriteLine($"  Status           : {(result.IsMatch ? "\u2713 MATCH" : "\u2717 MISMATCH")}");

                if (!result.IsMatch) exitCode = 1;
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
        Console.WriteLine($"\nDone! Checked {tableNames.Length} table(s) in {stopwatch.Elapsed.TotalSeconds:F2}s.");
        return exitCode;
    }

    public async Task<int> CopyTableAsync(
        string tableName,
        bool truncateDestination = false,
        bool preserveIdentity = true,
        CancellationToken cancellationToken = default)
    {
        await using var sourceConn = new MySqlConnection(_sourceConnectionString);
        await sourceConn.OpenAsync(cancellationToken);

        await using var destConn = new MySqlConnection(_destinationConnectionString);
        await destConn.OpenAsync(cancellationToken);

        if (truncateDestination)
        {
            await using var fkOffCmd = destConn.CreateCommand();
            fkOffCmd.CommandText = "SET FOREIGN_KEY_CHECKS = 0";
            await fkOffCmd.ExecuteNonQueryAsync(cancellationToken);

            await using var truncateCmd = destConn.CreateCommand();
            truncateCmd.CommandText = $"TRUNCATE TABLE `{tableName}`";
            await truncateCmd.ExecuteNonQueryAsync(cancellationToken);
            Console.WriteLine($"Destination table `{tableName}` truncated.");

            await using var fkOnCmd = destConn.CreateCommand();
            fkOnCmd.CommandText = "SET FOREIGN_KEY_CHECKS = 1";
            await fkOnCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var excludedColumns = await GetExcludedColumnsAsync(destConn, tableName, preserveIdentity, cancellationToken);

        await using var sourceCmd = sourceConn.CreateCommand();
        sourceCmd.CommandText = $"SELECT * FROM `{tableName}`";
        sourceCmd.CommandTimeout = 0;

        await using var reader = await sourceCmd.ExecuteReaderAsync(cancellationToken);

        var allColumnNames = Enumerable.Range(0, reader.FieldCount)
            .Select(i => reader.GetName(i))
            .ToList();

        var insertIndices = Enumerable.Range(0, allColumnNames.Count)
            .Where(i => !excludedColumns.Contains(allColumnNames[i]))
            .ToArray();

        var insertColumnNames = insertIndices.Select(i => allColumnNames[i]).ToList();

        int totalCopied = 0;
        var batch = new List<object?[]>(BatchSize);

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = new object?[insertIndices.Length];
            for (int i = 0; i < insertIndices.Length; i++)
                row[i] = reader.IsDBNull(insertIndices[i]) ? null : reader.GetValue(insertIndices[i]);

            batch.Add(row);

            if (batch.Count >= BatchSize)
            {
                await BatchInsertAsync(destConn, tableName, insertColumnNames, batch, cancellationToken);
                totalCopied += batch.Count;
                Console.WriteLine($"  {totalCopied} rows copied...");
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            await BatchInsertAsync(destConn, tableName, insertColumnNames, batch, cancellationToken);
            totalCopied += batch.Count;
        }

        return totalCopied;
    }

    private static async Task<HashSet<string>> GetExcludedColumnsAsync(
        MySqlConnection destConn,
        string tableName,
        bool preserveIdentity,
        CancellationToken cancellationToken)
    {
        var excluded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        await using var cmd = destConn.CreateCommand();
        cmd.CommandText = """
            SELECT COLUMN_NAME, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @tableName
            """;
        cmd.Parameters.AddWithValue("@tableName", tableName);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var columnName = reader.GetString(0);
            var extra = reader.GetString(1);

            // Generated columns (STORED/VIRTUAL) can never receive explicit values
            if (extra.Contains("STORED GENERATED", StringComparison.OrdinalIgnoreCase) ||
                extra.Contains("VIRTUAL GENERATED", StringComparison.OrdinalIgnoreCase))
            {
                excluded.Add(columnName);
            }
            // Auto-increment columns are excluded only when preserveIdentity is false
            else if (!preserveIdentity && extra.Contains("auto_increment", StringComparison.OrdinalIgnoreCase))
            {
                excluded.Add(columnName);
            }
        }

        return excluded;
    }

    private static async Task BatchInsertAsync(
        MySqlConnection destConn,
        string tableName,
        List<string> columnNames,
        List<object?[]> batch,
        CancellationToken cancellationToken)
    {
        var columnList = string.Join(", ", columnNames.Select(c => $"`{c}`"));

        var valuesClauses = new string[batch.Count];
        for (int r = 0; r < batch.Count; r++)
        {
            var paramNames = Enumerable.Range(0, columnNames.Count).Select(c => $"@r{r}c{c}");
            valuesClauses[r] = $"({string.Join(", ", paramNames)})";
        }

        var sql = $"INSERT INTO `{tableName}` ({columnList}) VALUES {string.Join(", ", valuesClauses)}";

        await using var transaction = await destConn.BeginTransactionAsync(cancellationToken);
        try
        {
            await using var cmd = destConn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = transaction;
            cmd.CommandTimeout = 0;

            for (int r = 0; r < batch.Count; r++)
                for (int c = 0; c < columnNames.Count; c++)
                    cmd.Parameters.AddWithValue($"@r{r}c{c}", batch[r][c] ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    public async Task<TableCheckResult> CheckTableAsync(
        string tableName,
        CancellationToken cancellationToken = default)
    {
        await using var sourceConn = new MySqlConnection(_sourceConnectionString);
        await sourceConn.OpenAsync(cancellationToken);
        await using var destConn = new MySqlConnection(_destinationConnectionString);
        await destConn.OpenAsync(cancellationToken);

        var pkColumns = await GetPrimaryKeyColumnsAsync(sourceConn, tableName, cancellationToken);

        if (pkColumns.Count == 0)
        {
            var srcCount = await GetRowCountAsync(sourceConn, tableName, cancellationToken);
            var dstCount = await GetRowCountAsync(destConn, tableName, cancellationToken);
            return new TableCheckResult(tableName, srcCount, dstCount,
                Math.Max(0, srcCount - dstCount), Math.Max(0, dstCount - srcCount), 0, HasPrimaryKey: false);
        }

        var orderBy = string.Join(", ", pkColumns.Select(c => $"`{c}`"));

        await using var sourceCmd = sourceConn.CreateCommand();
        sourceCmd.CommandText = $"SELECT * FROM `{tableName}` ORDER BY {orderBy}";
        sourceCmd.CommandTimeout = 0;

        await using var destCmd = destConn.CreateCommand();
        destCmd.CommandText = $"SELECT * FROM `{tableName}` ORDER BY {orderBy}";
        destCmd.CommandTimeout = 0;

        await using var sourceReader = await sourceCmd.ExecuteReaderAsync(cancellationToken);
        await using var destReader = await destCmd.ExecuteReaderAsync(cancellationToken);

        var pkIndices = pkColumns
            .Select(pk => Enumerable.Range(0, sourceReader.FieldCount)
                .First(i => string.Equals(sourceReader.GetName(i), pk, StringComparison.OrdinalIgnoreCase)))
            .ToArray();

        long sourceRowCount = 0, destRowCount = 0;
        long missingInDest = 0, extraInDest = 0, mismatched = 0;

        object?[]? sourceRow = null, destRow = null;

        if (await sourceReader.ReadAsync(cancellationToken)) { sourceRow = ReadRow(sourceReader); sourceRowCount++; }
        if (await destReader.ReadAsync(cancellationToken)) { destRow = ReadRow(destReader); destRowCount++; }

        while (sourceRow is not null || destRow is not null)
        {
            int cmp;
            if (sourceRow is null) cmp = 1;
            else if (destRow is null) cmp = -1;
            else cmp = CompareRowKeys(sourceRow, destRow, pkIndices);

            if (cmp == 0)
            {
                if (!RowsEqual(sourceRow!, destRow!)) mismatched++;
                sourceRow = null;
                if (await sourceReader.ReadAsync(cancellationToken)) { sourceRow = ReadRow(sourceReader); sourceRowCount++; }
                destRow = null;
                if (await destReader.ReadAsync(cancellationToken)) { destRow = ReadRow(destReader); destRowCount++; }
            }
            else if (cmp < 0)
            {
                missingInDest++;
                sourceRow = null;
                if (await sourceReader.ReadAsync(cancellationToken)) { sourceRow = ReadRow(sourceReader); sourceRowCount++; }
            }
            else
            {
                extraInDest++;
                destRow = null;
                if (await destReader.ReadAsync(cancellationToken)) { destRow = ReadRow(destReader); destRowCount++; }
            }
        }

        return new TableCheckResult(tableName, sourceRowCount, destRowCount, missingInDest, extraInDest, mismatched);
    }

    private static async Task<List<string>> GetPrimaryKeyColumnsAsync(
        MySqlConnection conn,
        string tableName,
        CancellationToken cancellationToken)
    {
        var pkColumns = new List<string>();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @tableName AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
            """;
        cmd.Parameters.AddWithValue("@tableName", tableName);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            pkColumns.Add(reader.GetString(0));
        return pkColumns;
    }

    private static async Task<long> GetRowCountAsync(
        MySqlConnection conn,
        string tableName,
        CancellationToken cancellationToken)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM `{tableName}`";
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(result);
    }

    private static object?[] ReadRow(MySqlDataReader reader)
    {
        var row = new object?[reader.FieldCount];
        for (int i = 0; i < row.Length; i++)
            row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
        return row;
    }

    private static int CompareRowKeys(object?[] row1, object?[] row2, int[] pkIndices)
    {
        foreach (var idx in pkIndices)
        {
            var v1 = row1[idx];
            var v2 = row2[idx];

            if (v1 is null && v2 is null) continue;
            if (v1 is null) return -1;
            if (v2 is null) return 1;

            if (v1 is IComparable comparable)
            {
                try
                {
                    int result = comparable.CompareTo(Convert.ChangeType(v2, v1.GetType()));
                    if (result != 0) return result;
                    continue;
                }
                catch { }
            }

            int strCmp = string.Compare(v1.ToString(), v2.ToString(), StringComparison.Ordinal);
            if (strCmp != 0) return strCmp;
        }
        return 0;
    }

    private static bool RowsEqual(object?[] row1, object?[] row2)
    {
        if (row1.Length != row2.Length) return false;
        for (int i = 0; i < row1.Length; i++)
        {
            var v1 = row1[i];
            var v2 = row2[i];
            if (v1 is null && v2 is null) continue;
            if (v1 is null || v2 is null) return false;
            if (!v1.Equals(v2)) return false;
        }
        return true;
    }
}

public record TableCheckResult(
    string TableName,
    long SourceRowCount,
    long DestinationRowCount,
    long MissingInDestination,
    long ExtraInDestination,
    long Mismatched,
    bool HasPrimaryKey = true)
{
    public bool IsMatch =>
        MissingInDestination == 0 && ExtraInDestination == 0 && Mismatched == 0;
}
