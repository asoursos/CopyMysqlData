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
}
