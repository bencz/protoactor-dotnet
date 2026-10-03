using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Npgsql;
using Proto.Cluster.Seed;

namespace Proto.Cluster.SeedNode.PostgreSql;

/// <summary>
///     Stores the seed node members in a PostgreSQL table. Every entry expires after
///     <see cref="PostgreSqlSeedNodeDiscoveryOptions.MemberTtl" /> unless the member that registered it keeps refreshing
///     it, so members that die without deregistering (crash, SIGKILL, lost node) disappear on their own.
/// </summary>
[PublicAPI]
public sealed class PostgreSqlSeedNodeDiscovery : ISeedNodeDiscovery, IDisposable
{
    private static readonly ILogger Logger = Log.CreateLogger<PostgreSqlSeedNodeDiscovery>();

    private readonly string _clusterName;
    private readonly NpgsqlDataSource _dataSource;
    private readonly ConcurrentDictionary<string, Heartbeat> _heartbeats = new();
    private readonly PostgreSqlSeedNodeDiscoveryOptions _options;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly PostgreSqlSeedNodeSql _sql;
    private volatile bool _schemaCreated;

    /// <param name="dataSource">PostgreSQL data source (connection pool)</param>
    /// <param name="clusterName">Name of the cluster; several clusters can share the same table</param>
    /// <param name="options">Table location and member expiration</param>
    public PostgreSqlSeedNodeDiscovery(
        NpgsqlDataSource dataSource,
        string clusterName,
        PostgreSqlSeedNodeDiscoveryOptions? options = null
    )
    {
        _dataSource = dataSource;
        _clusterName = clusterName;
        _options = options ?? new PostgreSqlSeedNodeDiscoveryOptions();
        _sql = new PostgreSqlSeedNodeSql(_options);
    }

    /// <summary>
    ///     The script that creates the seed member table, for schemas managed by migrations.
    /// </summary>
    public static string CreateSchemaSql(PostgreSqlSeedNodeDiscoveryOptions? options = null) =>
        new PostgreSqlSeedNodeSql(options ?? new PostgreSqlSeedNodeDiscoveryOptions()).CreateSchema;

    public async Task Register(string memberId, string host, int port)
    {
        await WriteEntryAsync(memberId, host, port, CancellationToken.None).ConfigureAwait(false);

        var heartbeat = new Heartbeat();
        heartbeat.Task = RefreshAsync(memberId, host, port, heartbeat.Cancellation.Token);

        if (_heartbeats.TryRemove(memberId, out var previous))
        {
            await previous.StopAsync().ConfigureAwait(false);
        }

        _heartbeats[memberId] = heartbeat;
    }

    public async Task Remove(string memberId)
    {
        // Stop refreshing first, so an in-flight refresh cannot write the entry back after it was deleted
        if (_heartbeats.TryRemove(memberId, out var heartbeat))
        {
            await heartbeat.StopAsync().ConfigureAwait(false);
        }

        await EnsureSchemaAsync().ConfigureAwait(false);
        await ExecuteAsync(_sql.Remove, CancellationToken.None, _clusterName, memberId).ConfigureAwait(false);
    }

    public async Task<(string memberId, string host, int port)[]> GetAll()
    {
        await EnsureSchemaAsync().ConfigureAwait(false);

        // Expired entries are left by members that died without deregistering; clean them up while reading
        await using var batch = _dataSource.CreateBatch();
        batch.BatchCommands.Add(Command(_sql.RemoveExpired, _clusterName));
        batch.BatchCommands.Add(Command(_sql.GetAll, _clusterName));

        await using var reader = await batch.ExecuteReaderAsync().ConfigureAwait(false);
        var members = new List<(string memberId, string host, int port)>();

        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            members.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
        }

        return members.ToArray();
    }

    /// <summary>
    ///     Stops refreshing the registered members. Their entries expire after the configured TTL.
    /// </summary>
    public void Dispose()
    {
        foreach (var memberId in _heartbeats.Keys)
        {
            if (_heartbeats.TryRemove(memberId, out var heartbeat))
            {
                heartbeat.Cancellation.Cancel();
                heartbeat.Cancellation.Dispose();
            }
        }
    }

    private async Task WriteEntryAsync(string memberId, string host, int port, CancellationToken ct)
    {
        await EnsureSchemaAsync().ConfigureAwait(false);
        await ExecuteAsync(_sql.Register, ct, _clusterName, memberId, host, port, _options.MemberTtl)
            .ConfigureAwait(false);
    }

    private async Task RefreshAsync(string memberId, string host, int port, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Refresh well before the entry expires, so a single failed write does not drop the member
                await Task.Delay(_options.MemberTtl / 3, ct).ConfigureAwait(false);
                await WriteEntryAsync(memberId, host, port, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                Logger.HeartbeatFailed(e, memberId);
            }
        }
    }

    private async Task EnsureSchemaAsync()
    {
        if (_schemaCreated || !_options.CreateSchema)
        {
            return;
        }

        await _schemaLock.WaitAsync().ConfigureAwait(false);

        try
        {
            if (_schemaCreated)
            {
                return;
            }

            await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);

            // Members start concurrently; CREATE ... IF NOT EXISTS is not safe against itself, so serialize the setup
            await using (var lockCommand =
                         new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtext($1))", connection, transaction))
            {
                lockCommand.Parameters.Add(new NpgsqlParameter { Value = _sql.CreateSchema });
                await lockCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            await using (var createCommand = new NpgsqlCommand(_sql.CreateSchema, connection, transaction))
            {
                await createCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

            await transaction.CommitAsync().ConfigureAwait(false);
            _schemaCreated = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private async Task ExecuteAsync(string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = _dataSource.CreateCommand(sql);

        foreach (var parameter in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = parameter });
        }

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static NpgsqlBatchCommand Command(string sql, params object[] parameters)
    {
        var command = new NpgsqlBatchCommand(sql);

        foreach (var parameter in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = parameter });
        }

        return command;
    }

    private sealed class Heartbeat
    {
        public CancellationTokenSource Cancellation { get; } = new();

        public Task Task { get; set; } = Task.CompletedTask;

        public async Task StopAsync()
        {
            Cancellation.Cancel();
            await Task.ConfigureAwait(false);
            Cancellation.Dispose();
        }
    }
}
