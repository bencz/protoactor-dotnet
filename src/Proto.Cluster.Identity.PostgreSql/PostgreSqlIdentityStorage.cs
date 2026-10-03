using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using Proto.Utils;

namespace Proto.Cluster.Identity.PostgreSql;

/// <summary>
///     <see cref="IIdentityStorage" /> backed by a PostgreSQL table, for use with <see cref="IdentityStorageLookup" />.
/// </summary>
public sealed class PostgreSqlIdentityStorage : IIdentityStorage
{
    private static readonly ILogger Logger = Log.CreateLogger<PostgreSqlIdentityStorage>();

    private readonly AsyncSemaphore _asyncSemaphore;
    private readonly string _clusterName;
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlIdentityStorageOptions _options;
    private readonly PostgreSqlIdentitySql _sql;

    /// <param name="clusterName">Name of the cluster; several clusters can share the same table</param>
    /// <param name="dataSource">PostgreSQL data source (connection pool)</param>
    /// <param name="options">Table location and lock behavior</param>
    public PostgreSqlIdentityStorage(
        string clusterName,
        NpgsqlDataSource dataSource,
        PostgreSqlIdentityStorageOptions? options = null
    )
    {
        _clusterName = clusterName;
        _dataSource = dataSource;
        _options = options ?? new PostgreSqlIdentityStorageOptions();
        _sql = new PostgreSqlIdentitySql(_options);
        _asyncSemaphore = new AsyncSemaphore(_options.MaxConcurrency);
    }

    /// <summary>
    ///     The script that creates the activation table and its index, for schemas managed by migrations.
    /// </summary>
    public static string CreateSchemaSql(PostgreSqlIdentityStorageOptions? options = null) =>
        new PostgreSqlIdentitySql(options ?? new PostgreSqlIdentityStorageOptions()).CreateSchema;

    public async Task Init()
    {
        if (!_options.CreateSchema)
        {
            return;
        }

        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);

        // Members start concurrently; CREATE ... IF NOT EXISTS is not safe against itself, so serialize the setup
        await using (var lockCommand = new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtext($1))", connection, transaction))
        {
            lockCommand.Parameters.Add(new NpgsqlParameter { Value = _sql.CreateSchema });
            await lockCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await using (var createCommand = new NpgsqlCommand(_sql.CreateSchema, connection, transaction))
        {
            await createCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        await transaction.CommitAsync().ConfigureAwait(false);
    }

    public async Task<SpawnLock?> TryAcquireLock(ClusterIdentity clusterIdentity, CancellationToken ct)
    {
        var lockId = Guid.NewGuid().ToString("N");

        // The primary key makes the insert fail if a lock or an activation already exists
        var inserted = await ExecuteAsync(_sql.TryAcquireLock, ct, Key(clusterIdentity, lockId)).ConfigureAwait(false);

        return inserted == 1 ? new SpawnLock(lockId, clusterIdentity) : null;
    }

    public async Task<StoredActivation?> WaitForActivation(ClusterIdentity clusterIdentity, CancellationToken ct)
    {
        var entry = await LookupAsync(clusterIdentity, ct).ConfigureAwait(false);
        var lockId = entry?.LockedBy;

        if (lockId != null)
        {
            //There is an active lock on the identity, spin wait with incremental backoff until the lock is considered stale
            var timer = Stopwatch.StartNew();
            var i = 1;

            do
            {
                // Back off a little more on every check, capped so a released lock is noticed quickly
                await Task.Delay(Math.Min(20 * i++, 200), ct).ConfigureAwait(false);
            } while ((entry = await LookupAsync(clusterIdentity, ct).ConfigureAwait(false))?.LockedBy == lockId &&
                     timer.Elapsed < _options.MaxWaitBeforeStaleLock);
        }

        if (entry == null)
        {
            return null;
        }

        if (entry.LockedBy == null)
        {
            return entry.ToActivation();
        }

        //Still locked but not by the same request that originally locked it, so not stale
        if (entry.LockedBy != lockId)
        {
            return null;
        }

        //Stale lock, remove it and let the cluster retry
        Logger.RemovingStaleLock(lockId!, clusterIdentity);
        await RemoveLock(new SpawnLock(lockId!, clusterIdentity), CancellationToken.None).ConfigureAwait(false);

        return null;
    }

    public Task RemoveLock(SpawnLock spawnLock, CancellationToken ct) =>
        ExecuteAsync(_sql.RemoveLock, ct, Key(spawnLock.ClusterIdentity, spawnLock.LockId));

    public async Task StoreActivation(string memberId, SpawnLock spawnLock, PID pid, CancellationToken ct)
    {
        Logger.StoringActivation(memberId, spawnLock.ClusterIdentity, pid);

        var updated = await ExecuteAsync(_sql.StoreActivation, ct,
            _clusterName, spawnLock.ClusterIdentity.Kind, spawnLock.ClusterIdentity.Identity, spawnLock.LockId,
            memberId, pid.Address, pid.Id
        ).ConfigureAwait(false);

        if (updated != 1)
        {
            throw new LockNotFoundException($"Failed to store activation of {pid}");
        }
    }

    public Task RemoveActivation(ClusterIdentity clusterIdentity, PID pid, CancellationToken ct)
    {
        Logger.RemovingActivation(clusterIdentity, pid);

        return ExecuteAsync(_sql.RemoveActivation, ct,
            _clusterName, clusterIdentity.Kind, clusterIdentity.Identity, pid.Address, pid.Id);
    }

    public Task RemoveMember(string memberId, CancellationToken ct) =>
        ExecuteAsync(_sql.RemoveMember, ct, _clusterName, memberId);

    public async Task<IReadOnlyCollection<string>> GetMemberIds(CancellationToken ct) =>
        await _asyncSemaphore.WaitAsync(async () =>
            {
                await using var command = _dataSource.CreateCommand(_sql.GetMemberIds);
                command.Parameters.Add(new NpgsqlParameter { Value = _clusterName });
                await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);

                var memberIds = new List<string>();

                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    memberIds.Add(reader.GetString(0));
                }

                return (IReadOnlyCollection<string>)memberIds;
            }
        ).ConfigureAwait(false);

    public async Task<StoredActivation?> TryGetExistingActivation(ClusterIdentity clusterIdentity, CancellationToken ct)
    {
        var entry = await LookupAsync(clusterIdentity, ct).ConfigureAwait(false);

        return entry?.LockedBy == null ? entry?.ToActivation() : null;
    }

    public void Dispose()
    {
    }

    private object[] Key(ClusterIdentity clusterIdentity, string lockId) =>
        new object[] { _clusterName, clusterIdentity.Kind, clusterIdentity.Identity, lockId };

    private Task<int> ExecuteAsync(string sql, CancellationToken ct, params object[] parameters) =>
        _asyncSemaphore.WaitAsync(async () =>
            {
                await using var command = _dataSource.CreateCommand(sql);

                foreach (var parameter in parameters)
                {
                    command.Parameters.Add(new NpgsqlParameter { Value = parameter });
                }

                return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        );

    private Task<ActivationEntry?> LookupAsync(ClusterIdentity clusterIdentity, CancellationToken ct) =>
        _asyncSemaphore.WaitAsync(async () =>
            {
                await using var command = _dataSource.CreateCommand(_sql.Lookup);
                command.Parameters.Add(new NpgsqlParameter { Value = _clusterName });
                command.Parameters.Add(new NpgsqlParameter { Value = clusterIdentity.Kind });
                command.Parameters.Add(new NpgsqlParameter { Value = clusterIdentity.Identity });
                await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);

                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    return null;
                }

                return new ActivationEntry(
                    reader.IsDBNull(0) ? null : reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3)
                );
            }
        );

    private sealed record ActivationEntry(string? LockedBy, string? MemberId, string? Address, string? PidId)
    {
        public StoredActivation? ToActivation() =>
            MemberId is null || Address is null || PidId is null
                ? null
                : new StoredActivation(MemberId, PID.FromAddress(Address, PidId));
    }
}
