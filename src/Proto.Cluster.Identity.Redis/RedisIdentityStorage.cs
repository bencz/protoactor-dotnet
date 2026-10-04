// -----------------------------------------------------------------------
// <copyright file="RedisIdentityStorage.cs" company="Asynkron AB">
//      Copyright (C) 2015-2025 Asynkron AB All rights reserved
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Proto.Utils;
using StackExchange.Redis;

namespace Proto.Cluster.Identity.Redis;

public sealed class RedisIdentityStorage : IIdentityStorage
{
    private static readonly ILogger Logger = Log.CreateLogger<RedisIdentityStorage>();

    private static readonly RedisValue UniqueIdentity = "pid";
    private static readonly RedisValue Address = "adr";
    private static readonly RedisValue MemberId = "mid";
    private static readonly RedisValue LockId = "lid";

    // Members can own many activations, so they are removed in batches to keep each script short
    private const int RemoveMemberBatchSize = 500;

    // Pops a batch of activation keys from the member set and deletes the ones still owned by that member.
    // SPOP shrinks the set on every batch, so an interrupted removal continues where it stopped, and members removing
    // the same member concurrently split the work instead of repeating it. Once the set is empty the member is dropped
    // from the member registry.
    private const string RemoveMemberBatchScript = "local keys = redis.call('SPOP', KEYS[1], ARGV[2])\n" +
                                                   "if #keys == 0 then redis.call('SREM', KEYS[2], ARGV[1]) return 0 end\n" +
                                                   "for _, key in ipairs(keys) do\n" +
                                                   " if redis.call('HGET', key, 'mid') == ARGV[1] then redis.call('DEL', key) end\n" +
                                                   "end\n" +
                                                   "return #keys";
    private readonly AsyncSemaphore _asyncSemaphore;

    private readonly RedisKey _clusterIdentityKey;

    private readonly IConnectionMultiplexer _connections;
    private readonly TimeSpan _maxLockTime;
    private readonly RedisKey _memberKey;

    // Set of the ids of all members that own activations, so they can be listed without scanning the keyspace
    private readonly RedisKey _membersKey;

    // Marks that members stored before the registry existed were added to it (done once per cluster name)
    private readonly RedisKey _membersBackfilledKey;

    public RedisIdentityStorage(
        string clusterName,
        IConnectionMultiplexer connections,
        TimeSpan? maxWaitBeforeStaleLock = null,
        int maxConcurrency = 200
    )
    {
        RedisKey baseKey = clusterName + ":";
        _clusterIdentityKey = baseKey.Append("ci:");
        _memberKey = baseKey.Append("mb:");
        _membersKey = baseKey.Append("members");
        _membersBackfilledKey = baseKey.Append("members-backfilled");
        _connections = connections;
        _maxLockTime = maxWaitBeforeStaleLock ?? TimeSpan.FromSeconds(5);
        _asyncSemaphore = new AsyncSemaphore(maxConcurrency);
    }

    public async Task<SpawnLock?> TryAcquireLock(ClusterIdentity clusterIdentity, CancellationToken ct)
    {
        var requestId = Guid.NewGuid().ToString("N");

        var hasLock = await _asyncSemaphore.WaitAsync(() => TryAcquireLockAsync(clusterIdentity, requestId))
            .ConfigureAwait(false);

        return hasLock ? new SpawnLock(requestId, clusterIdentity) : null;
    }

    public async Task<StoredActivation?> WaitForActivation(
        ClusterIdentity clusterIdentity,
        CancellationToken ct
    )
    {
        var timer = Stopwatch.StartNew();
        var key = IdKey(clusterIdentity);
        var db = GetDb();

        var activationStatus = await LookupKey(db, key).ConfigureAwait(false);
        var lockId = activationStatus?.ActiveLockId;

        if (lockId != null)
        {
            //There is an active lock on the pid, spin wait
            var i = 1;

            do
            {
                // Incrementally back off while waiting for the lock to be released
                await Task.Delay(20 * i++, ct).ConfigureAwait(false);
            } while (!ct.IsCancellationRequested
                     && _maxLockTime > timer.Elapsed
                     && (activationStatus = await LookupKey(db, key).ConfigureAwait(false))?.ActiveLockId == lockId
                    );
        }

        //the lookup entity was lost, stale lock maybe?
        if (activationStatus == null)
        {
            return null;
        }

        //lookup was unlocked, return this pid
        if (activationStatus.Activation != null)
        {
            return activationStatus.Activation;
        }

        //Still locked but not by the same request that originally locked it, so not stale
        if (activationStatus.ActiveLockId != lockId)
        {
            return null;
        }

        //Stale lock. just delete it and let cluster retry
        await RemoveLock(new SpawnLock(lockId!, clusterIdentity), CancellationToken.None).ConfigureAwait(false);

        return null;
    }

    public Task RemoveLock(SpawnLock spawnLock, CancellationToken ct)
    {
        var db = GetDb();

        var key = IdKey(spawnLock.ClusterIdentity);
        var transaction = db.CreateTransaction();
        transaction.AddCondition(Condition.HashEqual(key, LockId, spawnLock.LockId));
        _ = transaction.HashDeleteAsync(key, LockId);

        return transaction.ExecuteAsync();
    }

    public async Task StoreActivation(
        string memberId,
        SpawnLock spawnLock,
        PID pid,
        CancellationToken ct
    )
    {
        var key = IdKey(spawnLock.ClusterIdentity);

        var values = new[]
        {
            new HashEntry(UniqueIdentity, pid.Id),
            new HashEntry(Address, pid.Address),
            new HashEntry(MemberId, memberId),
            new HashEntry(LockId, RedisValue.EmptyString)
        };

        var executed = await _asyncSemaphore.WaitAsync(() =>
                {
                    var db = GetDb();

                    var transaction = db.CreateTransaction();
                    transaction.AddCondition(Condition.HashEqual(key, LockId, spawnLock.LockId));
                    _ = transaction.HashSetAsync(key, values, CommandFlags.DemandMaster);
                    _ = transaction.SetAddAsync(MemberKey(memberId), key.ToString());
                    _ = transaction.SetAddAsync(_membersKey, memberId);
                    _ = transaction.KeyPersistAsync(key);

                    return transaction.ExecuteAsync();
                }
            )
            .ConfigureAwait(false);

        if (!executed)
        {
            throw new LockNotFoundException($"Failed to store activation of {pid}");
        }
    }

    public Task RemoveActivation(ClusterIdentity clusterIdentity, PID pid, CancellationToken ct)
    {
        Logger.RemovingActivation(clusterIdentity, pid);

        const string removePid = "local pidEntry = redis.call('HMGET', KEYS[1], 'pid', 'adr', 'mid');\n" +
                                 "if pidEntry[1]~=ARGV[1] or pidEntry[2]~=ARGV[2] then return 0 end;\n" + // id / address matches
                                 "local memberKey = ARGV[3] .. pidEntry[3];\n" +
                                 "redis.call('SREM', memberKey, KEYS[1] .. '');" +
                                 "return redis.call('DEL', KEYS[1]);";

        var key = IdKey(clusterIdentity);

        return _asyncSemaphore.WaitAsync(()
                =>
            {
                return GetDb()
                    .ScriptEvaluateAsync(removePid, new[] { key },
                        new RedisValue[] { pid.Id, pid.Address, _memberKey.ToString() }
                    );
            }
        );
    }

    public async Task RemoveMember(string memberId, CancellationToken ct)
    {
        var memberKey = MemberKey(memberId);
        long popped;

        do
        {
            ct.ThrowIfCancellationRequested();

            var result = await _asyncSemaphore.WaitAsync(() => GetDb().ScriptEvaluateAsync(
                    RemoveMemberBatchScript,
                    new[] { memberKey, _membersKey },
                    new RedisValue[] { memberId, RemoveMemberBatchSize }
                )
            ).ConfigureAwait(false);

            popped = (long)result;
        } while (popped > 0);
    }

    public async Task<IReadOnlyCollection<string>> GetMemberIds(CancellationToken ct)
    {
        var db = GetDb();

        if (!await db.KeyExistsAsync(_membersBackfilledKey).ConfigureAwait(false))
        {
            await BackfillMemberRegistryAsync(db, ct).ConfigureAwait(false);
        }

        var memberIds = await db.SetMembersAsync(_membersKey).ConfigureAwait(false);

        return memberIds.Select(memberId => memberId.ToString()).ToArray();
    }

    // Members stored by versions without the registry are only discoverable by scanning for their member sets.
    // This runs once per cluster name; afterwards the registry is kept up to date by StoreActivation and RemoveMember.
    private async Task BackfillMemberRegistryAsync(IDatabase db, CancellationToken ct)
    {
        var prefix = _memberKey.ToString();
        var pattern = RedisPatterns.Escape(prefix) + "*";
        var memberIds = new HashSet<RedisValue>();
        var primaries = _connections.GetServers().Where(server => !server.IsReplica).ToArray();

        // Keys live on the primaries; SCAN is incremental, so this does not block the server like KEYS would
        foreach (var server in primaries.Where(server => server.IsConnected))
        {
            await foreach (var key in server.KeysAsync(db.Database, pattern, RemoveMemberBatchSize)
                               .WithCancellation(ct)
                               .ConfigureAwait(false))
            {
                memberIds.Add(key.ToString()[prefix.Length..]);
            }
        }

        if (memberIds.Count > 0)
        {
            await db.SetAddAsync(_membersKey, memberIds.ToArray()).ConfigureAwait(false);
        }

        // Only mark the backfill as done if every primary was scanned; otherwise the next sweep tries again
        if (primaries.All(server => server.IsConnected))
        {
            await db.StringSetAsync(_membersBackfilledKey, 1).ConfigureAwait(false);
        }
    }

    public async Task<StoredActivation?> TryGetExistingActivation(
        ClusterIdentity clusterIdentity,
        CancellationToken ct
    )
    {
        var activationStatus = await LookupKey(GetDb(), IdKey(clusterIdentity)).ConfigureAwait(false);

        return activationStatus?.Activation;
    }

    public void Dispose()
    {
    }

    public Task Init() => Task.CompletedTask;

    private IDatabase GetDb() => _connections.GetDatabase();

    private Task<bool> TryAcquireLockAsync(
        ClusterIdentity clusterIdentity,
        string requestId
    )
    {
        var key = IdKey(clusterIdentity);

        var db = GetDb();
        var transaction = db.CreateTransaction();

        transaction.AddCondition(Condition.KeyNotExists(key));
        transaction.HashSetAsync(key, LockId, requestId);
        transaction.KeyExpireAsync(key, _maxLockTime);

        return transaction.ExecuteAsync();
    }

    private async Task<ActivationStatus?> LookupKey(IDatabaseAsync db, RedisKey key)
    {
        var result = await _asyncSemaphore.WaitAsync(() => db.HashGetAllAsync(key)).ConfigureAwait(false);

        switch (result?.Length)
        {
            case 1:
                return new ActivationStatus(result.First(entry => entry.Name == LockId).Value);
            case 4:
                var values = result.ToDictionary();

                return new ActivationStatus
                (values[UniqueIdentity],
                    values[Address],
                    values[MemberId]
                );
            default:
                return null;
        }
    }

    private RedisKey IdKey(ClusterIdentity clusterIdentity) => _clusterIdentityKey.Append(clusterIdentity.ToString());

    private RedisKey MemberKey(string memberId) => _memberKey.Append(memberId);

    private class ActivationStatus
    {
        public ActivationStatus(string? uniqueIdentity, string? address, string? memberId)
        {
            if (uniqueIdentity == null || address == null || memberId == null)
            {
                throw new ArgumentException();
            }

            Activation = new StoredActivation(memberId, PID.FromAddress(address, uniqueIdentity));
        }

        public ActivationStatus(string? lockId)
        {
            ActiveLockId = lockId;
        }

        public StoredActivation? Activation { get; }

        public string? ActiveLockId { get; }
    }
}