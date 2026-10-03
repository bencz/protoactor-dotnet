using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Proto.Utils;

namespace Proto.Cluster.Identity.MongoDb;

public sealed class MongoIdentityStorage : IIdentityStorage
{
    private static readonly ILogger Logger = Log.CreateLogger<MongoIdentityStorage>();
    private readonly AsyncSemaphore _asyncSemaphore;

    private readonly string _clusterName;
    private readonly IMongoCollection<PidLookupEntity> _pids;

    public MongoIdentityStorage(string clusterName, IMongoCollection<PidLookupEntity> pids, int maxConcurrency = 50)
    {
        _asyncSemaphore = new AsyncSemaphore(maxConcurrency);
        _clusterName = clusterName;
        _pids = pids;
    }

    public async Task<SpawnLock?> TryAcquireLock(
        ClusterIdentity clusterIdentity,
        CancellationToken ct
    )
    {
        var requestId = Guid.NewGuid().ToString();
        var hasLock = await TryAcquireLockAsync(clusterIdentity, requestId, ct).ConfigureAwait(false);

        return hasLock ? new SpawnLock(requestId, clusterIdentity) : null;
    }

    public async Task<StoredActivation?> WaitForActivation(
        ClusterIdentity clusterIdentity,
        CancellationToken ct
    )
    {
        var key = GetKey(clusterIdentity);
        var pidLookupEntity = await LookupKey(key, ct).ConfigureAwait(false);
        var lockId = pidLookupEntity?.LockedBy;

        if (lockId != null)
        {
            //There is an active lock on the pid, spin wait with incremental backoff
            var i = 0;

            do
            {
                await Task.Delay(20 * i, ct).ConfigureAwait(false);
            } while ((pidLookupEntity = await LookupKey(key, ct).ConfigureAwait(false))?.LockedBy == lockId && ++i < 10);
        }

        //the lookup entity was lost, stale lock maybe?
        if (pidLookupEntity == null)
        {
            return null;
        }

        //lookup was unlocked, return this pid
        if (pidLookupEntity.LockedBy == null)
        {
            return new StoredActivation(pidLookupEntity.MemberId!,
                PID.FromAddress(pidLookupEntity.Address!, pidLookupEntity.UniqueIdentity!)
            );
        }

        //Still locked but not by the same request that originally locked it, so not stale
        if (pidLookupEntity.LockedBy != lockId)
        {
            return null;
        }

        //Stale lock. just delete it and let cluster retry
        // _logger.LogDebug($"Stale lock: {pidLookupEntity.Key}");
        await RemoveLock(new SpawnLock(lockId, clusterIdentity), CancellationToken.None).ConfigureAwait(false);

        return null;
    }

    public Task RemoveLock(SpawnLock spawnLock, CancellationToken ct) =>
        _asyncSemaphore.WaitAsync(() => _pids.DeleteOneAsync(
            PidLookupFilters.LockedEntry(GetKey(spawnLock.ClusterIdentity), spawnLock.LockId), ct));

    public async Task StoreActivation(string memberId, SpawnLock spawnLock, PID pid, CancellationToken ct)
    {
        Logger.StoringActivation(memberId, spawnLock, pid);
        var key = GetKey(spawnLock.ClusterIdentity);

        var res = await _asyncSemaphore.WaitAsync(() => _pids.UpdateOneAsync(
                s => s.Key == key && s.LockedBy == spawnLock.LockId && s.Revision == 1,
                Builders<PidLookupEntity>.Update
                    .Set(l => l.Address, pid.Address)
                    .Set(l => l.MemberId, memberId)
                    .Set(l => l.UniqueIdentity, pid.Id)
                    .Set(l => l.Revision, 2)
                    .Unset(l => l.LockedBy)
                , new UpdateOptions(), ct
            )
        ).ConfigureAwait(false);

        if (res.MatchedCount != 1)
        {
            throw new LockNotFoundException($"Failed to store activation of {pid}");
        }
    }

    public async Task RemoveActivation(ClusterIdentity clusterIdentity, PID pid, CancellationToken ct)
    {
        Logger.RemovingActivation(clusterIdentity, pid);

        var key = GetKey(clusterIdentity);

        await _asyncSemaphore.WaitAsync(
            () => _pids.DeleteManyAsync(p => p.Key == key && p.UniqueIdentity == pid.Id, ct)).ConfigureAwait(false);
    }

    public Task RemoveMember(string memberId, CancellationToken ct) =>
        _asyncSemaphore.WaitAsync(() => _pids.DeleteManyAsync(p => p.MemberId == memberId, ct));

    public async Task<IReadOnlyCollection<string>> GetMemberIds(CancellationToken ct)
    {
        // Served by the MemberId index; locks without an activation have no member yet
        var memberIds = await _asyncSemaphore.WaitAsync(async () =>
            {
                var cursor = await _pids.DistinctAsync(e => e.MemberId, PidLookupFilters.HasMember, cancellationToken: ct)
                    .ConfigureAwait(false);

                return await cursor.ToListAsync(ct).ConfigureAwait(false);
            }
        ).ConfigureAwait(false);

        return memberIds.OfType<string>().ToList();
    }

    public async Task<StoredActivation?> TryGetExistingActivation(
        ClusterIdentity clusterIdentity,
        CancellationToken ct
    )
    {
        var pidLookup = await LookupKey(GetKey(clusterIdentity), ct).ConfigureAwait(false);

        return pidLookup?.Address == null || pidLookup?.UniqueIdentity == null
            ? null
            : new StoredActivation(pidLookup.MemberId!,
                PID.FromAddress(pidLookup.Address, pidLookup.UniqueIdentity)
            );
    }

    public void Dispose()
    {
    }

    // Every other query is served by the primary key (Key); RemoveMember needs this secondary index
    public Task Init() => _pids.Indexes.CreateOneAsync(
        new CreateIndexModel<PidLookupEntity>(Builders<PidLookupEntity>.IndexKeys.Ascending(e => e.MemberId)));

    private async Task<bool> TryAcquireLockAsync(
        ClusterIdentity clusterIdentity,
        string requestId,
        CancellationToken ct
    )
    {
        var key = GetKey(clusterIdentity);

        var lockEntity = new PidLookupEntity
        {
            Address = null,
            Identity = clusterIdentity.Identity,
            Key = key,
            Kind = clusterIdentity.Kind,
            LockedBy = requestId,
            Revision = 1,
            MemberId = null
        };

        try
        {
            // The key is the primary key, so the insert only succeeds if no lock or activation exists yet
            await _asyncSemaphore.WaitAsync(() => _pids.InsertOneAsync(lockEntity, new InsertOneOptions(), ct)).ConfigureAwait(false);
            Logger.GotLock(clusterIdentity);

            return true;
        }
        catch (MongoWriteException e) when (PidLookupFilters.IsDuplicateKey(e))
        {
            Logger.DidNotGetLock(clusterIdentity);

            return false;
        }
    }

    private async Task<PidLookupEntity?> LookupKey(string key, CancellationToken ct)
    {
        try
        {
            var res = await _asyncSemaphore.WaitAsync(() =>
                _pids.Find(x => x.Key == key).Limit(1).SingleOrDefaultAsync(ct)).ConfigureAwait(false);

            return res;
        }
        catch (MongoConnectionException x)
        {
            Logger.ConnectionFailureOnLookup(x, key);
        }

        throw new StorageFailureException($"Failed to connect to MongoDB while looking up key {key}");
    }

    private string GetKey(ClusterIdentity clusterIdentity) => $"{_clusterName}/{clusterIdentity}";
}