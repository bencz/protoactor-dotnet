using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using Proto.Cluster.Identity.MongoDb;
using Xunit;

namespace Proto.Cluster.MongoIdentity.Tests;

/// <summary>
///     Integration tests against the MongoDB instance configured in appsettings.json.
/// </summary>
public class MongoLegacyLockTests
{
    [Fact]
    public async Task LockWrittenByAnOlderVersionBecomesStale()
    {
        var clusterName = $"legacy-{Guid.NewGuid():N}";
        var collection = MongoFixture.Database.GetCollection<PidLookupEntity>("pids");
        var storage = new MongoIdentityStorage(clusterName, collection,
            maxWaitBeforeStaleLock: TimeSpan.FromMilliseconds(1500));
        var identity = new ClusterIdentity { Kind = "thing", Identity = "legacy-lock" };

        // A lock as written by versions without a lock timestamp, whose holder crashed
        await MongoFixture.Database.GetCollection<BsonDocument>("pids").InsertOneAsync(new BsonDocument
        {
            { "_id", $"{clusterName}/{identity}" },
            { "Identity", identity.Identity },
            { "UniqueIdentity", BsonNull.Value },
            { "Kind", identity.Kind },
            { "Address", BsonNull.Value },
            { "MemberId", BsonNull.Value },
            { "LockedBy", "crashed-holder" },
            { "Revision", 1 }
        });

        // Waiters give up after 1 s, before the 1.5 s stale lock wait, like workers with the default timeouts
        var firstWaiter = () => storage.WaitForActivation(identity, new CancellationTokenSource(1000).Token);
        await firstWaiter.Should().ThrowAsync<OperationCanceledException>();

        await Task.Delay(1000);

        var activation = await storage.WaitForActivation(identity, new CancellationTokenSource(1000).Token);
        activation.Should().BeNull();

        var spawnLock = await storage.TryAcquireLock(identity, CancellationToken.None);
        spawnLock.Should().NotBeNull("a lock written by an older version must become stale as well");
    }
}
