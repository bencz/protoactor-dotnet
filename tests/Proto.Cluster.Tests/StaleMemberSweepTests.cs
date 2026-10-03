#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Proto.Cluster.Identity;
using Xunit;

namespace Proto.Cluster.Tests;

public class StaleMemberSweepTests
{
    [Fact]
    public void FindsOnlyMembersThatAreNotActive()
    {
        var active = new HashSet<string> { "member-1", "member-2" };

        var stale = StaleMemberSweep.FindStaleMembers(
            new[] { "member-1", "old-member", "member-2", "", "older-member" },
            active.Contains
        );

        stale.Should().BeEquivalentTo("old-member", "older-member");
    }

    [Fact]
    public void FindsNothingWhenAllMembersAreActive() =>
        StaleMemberSweep.FindStaleMembers(new[] { "member-1" }, _ => true).Should().BeEmpty();
}

public class StaleMemberSweepClusterFixture : BaseInMemoryClusterFixture
{
    public const string GhostMemberId = "ghost-member";

    public StaleMemberSweepClusterFixture() : base(1)
    {
        Storage = new RecordingIdentityStorage(() => Members.Select(member => member.System.Id).Append(GhostMemberId));
    }

    public RecordingIdentityStorage Storage { get; }

    protected override IIdentityLookup GetIdentityLookup(string clusterName) =>
        new IdentityStorageLookup(Storage, TimeSpan.FromMilliseconds(100));
}

public class IdentityStorageStaleMemberSweepTests : IClassFixture<StaleMemberSweepClusterFixture>
{
    private readonly StaleMemberSweepClusterFixture _fixture;

    public IdentityStorageStaleMemberSweepTests(StaleMemberSweepClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task RemovesActivationsOfMembersThatAreNotPartOfTheCluster()
    {
        await _fixture.Storage.GhostRemoved.WaitAsync(TimeSpan.FromSeconds(10));

        _fixture.Storage.RemovedMembers.Should().Equal(StaleMemberSweepClusterFixture.GhostMemberId);
    }
}

/// <summary>
///     Identity storage that only reports stored members and records which members were removed.
/// </summary>
public sealed class RecordingIdentityStorage : IIdentityStorage
{
    private readonly TaskCompletionSource _ghostRemoved = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ConcurrentQueue<string> _removedMembers = new();
    private readonly Func<IEnumerable<string>> _storedMemberIds;

    public RecordingIdentityStorage(Func<IEnumerable<string>> storedMemberIds)
    {
        _storedMemberIds = storedMemberIds;
    }

    public Task GhostRemoved => _ghostRemoved.Task;

    public IReadOnlyCollection<string> RemovedMembers => _removedMembers.ToArray();

    public Task<IReadOnlyCollection<string>> GetMemberIds(CancellationToken ct) =>
        Task.FromResult<IReadOnlyCollection<string>>(_storedMemberIds().ToArray());

    public Task RemoveMember(string memberId, CancellationToken ct)
    {
        _removedMembers.Enqueue(memberId);

        if (memberId == StaleMemberSweepClusterFixture.GhostMemberId)
        {
            _ghostRemoved.TrySetResult();
        }

        return Task.CompletedTask;
    }

    public Task Init() => Task.CompletedTask;

    public void Dispose()
    {
    }

    public Task<StoredActivation?> TryGetExistingActivation(ClusterIdentity clusterIdentity, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<SpawnLock?> TryAcquireLock(ClusterIdentity clusterIdentity, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<StoredActivation?> WaitForActivation(ClusterIdentity clusterIdentity, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task RemoveLock(SpawnLock spawnLock, CancellationToken ct) => throw new NotSupportedException();

    public Task StoreActivation(string memberId, SpawnLock spawnLock, PID pid, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task RemoveActivation(ClusterIdentity clusterIdentity, PID pid, CancellationToken ct) =>
        throw new NotSupportedException();
}
