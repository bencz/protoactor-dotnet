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
using static Proto.TestKit.TestKit;

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

    [Fact]
    public void OnlyTheMemberWithTheLowestIdIsResponsibleForCleanup()
    {
        var members = new[] { "c-member", "a-member", "b-member" };

        members.Where(member => StaleMemberSweep.IsResponsibleForCleanup(member, members))
            .Should().Equal("a-member");
    }

    [Fact]
    public void ResponsibilityUsesOrdinalOrdering() =>
        // Ordinal ordering puts upper case before lower case; a culture-aware comparison would not
        StaleMemberSweep.IsResponsibleForCleanup("B-member", new[] { "a-member", "B-member" }).Should().BeTrue();

    [Fact]
    public void NonMembersAreNeverResponsibleForCleanup() =>
        StaleMemberSweep.IsResponsibleForCleanup("client", new[] { "a-member", "b-member" }).Should().BeFalse();
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
        await AwaitConditionAsync(
            () => _fixture.Storage.RemovedMembers.Contains(StaleMemberSweepClusterFixture.GhostMemberId),
            TimeSpan.FromSeconds(10)
        );

        _fixture.Storage.RemovedMembers.Should().Equal(StaleMemberSweepClusterFixture.GhostMemberId);
    }
}

public class LeftMemberCleanupClusterFixture : BaseInMemoryClusterFixture
{
    public LeftMemberCleanupClusterFixture() : base(3)
    {
        Storage = new RecordingIdentityStorage(() => Members.Select(member => member.System.Id));
    }

    public RecordingIdentityStorage Storage { get; }

    protected override IIdentityLookup GetIdentityLookup(string clusterName) => new IdentityStorageLookup(Storage);
}

public class LeftMemberCleanupTests : IClassFixture<LeftMemberCleanupClusterFixture>
{
    private readonly LeftMemberCleanupClusterFixture _fixture;

    public LeftMemberCleanupTests(LeftMemberCleanupClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task OnlyOneMemberRemovesTheActivationsOfAMemberThatLeft()
    {
        var leaving = _fixture.Members[^1];
        var leavingId = leaving.System.Id;

        // Not graceful: the member does not clean up after itself, so the remaining members have to
        await _fixture.RemoveNode(leaving, false);
        await _fixture.WaitForMemberAsync(leavingId, false);

        await AwaitConditionAsync(() => _fixture.Storage.RemovedMembers.Contains(leavingId), TimeSpan.FromSeconds(10));

        // Give the other remaining member time to handle the same topology change before counting the removals
        await Task.Delay(500);

        _fixture.Storage.RemovedMembers.Count(memberId => memberId == leavingId).Should().Be(1);
    }
}

/// <summary>
///     Identity storage that only reports stored members and records which members were removed.
/// </summary>
public sealed class RecordingIdentityStorage : IIdentityStorage
{
    private readonly ConcurrentQueue<string> _removedMembers = new();
    private readonly Func<IEnumerable<string>> _storedMemberIds;

    public RecordingIdentityStorage(Func<IEnumerable<string>> storedMemberIds)
    {
        _storedMemberIds = storedMemberIds;
    }

    public IReadOnlyCollection<string> RemovedMembers => _removedMembers.ToArray();

    public Task<IReadOnlyCollection<string>> GetMemberIds(CancellationToken ct) =>
        Task.FromResult<IReadOnlyCollection<string>>(_storedMemberIds().ToArray());

    public Task RemoveMember(string memberId, CancellationToken ct)
    {
        _removedMembers.Enqueue(memberId);

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
