using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Proto.Cluster.SeedNode.PostgreSql;
using Xunit;
using static Proto.TestKit.TestKit;

namespace Proto.Cluster.PostgreSqlIdentity.Tests;

/// <summary>
///     Integration tests against the PostgreSQL instance configured in appsettings.json.
/// </summary>
public sealed class PostgreSqlSeedNodeDiscoveryTests : IDisposable
{
    private static readonly PostgreSqlSeedNodeDiscoveryOptions ShortTtl = new() { MemberTtl = TimeSpan.FromSeconds(1) };
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    private readonly string _clusterName = $"seed-{Guid.NewGuid():N}";
    private readonly PostgreSqlSeedNodeDiscovery _discovery;

    public PostgreSqlSeedNodeDiscoveryTests()
    {
        _discovery = new PostgreSqlSeedNodeDiscovery(PostgreSqlFixture.DataSource, _clusterName, ShortTtl);
    }

    public void Dispose() => _discovery.Dispose();

    [Fact]
    public async Task MultipleMembersCanRegister()
    {
        await _discovery.Register("member-1", "10.0.0.1", 4020);
        await _discovery.Register("member-2", "10.0.0.2", 4020);

        var members = await _discovery.GetAll();

        members.Should().BeEquivalentTo(new[]
        {
            ("member-1", "10.0.0.1", 4020),
            ("member-2", "10.0.0.2", 4020)
        });
    }

    [Fact]
    public async Task RegisteringTheSameMemberAgainReplacesItsAddress()
    {
        await _discovery.Register("member-1", "10.0.0.1", 4020);
        await _discovery.Register("member-1", "10.0.0.9", 4021);

        var members = await _discovery.GetAll();

        members.Should().BeEquivalentTo(new[] { ("member-1", "10.0.0.9", 4021) });
    }

    [Fact]
    public async Task RemoveDeletesTheEntry()
    {
        await _discovery.Register("member-1", "10.0.0.1", 4020);
        await _discovery.Register("member-2", "10.0.0.2", 4020);

        await _discovery.Remove("member-1");

        var members = await _discovery.GetAll();

        members.Should().BeEquivalentTo(new[] { ("member-2", "10.0.0.2", 4020) });
    }

    [Fact]
    public async Task RegisteredMembersStayWhileTheyAreRefreshed()
    {
        await _discovery.Register("member-1", "10.0.0.1", 4020);

        // Wait for several TTL periods; only the heartbeat can keep the entry alive this long
        await Task.Delay(ShortTtl.MemberTtl * 3);

        var members = await _discovery.GetAll();

        members.Should().BeEquivalentTo(new[] { ("member-1", "10.0.0.1", 4020) });
    }

    [Fact]
    public async Task EntriesExpireWhenTheMemberStopsRefreshing()
    {
        var crashingMember = new PostgreSqlSeedNodeDiscovery(PostgreSqlFixture.DataSource, _clusterName, ShortTtl);
        await crashingMember.Register("crashed-member", "10.0.0.1", 4020);
        await _discovery.Register("member-2", "10.0.0.2", 4020);

        // Simulates a member that dies without deregistering: the heartbeat stops but the entry is not removed
        crashingMember.Dispose();

        await AwaitConditionAsync(
            async () => (await _discovery.GetAll()).All(member => member.memberId != "crashed-member"),
            WaitTimeout
        );

        var members = await _discovery.GetAll();
        members.Should().BeEquivalentTo(new[] { ("member-2", "10.0.0.2", 4020) });
    }

    [Fact]
    public async Task ClustersSharingTheTableDoNotSeeEachOther()
    {
        using var otherCluster =
            new PostgreSqlSeedNodeDiscovery(PostgreSqlFixture.DataSource, $"other-{Guid.NewGuid():N}", ShortTtl);

        await _discovery.Register("member-1", "10.0.0.1", 4020);
        await otherCluster.Register("member-2", "10.0.0.2", 4020);

        (await _discovery.GetAll()).Should().BeEquivalentTo(new[] { ("member-1", "10.0.0.1", 4020) });
        (await otherCluster.GetAll()).Should().BeEquivalentTo(new[] { ("member-2", "10.0.0.2", 4020) });
    }
}
