using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Proto.Cluster.SeedNode.Redis;
using StackExchange.Redis;
using Xunit;
using static Proto.TestKit.TestKit;

namespace Proto.Cluster.RedisIdentity.Tests;

public class SeedNodeAddressTests
{
    [Theory]
    [InlineData("10.0.0.1:4020", "10.0.0.1", 4020)]
    [InlineData("my-pod.my-service:8090", "my-pod.my-service", 8090)]
    [InlineData("::1:4020", "::1", 4020)]
    public void ParsesStoredAddresses(string value, string expectedHost, int expectedPort)
    {
        SeedNodeAddress.TryParse(value, out var host, out var port).Should().BeTrue();

        host.Should().Be(expectedHost);
        port.Should().Be(expectedPort);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("10.0.0.1")]
    [InlineData(":4020")]
    [InlineData("10.0.0.1:port")]
    public void RejectsInvalidAddresses(string? value) =>
        SeedNodeAddress.TryParse(value, out _, out _).Should().BeFalse();

    [Fact]
    public void FormatAndParseRoundTrip()
    {
        SeedNodeAddress.TryParse(SeedNodeAddress.Format("10.0.0.1", 4020), out var host, out var port).Should().BeTrue();

        (host, port).Should().Be(("10.0.0.1", 4020));
    }
}

/// <summary>
///     Integration tests against the Redis instance configured in appsettings.json (requires Redis/Valkey 7.4+).
/// </summary>
public sealed class RedisSeedNodeDiscoveryTests : IDisposable
{
    private static readonly TimeSpan ShortTtl = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    private readonly string _storageKey = $"seed-members-{Guid.NewGuid():N}";
    private readonly RedisSeedNodeDiscovery _discovery;

    public RedisSeedNodeDiscoveryTests()
    {
        _discovery = new RedisSeedNodeDiscovery(RedisFixture.Multiplexer, _storageKey, ShortTtl);
    }

    public void Dispose()
    {
        _discovery.Dispose();
        RedisFixture.Multiplexer.GetDatabase().KeyDelete(_storageKey);
    }

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
        await Task.Delay(ShortTtl * 3);

        var members = await _discovery.GetAll();

        members.Should().BeEquivalentTo(new[] { ("member-1", "10.0.0.1", 4020) });
    }

    [Fact]
    public async Task EntriesExpireWhenTheMemberStopsRefreshing()
    {
        var crashingMember = new RedisSeedNodeDiscovery(RedisFixture.Multiplexer, _storageKey, ShortTtl);
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
    public async Task EntriesOfOtherWritersAreNotTouched()
    {
        // An entry without expiration, as written by another application or an older version sharing the hash
        await RedisFixture.Multiplexer.GetDatabase().HashSetAsync(_storageKey, "other-member", "10.0.0.1:4020");

        await _discovery.Register("member-2", "10.0.0.2", 4020);

        // Wait for several TTL periods of this discovery
        await Task.Delay(ShortTtl * 3);

        var members = await _discovery.GetAll();
        members.Should().BeEquivalentTo(new[]
        {
            ("other-member", "10.0.0.1", 4020),
            ("member-2", "10.0.0.2", 4020)
        });
    }

    [Fact]
    public async Task InvalidEntriesAreSkipped()
    {
        await RedisFixture.Multiplexer.GetDatabase().HashSetAsync(_storageKey, "broken-member", "not-an-address");
        await _discovery.Register("member-1", "10.0.0.1", 4020);

        var members = await _discovery.GetAll();

        members.Should().BeEquivalentTo(new[] { ("member-1", "10.0.0.1", 4020) });
    }
}
