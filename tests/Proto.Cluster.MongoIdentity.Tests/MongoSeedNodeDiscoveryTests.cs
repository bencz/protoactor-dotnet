using System;
using System.Threading.Tasks;
using FluentAssertions;
using Proto.Cluster.SeedNode.MongoDb;
using Xunit;

namespace Proto.Cluster.MongoIdentity.Tests;

/// <summary>
///     Integration tests against the MongoDB instance configured in appsettings.json.
/// </summary>
public class MongoSeedNodeDiscoveryTests
{
    private readonly MongoDbSeedNodeDiscovery _discovery =
        new(MongoFixture.Database, $"seed-members-{Guid.NewGuid():N}");

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
    public async Task RemoveOnlyRemovesTheGivenMember()
    {
        await _discovery.Register("member-1", "10.0.0.1", 4020);
        await _discovery.Register("member-2", "10.0.0.2", 4020);

        await _discovery.Remove("member-1");

        var members = await _discovery.GetAll();

        members.Should().BeEquivalentTo(new[] { ("member-2", "10.0.0.2", 4020) });
    }
}
