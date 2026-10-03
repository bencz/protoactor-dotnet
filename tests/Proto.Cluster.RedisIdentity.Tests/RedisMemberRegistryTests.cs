using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Proto.Cluster.Identity.Redis;
using Xunit;

namespace Proto.Cluster.RedisIdentity.Tests;

/// <summary>
///     Integration tests against the Redis instance configured in appsettings.json.
/// </summary>
public class RedisMemberRegistryTests
{
    [Fact]
    public async Task MembersStoredBeforeTheRegistryExistedAreDiscovered()
    {
        var clusterName = $"registry-{Guid.NewGuid():N}";
        var db = RedisFixture.Multiplexer.GetDatabase();

        // Written the way versions without the member registry stored a member's activations
        await db.SetAddAsync($"{clusterName}:mb:legacy-member", $"{clusterName}:ci:some-identity");

        var storage = new RedisIdentityStorage(clusterName, RedisFixture.Multiplexer);

        var memberIds = await storage.GetMemberIds(CancellationToken.None);

        memberIds.Should().Equal("legacy-member");

        await storage.RemoveMember("legacy-member", CancellationToken.None);

        var remaining = await storage.GetMemberIds(CancellationToken.None);

        remaining.Should().BeEmpty();
        (await db.KeyExistsAsync($"{clusterName}:mb:legacy-member")).Should().BeFalse();
    }
}
