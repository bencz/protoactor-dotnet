using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Proto.Cluster.Identity.MongoDb;
using Proto.Cluster.SeedNode.MongoDb;
using Xunit;

namespace Proto.Cluster.MongoIdentity.Tests;

/// <summary>
///     Verifies the BSON mapping and the rendered filters without a running MongoDB instance.
/// </summary>
public class MongoMappingTests
{
    [Fact]
    public void SeedMemberIsStoredWithMemberIdAsPrimaryKey()
    {
        var document = new ProtoActorMember { MemberId = "member-1", Host = "10.0.0.1", Port = 4020 }.ToBsonDocument();

        document.Should().Equal(new BsonDocument
        {
            { "_id", "member-1" },
            { "Host", "10.0.0.1" },
            { "Port", 4020 }
        });
    }

    [Fact]
    public void LegacySeedMemberDocumentCanStillBeRead()
    {
        // Older versions stored the member id as a regular field and wrote a null _id
        var legacy = new BsonDocument
        {
            { "_id", BsonNull.Value },
            { "MemberId", "member-1" },
            { "Host", "10.0.0.1" },
            { "Port", 4020 }
        };

        var member = BsonSerializer.Deserialize<ProtoActorMember>(legacy);

        member.MemberId.Should().BeNull();
        member.Host.Should().Be("10.0.0.1");
        member.Port.Should().Be(4020);
    }

    [Fact]
    public void SeedMemberLookupUsesPrimaryKey() =>
        Render(SeedMemberFilters.ByMemberId("member-1"))
            .Should().Equal(new BsonDocument("_id", "member-1"));

    [Fact]
    public void RegisteredSeedMembersExcludeLegacyEntries() =>
        Render(SeedMemberFilters.Registered)
            .Should().Equal(new BsonDocument("_id", new BsonDocument("$ne", BsonNull.Value)));

    [Fact]
    public void LockRemovalUsesPrimaryKey() =>
        Render(PidLookupFilters.LockedEntry("cluster/identity", "lock-1"))
            .Should().Equal(new BsonDocument
            {
                { "_id", "cluster/identity" },
                { "LockedBy", "lock-1" }
            });

    private static BsonDocument Render<T>(FilterDefinition<T> filter) =>
        filter.Render(new RenderArgs<T>(BsonSerializer.LookupSerializer<T>(), BsonSerializer.SerializerRegistry));
}
