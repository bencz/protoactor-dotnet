using MongoDB.Driver;
using Proto.Cluster.Seed;

namespace Proto.Cluster.SeedNode.MongoDb;

public class MongoDbSeedNodeDiscovery : ISeedNodeDiscovery
{
    private readonly IMongoCollection<ProtoActorMember> _collection;

    public MongoDbSeedNodeDiscovery(IMongoDatabase mongoDatabase, string storageKey = "ProtoActorMembers")
    {
        _collection = mongoDatabase.GetCollection<ProtoActorMember>(storageKey);
    }

    public Task Register(string memberId, string host, int port) =>
        _collection.ReplaceOneAsync(
            SeedMemberFilters.ByMemberId(memberId),
            new ProtoActorMember
            {
                MemberId = memberId,
                Host = host,
                Port = port
            },
            new ReplaceOptions { IsUpsert = true }
        );

    public Task Remove(string memberId) => _collection.DeleteOneAsync(SeedMemberFilters.ByMemberId(memberId));

    public async Task<(string memberId, string host, int port)[]> GetAll()
    {
        var mongoResult = await _collection
            .Find(SeedMemberFilters.Registered)
            .ToListAsync()
            .ConfigureAwait(false);

        return mongoResult
            .Select(x => (x.MemberId, x.Host, x.Port))
            .ToArray();
    }
}
