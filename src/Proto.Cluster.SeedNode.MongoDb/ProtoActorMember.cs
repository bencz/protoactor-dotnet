using MongoDB.Bson.Serialization.Attributes;

namespace Proto.Cluster.SeedNode.MongoDb;

// Extra elements are ignored so documents written by older versions (which stored MemberId as a regular field) can still be read
[BsonIgnoreExtraElements]
public class ProtoActorMember
{
    // The member id is the natural key: it makes registration idempotent and lookups use the primary key index
    [BsonId]
    public string MemberId { get; set; } = "";

    public string Host { get; set; } = "";
    public int Port { get; set; } = int.MinValue;
}
