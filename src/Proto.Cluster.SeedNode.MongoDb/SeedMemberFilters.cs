using MongoDB.Driver;

namespace Proto.Cluster.SeedNode.MongoDb;

/// <summary>
///     Filters used by <see cref="MongoDbSeedNodeDiscovery" />. <see cref="ProtoActorMember.MemberId" /> is mapped to
///     <c>_id</c>, so all of them are served by the primary key index.
/// </summary>
internal static class SeedMemberFilters
{
    private static FilterDefinitionBuilder<ProtoActorMember> Filter => Builders<ProtoActorMember>.Filter;

    public static FilterDefinition<ProtoActorMember> ByMemberId(string memberId) => Filter.Eq(m => m.MemberId, memberId);

    // Older versions stored every member with a null _id; such entries cannot be addressed and are skipped
    public static FilterDefinition<ProtoActorMember> Registered { get; } = Filter.Ne(m => m.MemberId, null);
}
