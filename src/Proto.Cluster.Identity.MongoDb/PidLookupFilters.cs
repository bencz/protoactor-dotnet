using System.Text.RegularExpressions;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Proto.Cluster.Identity.MongoDb;

/// <summary>
///     Filters and error classification used by <see cref="MongoIdentityStorage" />.
///     Every filter that targets a single lookup entry includes the <see cref="PidLookupEntity.Key" />,
///     which is mapped to <c>_id</c>, so the queries are always served by the primary key index.
/// </summary>
internal static class PidLookupFilters
{
    private static FilterDefinitionBuilder<PidLookupEntity> Filter => Builders<PidLookupEntity>.Filter;

    public static FilterDefinition<PidLookupEntity> LockedEntry(string key, string lockId) =>
        Filter.Eq(e => e.Key, key) & Filter.Eq(e => e.LockedBy, lockId);

    /// <summary>
    ///     Activations of the given cluster that belong to a member. Keys are <c>{clusterName}/{identity}</c>, so an anchored
    ///     prefix on the key keeps other clusters sharing the collection out and is served by the primary key index.
    /// </summary>
    public static FilterDefinition<PidLookupEntity> ClusterMembers(string clusterName) =>
        Filter.Regex(e => e.Key, new BsonRegularExpression("^" + Regex.Escape(clusterName + "/"))) &
        Filter.Ne(e => e.MemberId, null);

    public static bool IsDuplicateKey(MongoWriteException exception) =>
        exception.WriteError?.Category == ServerErrorCategory.DuplicateKey;
}
