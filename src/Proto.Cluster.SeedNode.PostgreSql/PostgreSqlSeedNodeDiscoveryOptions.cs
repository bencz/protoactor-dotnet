using System;

namespace Proto.Cluster.SeedNode.PostgreSql;

public sealed record PostgreSqlSeedNodeDiscoveryOptions
{
    /// <summary>
    ///     Schema of the seed member table. Default is <c>public</c>.
    /// </summary>
    public string Schema { get; init; } = "public";

    /// <summary>
    ///     Name of the seed member table. Default is <c>proto_cluster_seed_members</c>.
    /// </summary>
    public string TableName { get; init; } = "proto_cluster_seed_members";

    /// <summary>
    ///     Creates the table on first use if it does not exist. Disable it when the schema is managed by migrations;
    ///     <see cref="PostgreSqlSeedNodeDiscovery.CreateSchemaSql" /> returns the script to apply.
    /// </summary>
    public bool CreateSchema { get; init; } = true;

    /// <summary>
    ///     How long an entry stays valid without being refreshed. Registered members refresh their entry every third of
    ///     this period, so members that die without deregistering disappear on their own. Default is 30 seconds.
    /// </summary>
    public TimeSpan MemberTtl { get; init; } = TimeSpan.FromSeconds(30);
}
