using System;

namespace Proto.Cluster.Identity.PostgreSql;

public sealed record PostgreSqlIdentityStorageOptions
{
    /// <summary>
    ///     Schema of the activation table. Default is <c>public</c>.
    /// </summary>
    public string Schema { get; init; } = "public";

    /// <summary>
    ///     Name of the activation table. Default is <c>proto_cluster_activations</c>.
    /// </summary>
    public string TableName { get; init; } = "proto_cluster_activations";

    /// <summary>
    ///     Creates the table and its index on startup if they do not exist. Disable it when the schema is managed by
    ///     migrations; <see cref="PostgreSqlIdentityStorage.CreateSchemaSql" /> returns the script to apply.
    /// </summary>
    public bool CreateSchema { get; init; } = true;

    /// <summary>
    ///     How long a member waits for another member's spawn lock before treating it as abandoned and removing it.
    ///     The lock is held while the activation is spawned and stored, so this must comfortably exceed
    ///     <see cref="ClusterConfig.ActorActivationTimeout" /> plus the database latency under load; a too short value
    ///     removes locks that are still in use and causes a second activation. Default is 10 seconds.
    /// </summary>
    public TimeSpan MaxWaitBeforeStaleLock { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    ///     Maximum number of concurrent database operations per member. Default is 50.
    /// </summary>
    public int MaxConcurrency { get; init; } = 50;
}
