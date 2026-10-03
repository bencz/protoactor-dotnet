using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Identity.PostgreSql;

internal static partial class PostgreSqlIdentityStorageLogMessages
{
    [LoggerMessage(0, LogLevel.Debug, "Storing activation: {ActivatorId}, {ClusterIdentity}, {Pid}")]
    internal static partial void StoringActivation(this ILogger logger, string activatorId,
        ClusterIdentity clusterIdentity, PID pid);

    [LoggerMessage(1, LogLevel.Debug, "Removing activation: {ClusterIdentity} {Pid}")]
    internal static partial void RemovingActivation(this ILogger logger, ClusterIdentity clusterIdentity, PID pid);

    [LoggerMessage(2, LogLevel.Debug, "Removing stale lock {LockId} for {ClusterIdentity}")]
    internal static partial void RemovingStaleLock(this ILogger logger, string lockId, ClusterIdentity clusterIdentity);
}
