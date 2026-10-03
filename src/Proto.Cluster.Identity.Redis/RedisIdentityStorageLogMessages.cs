using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Identity.Redis;

internal static partial class RedisIdentityStorageLogMessages
{
    [LoggerMessage(0, LogLevel.Debug, "Removing activation: {ClusterIdentity} {Pid}")]
    internal static partial void RemovingActivation(this ILogger logger, ClusterIdentity clusterIdentity, PID pid);
}
