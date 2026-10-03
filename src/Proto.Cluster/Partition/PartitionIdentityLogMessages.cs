using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Partition;

internal static partial class PartitionIdentityLogMessages
{
    [LoggerMessage(0, LogLevel.Warning,
        "[PartitionIdentity] Activation request for {ClusterIdentity} to {ActivatorAddress} timed out (attempt {Attempt} of {MaxAttempts}), retrying the same activator")]
    internal static partial void ActivationRequestTimedOut(this ILogger logger, ClusterIdentity clusterIdentity,
        string activatorAddress, int attempt, int maxAttempts);

    [LoggerMessage(1, LogLevel.Warning,
        "[PartitionIdentity] Activator {ActivatorAddress} did not answer the activation request for {ClusterIdentity}, the next request will be sent to the same activator")]
    internal static partial void ActivatorUnresponsive(this ILogger logger, ClusterIdentity clusterIdentity,
        string activatorAddress);
}
