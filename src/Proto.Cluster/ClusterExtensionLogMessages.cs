using System;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster;

internal static partial class ClusterExtensionLogMessages
{
    [LoggerMessage(0, LogLevel.Error, "Virtual actor {Pid} ({ClusterIdentity}) failed to start, deactivating it")]
    internal static partial void ActivationFailedToStart(this ILogger logger, Exception exception, PID pid,
        ClusterIdentity? clusterIdentity);
}

/// <summary>
///     Marks a virtual actor whose Started handler failed; it is being deactivated and rejects further messages.
/// </summary>
internal sealed class FailedToStart
{
    public static readonly FailedToStart Instance = new();

    private FailedToStart()
    {
    }
}
