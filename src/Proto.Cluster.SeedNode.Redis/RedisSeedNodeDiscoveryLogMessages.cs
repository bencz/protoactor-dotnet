using System;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.SeedNode.Redis;

internal static partial class RedisSeedNodeDiscoveryLogMessages
{
    [LoggerMessage(0, LogLevel.Warning, "Failed to refresh seed node registration of member {MemberId}")]
    internal static partial void HeartbeatFailed(this ILogger logger, Exception exception, string memberId);

    [LoggerMessage(1, LogLevel.Warning, "Ignoring seed node entry of member {MemberId} with invalid address {Address}")]
    internal static partial void InvalidAddress(this ILogger logger, string memberId, string? address);
}
