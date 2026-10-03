using System;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.SeedNode.PostgreSql;

internal static partial class PostgreSqlSeedNodeDiscoveryLogMessages
{
    [LoggerMessage(0, LogLevel.Warning, "Failed to refresh seed node registration of member {MemberId}")]
    internal static partial void HeartbeatFailed(this ILogger logger, Exception exception, string memberId);
}
