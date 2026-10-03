using Microsoft.Extensions.Logging;

namespace Proto.Cluster.PubSub;

internal static partial class TopicActorLogMessages
{
    [LoggerMessage(0, LogLevel.Warning,
        "Topic {Topic}: {Count} subscribers could not be resolved to an activation and were skipped: {Subscribers}")]
    internal static partial void UnresolvedSubscribers(this ILogger logger, string topic, int count, string subscribers);
}
