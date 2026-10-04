using System;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Testing;

internal static partial class TestProviderLogMessages
{
    [LoggerMessage(0, LogLevel.Debug, "Unregistering service {Service}")]
    internal static partial void UnregisteringService(this ILogger logger, string service);

    [LoggerMessage(1, LogLevel.Debug, "TestAgent response: {Count} members {MemberIds}")]
    internal static partial void TestAgentResponse(this ILogger logger, int count, string memberIds);

    [LoggerMessage(2, LogLevel.Warning, "A status update handler failed, delivering the update to the remaining handlers")]
    internal static partial void StatusUpdateHandlerFailed(this ILogger logger, Exception exception);
}
