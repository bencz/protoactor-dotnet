using System;
using Microsoft.Extensions.Logging;

namespace Proto.Cluster.Identity;

internal static partial class IdentityStorageLookupLogMessages
{
    [LoggerMessage(0, LogLevel.Information,
        "Found {Count} members in the identity storage that are not part of the cluster, removing them in {Delay}")]
    internal static partial void FoundStaleMembers(this ILogger logger, int count, TimeSpan delay);

    [LoggerMessage(1, LogLevel.Information, "Removing activations of stale member {MemberId} from the identity storage")]
    internal static partial void RemovingStaleMember(this ILogger logger, string memberId);

    [LoggerMessage(2, LogLevel.Warning, "Failed to remove stale members from the identity storage")]
    internal static partial void StaleMemberSweepFailed(this ILogger logger, Exception exception);

    [LoggerMessage(3, LogLevel.Warning, "Failed to remove the activations of member {MemberId}, which left the cluster")]
    internal static partial void LeftMemberCleanupFailed(this ILogger logger, Exception exception, string memberId);
}
