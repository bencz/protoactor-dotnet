using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Proto.Cluster.Identity;

/// <summary>
///     Decides which members stored in an <see cref="IIdentityStorage" /> are leftovers that can be removed.
/// </summary>
internal static class StaleMemberSweep
{
    /// <summary>
    ///     Returns the stored member ids that are not active cluster members.
    /// </summary>
    /// <param name="storedMemberIds">Member ids that own activations in the storage</param>
    /// <param name="isActiveMember">Whether the member id is part of the current cluster topology</param>
    public static ImmutableHashSet<string> FindStaleMembers(
        IEnumerable<string> storedMemberIds,
        Func<string, bool> isActiveMember
    ) =>
        storedMemberIds
            .Where(memberId => !string.IsNullOrEmpty(memberId) && !isActiveMember(memberId))
            .ToImmutableHashSet();

    /// <summary>
    ///     Whether this member is the one that cleans up after members that are gone: the active member with the lowest
    ///     id. Every member applies the same rule to the same topology, so normally only one member does the cleanup
    ///     instead of all of them repeating the same work.
    /// </summary>
    /// <param name="selfId">Id of this member</param>
    /// <param name="activeMemberIds">Ids of the active cluster members</param>
    public static bool IsResponsibleForCleanup(string selfId, IEnumerable<string> activeMemberIds) =>
        activeMemberIds.Min(StringComparer.Ordinal) == selfId;
}
