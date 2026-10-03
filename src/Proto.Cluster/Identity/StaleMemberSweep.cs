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
}
