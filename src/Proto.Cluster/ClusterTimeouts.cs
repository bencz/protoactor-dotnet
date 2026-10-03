using System;

namespace Proto.Cluster;

internal static class ClusterTimeouts
{
    /// <summary>
    ///     Cluster requests use cached cancellation tokens with whole second granularity, so timeouts are rounded up to
    ///     whole seconds, with a minimum of one second. Truncating instead would turn a sub-second timeout into zero, which
    ///     cannot create a token and fails every request that uses it.
    /// </summary>
    public static int ToWholeSeconds(TimeSpan timeout) => Math.Max(1, (int)Math.Ceiling(timeout.TotalSeconds));
}
