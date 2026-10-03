namespace Proto.Cluster.SeedNode.Redis;

/// <summary>
///     The <c>host:port</c> value stored for each seed node member.
/// </summary>
internal static class SeedNodeAddress
{
    public static string Format(string host, int port) => $"{host}:{port}";

    /// <summary>
    ///     Parses a stored address. The port is taken after the last colon, so IPv6 hosts are supported.
    /// </summary>
    public static bool TryParse(string? value, out string host, out int port)
    {
        host = "";
        port = 0;

        var separator = value?.LastIndexOf(':') ?? -1;

        if (separator <= 0 || !int.TryParse(value![(separator + 1)..], out port))
        {
            return false;
        }

        host = value[..separator];

        return true;
    }
}
