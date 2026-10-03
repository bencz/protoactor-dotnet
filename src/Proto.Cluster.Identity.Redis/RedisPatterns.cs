using System.Text;

namespace Proto.Cluster.Identity.Redis;

internal static class RedisPatterns
{
    /// <summary>
    ///     Escapes the glob characters of a literal so it can be used as a prefix in a SCAN MATCH pattern.
    /// </summary>
    public static string Escape(string literal)
    {
        var builder = new StringBuilder(literal.Length);

        foreach (var c in literal)
        {
            if (c is '\\' or '*' or '?' or '[' or ']')
            {
                builder.Append('\\');
            }

            builder.Append(c);
        }

        return builder.ToString();
    }
}
