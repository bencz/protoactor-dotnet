using System;
using System.Text.RegularExpressions;

namespace Proto.Cluster.SeedNode.PostgreSql;

internal static class PostgreSqlIdentifier
{
    private const int MaxLength = 63;
    private static readonly Regex ValidIdentifier = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    /// <summary>
    ///     Quotes a schema, table or index name that comes from configuration. Names cannot be passed as query
    ///     parameters, so only plain identifiers are accepted to rule out SQL injection.
    /// </summary>
    public static string Quote(string name)
    {
        if (string.IsNullOrEmpty(name) || name.Length > MaxLength || !ValidIdentifier.IsMatch(name))
        {
            throw new ArgumentException(
                $"'{name}' is not a valid PostgreSQL identifier: use up to {MaxLength} letters, digits or underscores, not starting with a digit",
                nameof(name));
        }

        return $"\"{name}\"";
    }
}
