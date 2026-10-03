namespace Proto.Cluster.SeedNode.PostgreSql;

/// <summary>
///     The statements used by <see cref="PostgreSqlSeedNodeDiscovery" />. Expiration is evaluated with the database
///     clock (<c>now()</c>), so clock differences between members do not matter.
/// </summary>
internal sealed class PostgreSqlSeedNodeSql
{
    public PostgreSqlSeedNodeSql(PostgreSqlSeedNodeDiscoveryOptions options)
    {
        var schema = PostgreSqlIdentifier.Quote(options.Schema);
        var table = $"{schema}.{PostgreSqlIdentifier.Quote(options.TableName)}";
        var expiresIndex = PostgreSqlIdentifier.Quote(options.TableName + "_expires_at");

        CreateSchema = $"""
            CREATE SCHEMA IF NOT EXISTS {schema};
            CREATE TABLE IF NOT EXISTS {table} (
                cluster_name text        NOT NULL,
                member_id    text        NOT NULL,
                host         text        NOT NULL,
                port         integer     NOT NULL,
                expires_at   timestamptz NOT NULL,
                PRIMARY KEY (cluster_name, member_id)
            );
            CREATE INDEX IF NOT EXISTS {expiresIndex} ON {table} (cluster_name, expires_at);
            """;

        Register = $"""
            INSERT INTO {table} (cluster_name, member_id, host, port, expires_at)
            VALUES ($1, $2, $3, $4, now() + $5)
            ON CONFLICT (cluster_name, member_id)
            DO UPDATE SET host = EXCLUDED.host, port = EXCLUDED.port, expires_at = EXCLUDED.expires_at
            """;

        Remove = $"""
            DELETE FROM {table}
            WHERE cluster_name = $1 AND member_id = $2
            """;

        RemoveExpired = $"""
            DELETE FROM {table}
            WHERE cluster_name = $1 AND expires_at <= now()
            """;

        GetAll = $"""
            SELECT member_id, host, port
            FROM {table}
            WHERE cluster_name = $1 AND expires_at > now()
            """;
    }

    public string CreateSchema { get; }
    public string Register { get; }
    public string Remove { get; }
    public string RemoveExpired { get; }
    public string GetAll { get; }
}
