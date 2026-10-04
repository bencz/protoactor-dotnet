namespace Proto.Cluster.Identity.PostgreSql;

/// <summary>
///     The statements used by <see cref="PostgreSqlIdentityStorage" />. Activations are keyed by
///     (cluster_name, kind, identity), so every lookup, lock and update is served by the primary key.
/// </summary>
internal sealed class PostgreSqlIdentitySql
{
    public PostgreSqlIdentitySql(PostgreSqlIdentityStorageOptions options)
    {
        var schema = PostgreSqlIdentifier.Quote(options.Schema);
        var table = $"{schema}.{PostgreSqlIdentifier.Quote(options.TableName)}";
        var memberIndex = PostgreSqlIdentifier.Quote(options.TableName + "_member_id");

        CreateSchema = $"""
            CREATE SCHEMA IF NOT EXISTS {schema};
            CREATE TABLE IF NOT EXISTS {table} (
                cluster_name text        NOT NULL,
                kind         text        NOT NULL,
                identity     text        NOT NULL,
                locked_by    text        NULL,
                member_id    text        NULL,
                address      text        NULL,
                pid_id       text        NULL,
                updated_at   timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (cluster_name, kind, identity)
            );
            CREATE INDEX IF NOT EXISTS {memberIndex} ON {table} (cluster_name, member_id) WHERE member_id IS NOT NULL;
            """;

        TryAcquireLock = $"""
            INSERT INTO {table} (cluster_name, kind, identity, locked_by)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
            """;

        // The lock age comes from the database clock, so clock differences between members do not matter
        Lookup = $"""
            SELECT locked_by, member_id, address, pid_id, extract(epoch FROM now() - updated_at)::float8
            FROM {table}
            WHERE cluster_name = $1 AND kind = $2 AND identity = $3
            """;

        RemoveLock = $"""
            DELETE FROM {table}
            WHERE cluster_name = $1 AND kind = $2 AND identity = $3 AND locked_by = $4
            """;

        StoreActivation = $"""
            UPDATE {table}
            SET locked_by = NULL, member_id = $5, address = $6, pid_id = $7, updated_at = now()
            WHERE cluster_name = $1 AND kind = $2 AND identity = $3 AND locked_by = $4
            """;

        RemoveActivation = $"""
            DELETE FROM {table}
            WHERE cluster_name = $1 AND kind = $2 AND identity = $3 AND address = $4 AND pid_id = $5
            """;

        RemoveMember = $"""
            DELETE FROM {table}
            WHERE cluster_name = $1 AND member_id = $2
            """;

        // Loose index scan: one index probe per distinct member instead of reading every activation
        GetMemberIds = $"""
            WITH RECURSIVE members AS (
                (SELECT member_id FROM {table}
                 WHERE cluster_name = $1 AND member_id IS NOT NULL
                 ORDER BY member_id LIMIT 1)
                UNION ALL
                SELECT (SELECT t.member_id FROM {table} t
                        WHERE t.cluster_name = $1 AND t.member_id > members.member_id
                        ORDER BY t.member_id LIMIT 1)
                FROM members
                WHERE members.member_id IS NOT NULL
            )
            SELECT member_id FROM members WHERE member_id IS NOT NULL
            """;
    }

    public string CreateSchema { get; }
    public string TryAcquireLock { get; }
    public string Lookup { get; }
    public string RemoveLock { get; }
    public string StoreActivation { get; }
    public string RemoveActivation { get; }
    public string RemoveMember { get; }
    public string GetMemberIds { get; }
}
