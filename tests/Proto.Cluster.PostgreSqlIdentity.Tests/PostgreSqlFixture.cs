using Npgsql;
using Proto.Cluster.Identity.PostgreSql;
using Proto.Cluster.SeedNode.PostgreSql;
using Proto.TestFixtures;
using Microsoft.Extensions.Configuration;

namespace Proto.Cluster.PostgreSqlIdentity.Tests;

internal static class PostgreSqlFixture
{
    static PostgreSqlFixture()
    {
        DataSource = NpgsqlDataSource.Create(TestConfig.Configuration.GetConnectionString("PostgreSql")!);

        // The shared storage tests use the storage without IdentityStorageLookup, which is what normally creates it
        using var command = DataSource.CreateCommand(PostgreSqlIdentityStorage.CreateSchemaSql());
        command.ExecuteNonQuery();
    }

    public static NpgsqlDataSource DataSource { get; }
}
