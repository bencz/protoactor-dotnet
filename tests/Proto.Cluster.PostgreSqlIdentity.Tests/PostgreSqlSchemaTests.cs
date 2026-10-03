using System;
using FluentAssertions;
using Proto.Cluster.Identity.PostgreSql;
using Proto.Cluster.SeedNode.PostgreSql;
using Xunit;
using IdentityQuoting = Proto.Cluster.Identity.PostgreSql.PostgreSqlIdentifier;

namespace Proto.Cluster.PostgreSqlIdentity.Tests;

public class PostgreSqlSchemaTests
{
    [Theory]
    [InlineData("proto_cluster_activations")]
    [InlineData("_private")]
    [InlineData("Tenant01")]
    public void AcceptsPlainIdentifiers(string name) =>
        IdentityQuoting.Quote(name).Should().Be($"\"{name}\"");

    [Theory]
    [InlineData("")]
    [InlineData("1table")]
    [InlineData("table; DROP TABLE users")]
    [InlineData("my\"table")]
    [InlineData("schema.table")]
    [InlineData("a_name_that_is_far_too_long_to_be_a_valid_postgresql_identifier_x")]
    public void RejectsIdentifiersThatCouldInjectSql(string name)
    {
        var quote = () => IdentityQuoting.Quote(name);

        quote.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void IdentitySchemaUsesTheConfiguredTableAndIndexesMembers()
    {
        var sql = PostgreSqlIdentityStorage.CreateSchemaSql(
            new PostgreSqlIdentityStorageOptions { Schema = "cluster", TableName = "activations" });

        sql.Should().Contain("CREATE TABLE IF NOT EXISTS \"cluster\".\"activations\"")
            .And.Contain("PRIMARY KEY (cluster_name, kind, identity)")
            .And.Contain("CREATE INDEX IF NOT EXISTS \"activations_member_id\" ON \"cluster\".\"activations\" (cluster_name, member_id) WHERE member_id IS NOT NULL");
    }

    [Fact]
    public void SeedSchemaUsesTheConfiguredTableAndIndexesExpiration()
    {
        var sql = PostgreSqlSeedNodeDiscovery.CreateSchemaSql(
            new PostgreSqlSeedNodeDiscoveryOptions { Schema = "cluster", TableName = "seeds" });

        sql.Should().Contain("CREATE TABLE IF NOT EXISTS \"cluster\".\"seeds\"")
            .And.Contain("PRIMARY KEY (cluster_name, member_id)")
            .And.Contain("CREATE INDEX IF NOT EXISTS \"seeds_expires_at\" ON \"cluster\".\"seeds\" (cluster_name, expires_at)");
    }

    [Fact]
    public void InvalidConfiguredNamesAreRejectedWhenTheStorageIsCreated()
    {
        var create = () => PostgreSqlIdentityStorage.CreateSchemaSql(
            new PostgreSqlIdentityStorageOptions { TableName = "activations; DROP TABLE users" });

        create.Should().Throw<ArgumentException>();
    }
}
