using System;
using Proto.Cluster.Identity;
using Proto.Cluster.Identity.PostgreSql;
using Proto.Cluster.Identity.Tests;
using Proto.Cluster.Tests;
using Xunit;
using Xunit.Abstractions;

namespace Proto.Cluster.PostgreSqlIdentity.Tests;

public class PostgreSqlIdentityClusterFixture : BaseInMemoryClusterFixture
{
    public PostgreSqlIdentityClusterFixture() : base(3,
        config => config with { ActorActivationTimeout = TimeSpan.FromSeconds(10) })
    {
    }

    protected override IIdentityLookup GetIdentityLookup(string clusterName) =>
        new IdentityStorageLookup(new PostgreSqlIdentityStorage(clusterName, PostgreSqlFixture.DataSource));

    public class PostgreSqlClusterTests : ClusterTests, IClassFixture<PostgreSqlIdentityClusterFixture>
    {
        // ReSharper disable once SuggestBaseTypeForParameter
        public PostgreSqlClusterTests(ITestOutputHelper testOutputHelper, PostgreSqlIdentityClusterFixture clusterFixture)
            : base(testOutputHelper, clusterFixture)
        {
        }
    }
}

public class ChaosPostgreSqlIdentityClusterFixture : BaseInMemoryClusterFixture
{
    public ChaosPostgreSqlIdentityClusterFixture() : base(3,
        config => config with { ActorActivationTimeout = TimeSpan.FromSeconds(10) })
    {
    }

    protected override IIdentityLookup GetIdentityLookup(string clusterName) =>
        new IdentityStorageLookup(
            new FailureInjectionStorage(new PostgreSqlIdentityStorage(clusterName, PostgreSqlFixture.DataSource)));

    public class ChaosPostgreSqlClusterTests : ClusterTests, IClassFixture<ChaosPostgreSqlIdentityClusterFixture>
    {
        // ReSharper disable once SuggestBaseTypeForParameter
        public ChaosPostgreSqlClusterTests(ITestOutputHelper testOutputHelper,
            ChaosPostgreSqlIdentityClusterFixture clusterFixture)
            : base(testOutputHelper, clusterFixture)
        {
        }
    }
}

public class PostgreSqlStorageTests : IdentityStorageTests
{
    public PostgreSqlStorageTests(ITestOutputHelper testOutputHelper) : base(Init, testOutputHelper)
    {
    }

    private static IIdentityStorage Init(string clusterName) =>
        new PostgreSqlIdentityStorage(clusterName, PostgreSqlFixture.DataSource,
            new PostgreSqlIdentityStorageOptions { MaxWaitBeforeStaleLock = TimeSpan.FromMilliseconds(1500) });
}
