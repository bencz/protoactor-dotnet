#nullable enable
using System;
using System.Threading.Tasks;
using FluentAssertions;
using Proto.Cluster.Partition;
using Proto.Cluster.Testing;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using Xunit;

namespace Proto.Cluster.Tests;

public class TestProviderTests
{
    [Fact]
    public void A_failing_status_handler_does_not_stop_delivery_to_the_other_handlers()
    {
        var agent = new InMemAgent();
        var delivered = 0;
        agent.StatusUpdate += (_, _) => throw new InvalidOperationException("handler failure");
        agent.StatusUpdate += (_, _) => delivered++;

        var register = () => agent.RegisterService(Registration("member-1"));

        register.Should().NotThrow();
        delivered.Should().Be(1);
    }

    [Fact]
    public async Task A_stopped_member_no_longer_receives_status_updates()
    {
        var agent = new InMemAgent();
        var system = new ActorSystem()
            .WithRemote(RemoteConfig.BindToLocalhost())
            .WithCluster(ClusterConfig.Setup("MyCluster", new TestProvider(new TestProviderOptions(), agent),
                new PartitionIdentityLookup()));

        var cluster = system.Cluster();
        await cluster.StartMemberAsync();
        agent.StatusUpdateSubscriberCount.Should().Be(1);

        await cluster.ShutdownAsync(reason: "test");

        agent.StatusUpdateSubscriberCount.Should().Be(0);
    }

    private static AgentServiceRegistration Registration(string id) => new()
    {
        ID = id,
        Host = "127.0.0.1",
        Port = 4020,
        Kinds = Array.Empty<string>()
    };
}
