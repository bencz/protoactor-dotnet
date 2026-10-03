using FluentAssertions;
using Proto.TestKit;
using Xunit;

namespace Proto.Cluster.PubSub.Tests;

public class PubSubMemberDeliveryActorTests
{
    [Fact]
    public async Task Delivers_with_a_sub_second_subscriber_timeout()
    {
        await using var system = new ActorSystem();
        var (probe, subscriber) = system.CreateTestProbe();

        // Used to be truncated to zero seconds, which failed every delivery
        var delivery = system.Root.Spawn(
            Props.FromProducer(() => new PubSubMemberDeliveryActor(TimeSpan.FromMilliseconds(500))));

        system.Root.Send(delivery, new DeliverBatchRequest(
            new Subscribers { Subscribers_ = { new SubscriberIdentity { Pid = subscriber } } },
            new PubSubBatch { Envelopes = { new DataPublished(1) } },
            "topic"));

        var message = await probe.GetNextUserMessageAsync<DataPublished>();

        message.Data.Should().Be(1);
    }
}
