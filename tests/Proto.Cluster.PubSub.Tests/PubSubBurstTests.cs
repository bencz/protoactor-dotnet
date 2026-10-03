using System.Collections.Concurrent;
using FluentAssertions;
using Proto.Cluster.Tests;
using Xunit;
using Xunit.Abstractions;
using static Proto.TestKit.TestKit;

namespace Proto.Cluster.PubSub.Tests;

[CollectionDefinition(nameof(PubSubBurstTests), DisableParallelization = true)]
public class PubSubBurstCollection
{
}

/// <summary>
///     A burst of publishes to a subscriber that is slower than the subscriber timeout.
/// </summary>
[Collection(nameof(PubSubBurstTests))]
public class PubSubBurstTests : IAsyncLifetime
{
    private const string Topic = "burst-topic";
    private const int MessageCount = 200;

    // 200 messages x 25 ms keep the subscriber busy for about 5 s, far longer than the 1 s subscriber timeout
    private static readonly TimeSpan HandlingTime = TimeSpan.FromMilliseconds(25);

    private readonly BurstClusterFixture _fixture = new();
    private readonly ITestOutputHelper _output;

    public PubSubBurstTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public Task InitializeAsync() => _fixture.InitializeAsync();

    public Task DisposeAsync() => _fixture.DisposeAsync();

    [Fact]
    public async Task Slow_subscriber_gets_every_message_of_a_burst_while_the_timeouts_are_logged_and_throttled()
    {
        var member = _fixture.Members[0];
        var received = new ConcurrentQueue<int>();
        var linesBefore = DeliveryLogCapture.Lines.Count;

        var subscriber = member.System.Root.Spawn(Props.FromFunc(async context =>
        {
            if (context.Message is DataPublished published)
            {
                // Simulates a subscriber doing slow work for every message
                await Task.Delay(HandlingTime);
                received.Enqueue(published.Data);
            }
        }));

        await member.Subscribe(Topic, subscriber);

        var publisher = member.System.Root.Spawn(Props.FromFunc(async context =>
        {
            if (context.Message is PublishBurst burst)
            {
                for (var i = 0; i < burst.Count; i++)
                {
                    await context.Cluster().Publisher().Publish(Topic, new DataPublished(i), context.CancellationToken);
                }

                context.Respond(new BurstPublished());
            }
        }));

        await member.System.Root.RequestAsync<BurstPublished>(publisher, new PublishBurst(MessageCount),
            TimeSpan.FromSeconds(30));

        // Timed out deliveries are already in the subscriber's mailbox, so nothing is lost and the order is kept
        await AwaitConditionAsync(() => received.Count == MessageCount, TimeSpan.FromSeconds(30));
        received.Should().Equal(Enumerable.Range(0, MessageCount));

        // The throttle summary is written when its 1 s window closes
        await AwaitConditionAsync(
            () => NewLines(linesBefore).Any(line => line.Contains("[PubSubMemberDeliveryActor] Throttled")),
            TimeSpan.FromSeconds(10));

        var lines = NewLines(linesBefore);

        foreach (var line in lines.Where(line => line.Contains("Throttled")))
        {
            _output.WriteLine(line);
        }

        _output.WriteLine($"{lines.Count(line => line.Contains("timed out"))} delivery timeouts logged before throttling");
        lines.Should().Contain(line => line.Contains("timed out") && line.Contains(subscriber.Id));
        lines.Should().Contain(line => line.Contains("[PubSubMemberDeliveryActor] Throttled"));
    }

    private static string[] NewLines(int skip) => DeliveryLogCapture.Lines.Skip(skip).ToArray();

    private record PublishBurst(int Count);

    private record BurstPublished;

    private class BurstClusterFixture : BaseInMemoryClusterFixture
    {
        public BurstClusterFixture() : base(1, config => config
            .WithPubSubConfig(PubSubConfig.Setup().WithSubscriberTimeout(TimeSpan.FromSeconds(1))))
        {
        }
    }
}
